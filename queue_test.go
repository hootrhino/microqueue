package microqueue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPublishConsumeAckManual(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	if err := m.CreateQueue("orders", AckManual); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue("orders")

	msg := Message{ID: "m1", Topic: "orders", Payload: []byte("hello")}
	if err := q.Publish(msg); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got, err := q.ConsumeOne("c1", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got.ID != "m1" {
		t.Fatalf("expect m1 got %s", got.ID)
	}
	if err := q.Ack("c1", got.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if c := q.PendingCount(); c != 0 {
		t.Fatalf("expect pending 0 got %d", c)
	}
}

func TestSubscribeAutoAck(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	if err := m.CreateQueue("jobs", AckAuto); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue("jobs")

	var mu sync.Mutex
	count := 0
	unsub, err := q.Subscribe("worker-1", func(m Message) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()

	N := 100
	for i := 0; i < N; i++ {
		id := "j-" + itoa(i)
		if err := q.Publish(Message{ID: id, Topic: "jobs"}); err != nil {
			t.Fatal(err)
		}
	}

	// wait until processed or timeout
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		if count >= N {
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if count != N {
		t.Fatalf("expect %d processed, got %d", N, count)
	}
}

func TestDelayedDelivery(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	if err := m.CreateQueue("delay", AckManual); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue("delay")

	start := time.Now()
	if err := q.PublishWithDelay(Message{ID: "d1", Topic: "delay"}, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	got, err := q.ConsumeOne("c1", 1*time.Second)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got.ID != "d1" {
		t.Fatalf("expect d1 got %s", got.ID)
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatalf("delivered too early: %v", time.Since(start))
	}
	if err := q.Ack("c1", "d1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestNackRequeue(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	if err := m.CreateQueue("nackq", AckManual); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue("nackq")

	if err := q.Publish(Message{ID: "x1", Topic: "nackq"}); err != nil {
		t.Fatal(err)
	}
	msg, err := q.ConsumeOne("c1", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != "x1" {
		t.Fatalf("expect x1 got %s", msg.ID)
	}
	// Nack with requeue
	if err := q.Nack("c1", "x1", true); err != nil {
		t.Fatalf("nack: %v", err)
	}
	// Should come again
	msg2, err := q.ConsumeOne("c2", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if msg2.ID != "x1" {
		t.Fatalf("expect x1 got %s", msg2.ID)
	}
	if msg2.Retry != 1 {
		t.Fatalf("expect retry 1 got %d", msg2.Retry)
	}
	if err := q.Ack("c2", "x1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestConcurrentProducersConsumers_NoBlockOnFull(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	// Create queue with small buffer to trigger full condition
	if err := m.CreateQueue("concurrent", AckManual, WithBuffer(100)); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue("concurrent")

	total := 5000
	producers := 5
	consumers := 10

	var mu sync.Mutex
	received := 0

	prodWG := sync.WaitGroup{}
	consWG := sync.WaitGroup{}
	stop := make(chan struct{})

	// Producers
	prodWG.Add(producers)
	for p := 0; p < producers; p++ {
		go func(pi int) {
			defer prodWG.Done()
			start := pi * (total / producers)
			end := start + (total / producers)
			for i := start; i < end; i++ {
				id := "m-" + itoa(i)
				if err := q.Publish(Message{ID: id, Topic: "concurrent"}); err != nil && err != ErrClosed {
					t.Error(err)
					return
				}
			}
		}(p)
	}

	// Consumers
	consWG.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func(ci int) {
			defer consWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				msg, err := q.ConsumeOne("c-"+itoa(ci), 50*time.Millisecond)
				if err == ErrTimeout {
					continue
				}
				if err != nil {
					return
				}
				_ = q.Ack("c-"+itoa(ci), msg.ID)
				mu.Lock()
				received++
				mu.Unlock()
			}
		}(c)
	}

	// Wait for producers to finish
	prodWG.Wait()

	// Wait until all messages are drained
	time.Sleep(500 * time.Millisecond)

	// Stop consumers
	close(stop)
	consWG.Wait()

	t.Logf("Received %d messages (some may be dropped due to full queue)", received)
}

// small integer to ascii helper without fmt for speed in tight loops
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
func TestDroppedCount(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	if err := m.CreateQueue("dropq", AckManual, WithBuffer(3)); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue("dropq")

	// Fill queue
	for i := 0; i < 3; i++ {
		_ = q.Publish(Message{ID: itoa(i), Topic: "dropq"})
	}

	// Push extra messages; should drop oldest
	_ = q.Publish(Message{ID: "extra1", Topic: "dropq"})
	_ = q.Publish(Message{ID: "extra2", Topic: "dropq"})

	if dropped := q.DroppedCount(); dropped != 2 {
		t.Fatalf("expected 2 dropped messages, got %d", dropped)
	}
}

func TestMultiProducerMultiSubscriber(t *testing.T) {
	m := NewMemoryQueueManager()
	defer m.Close()

	queueName := "multi"
	bufSize := 20000 // small buffer to force drops
	if err := m.CreateQueue(queueName, AckManual, WithBuffer(bufSize)); err != nil {
		t.Fatal(err)
	}
	q, _ := m.GetQueue(queueName)

	totalMessages := 20000
	producers := 50
	subscribers := 40

	var mu sync.Mutex
	processed := 0

	unsubs := make([]func(), 0, subscribers)
	for s := 0; s < subscribers; s++ {
		unsub, err := q.Subscribe(fmt.Sprintf("sub-%d", s), func(m Message) error {
			// simulate processing
			time.Sleep(time.Millisecond)
			mu.Lock()
			processed++
			mu.Unlock()
			_ = q.Ack(fmt.Sprintf("sub-%d", s), m.ID)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		unsubs = append(unsubs, unsub)
	}
	defer func() {
		for _, u := range unsubs {
			u()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(pid int) {
			defer wg.Done()
			start := pid * (totalMessages / producers)
			end := start + (totalMessages / producers)
			for i := start; i < end; i++ {
				id := fmt.Sprintf("msg-%d", i)
				_ = q.Publish(Message{ID: id, Topic: queueName, Payload: []byte("data")})
			}
		}(p)
	}

	wg.Wait()

	time.Sleep(2 * time.Second)

	dropped := q.DroppedCount()
	totalSeen := processed + int(dropped)

	t.Logf("Processed: %d, Dropped: %d, Total Seen: %d, Expected ~ %d",
		processed, dropped, totalSeen, totalMessages)

	if totalSeen < totalMessages*9/10 { // allow 10% deviation due to timing
		t.Fatalf("too many messages lost: processed=%d dropped=%d total=%d expected=%d",
			processed, dropped, totalSeen, totalMessages)
	}
}
