package microqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Queue is the user-facing interface for a single named queue.
type Queue interface {
	// Producer APIs
	Publish(msg Message) error
	PublishWithDelay(msg Message, delay time.Duration) error

	// Consumer APIs
	Subscribe(consumerID string, handler func(Message) error) (unsubscribe func(), err error)
	ConsumeOne(consumerID string, timeout time.Duration) (Message, error)

	// Ack/Nack (only meaningful in AckManual)
	Ack(consumerID string, msgID string) error
	Nack(consumerID string, msgID string, requeue bool) error
	DroppedCount() uint64
	// Observability & lifecycle
	PendingCount() int
	Close() error
}

// QueueOption allows tuning queue behavior.
type QueueOption func(*queue)

// WithBuffer sets the delivery buffer size (ready channel capacity).
func WithBuffer(n int) QueueOption {
	return func(q *queue) { q.readyBuf = n }
}

// WithMaxTimers bounds internal timer slice to help GC (soft limit).
func WithMaxTimers(n int) QueueOption {
	return func(q *queue) { q.maxTimers = n }
}

// internal structure tracked for in-flight ownership
type inflight struct {
	msg        Message
	consumerID string
	delivered  time.Time
}

type queue struct {
	name    string
	ackMode AckMode

	ctx    context.Context
	cancel context.CancelFunc

	// ready channel serves messages to both Subscribe workers and ConsumeOne pulls
	ready    chan Message
	readyBuf int

	// in-flight map tracks messages delivered but not yet acked (manual) or not yet returned (auto-pull).
	muIn  sync.Mutex
	inMap map[string]inflight // key: msg.ID

	// delayed message timers (one-shot). We keep handles for shutdown.
	muTimers     sync.Mutex
	timers       []*time.Timer
	maxTimers    int
	droppedCount uint64 // count of dropped messages when full
	// subscription workers
	muSubs    sync.Mutex
	subs      map[string]*subWorker // key: consumerID
	closed    bool
	closeOnce sync.Once
}

// subWorker binds a consumerID to a delivery goroutine.
type subWorker struct {
	consumerID string
	stop       context.CancelFunc
	wg         sync.WaitGroup
}

// newQueue constructs a queue instance.
func newQueue(parent context.Context, name string, ackMode AckMode, opts ...QueueOption) *queue {
	ctx, cancel := context.WithCancel(parent)
	q := &queue{
		name:      name,
		ackMode:   ackMode,
		ctx:       ctx,
		cancel:    cancel,
		readyBuf:  1024,
		inMap:     make(map[string]inflight),
		subs:      make(map[string]*subWorker),
		maxTimers: 1_000_000, // generous default
	}
	for _, opt := range opts {
		opt(q)
	}
	q.ready = make(chan Message, q.readyBuf)
	return q
}

// Publish enqueues message for immediate delivery.
// If the queue is full, the oldest message will be dropped instead of blocking.
func (q *queue) Publish(msg Message) error {
	if msg.ID == "" {
		return ErrEmptyID
	}
	msg.Timestamp = time.Now()
	if q.isClosed() {
		return ErrClosed
	}

	select {
	case q.ready <- msg:
		return nil
	default:
		// Drop oldest
		select {
		case <-q.ready:
			atomic.AddUint64(&q.droppedCount, 1)
		default:
		}
		// Push new
		select {
		case q.ready <- msg:
			return nil
		case <-q.ctx.Done():
			return ErrClosed
		}
	}
}

// PublishWithDelay enqueues message to be delivered after delay.
func (q *queue) PublishWithDelay(msg Message, delay time.Duration) error {
	if msg.ID == "" {
		return ErrEmptyID
	}
	if q.isClosed() {
		return ErrClosed
	}
	if delay <= 0 {
		return q.Publish(msg)
	}
	msg.Delay = delay
	msg.Timestamp = time.Now()

	timer := time.AfterFunc(delay, func() {
		// try deliver unless closed
		select {
		case <-q.ctx.Done():
			return
		default:
		}
		_ = q.Publish(msg) // Publish already checks closed; ignore error on shutdown.
	})
	q.trackTimer(timer)
	return nil
}

func (q *queue) trackTimer(t *time.Timer) {
	q.muTimers.Lock()
	defer q.muTimers.Unlock()
	q.timers = append(q.timers, t)
	// Soft prune to avoid unbounded slice growth
	if len(q.timers) > q.maxTimers {
		alive := q.timers[:0]
		for _, tm := range q.timers {
			if tm != nil {
				alive = append(alive, tm)
			}
		}
		q.timers = alive
	}
}

// Subscribe starts a competing-consumer worker bound to consumerID.
// In AckManual mode, Subscribe records in-flight ownership before invoking handler.
// In AckAuto mode, after handler returns:
//   - nil  -> done (no requeue)
//   - error-> message re-queued with Retry++.
func (q *queue) Subscribe(consumerID string, handler func(Message) error) (func(), error) {
	if q.isClosed() {
		return func() {}, ErrClosed
	}
	ctx, stop := context.WithCancel(q.ctx)
	w := &subWorker{consumerID: consumerID, stop: stop}
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-q.ready:
				if !ok {
					return
				}
				// mark in-flight for manual ack
				if q.ackMode == AckManual {
					q.markInflight(consumerID, msg)
				}
				err := handler(msg)

				// auto-ack behavior
				if q.ackMode == AckAuto {
					if err != nil {
						// requeue on handler error
						msg.Retry++
						_ = q.Publish(msg)
					}
				}
				// note: in manual mode user must call Ack/Nack explicitly.
			}
		}
	}()

	q.muSubs.Lock()
	q.subs[consumerID] = w
	q.muSubs.Unlock()

	unsub := func() {
		w.stop()
		w.wg.Wait()
		q.muSubs.Lock()
		delete(q.subs, consumerID)
		q.muSubs.Unlock()
	}
	return unsub, nil
}

// ConsumeOne pulls a single message (competing consumer) with timeout.
// In AckManual mode, the message is marked in-flight and must be Ack'ed or Nack'ed.
// In AckAuto mode, Ack/Nack are no-op for this msg; delivery completes on return.
func (q *queue) ConsumeOne(consumerID string, timeout time.Duration) (Message, error) {
	if q.isClosed() {
		return Message{}, ErrClosed
	}
	var timer <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-q.ctx.Done():
		return Message{}, ErrClosed
	case <-timer:
		return Message{}, ErrTimeout
	case msg := <-q.ready:
		if q.ackMode == AckManual {
			q.markInflight(consumerID, msg)
		}
		return msg, nil
	}
}

func (q *queue) markInflight(consumerID string, msg Message) {
	q.muIn.Lock()
	q.inMap[msg.ID] = inflight{msg: msg, consumerID: consumerID, delivered: time.Now()}
	q.muIn.Unlock()
}

// Ack marks an in-flight message as done (AckManual only).
func (q *queue) Ack(consumerID, msgID string) error {
	if q.ackMode == AckAuto {
		return ErrAckModeAuto
	}
	q.muIn.Lock()
	defer q.muIn.Unlock()
	inf, ok := q.inMap[msgID]
	if !ok {
		return ErrMsgNotInFlight
	}
	if inf.consumerID != consumerID {
		return ErrWrongConsumer
	}
	delete(q.inMap, msgID)
	return nil
}

// Nack negatively acknowledges an in-flight message (AckManual only).
// If requeue is true, the message is appended back to the ready queue (Retry++).
func (q *queue) Nack(consumerID, msgID string, requeue bool) error {
	if q.ackMode == AckAuto {
		return ErrAckModeAuto
	}
	q.muIn.Lock()
	inf, ok := q.inMap[msgID]
	if !ok {
		q.muIn.Unlock()
		return ErrMsgNotInFlight
	}
	if inf.consumerID != consumerID {
		q.muIn.Unlock()
		return ErrWrongConsumer
	}
	delete(q.inMap, msgID)
	q.muIn.Unlock()

	if requeue {
		inf.msg.Retry++
		return q.Publish(inf.msg)
	}
	return nil
}

// PendingCount returns approximate count of not-yet-delivered messages.
// It excludes in-flight. Intended for metrics/rough sizing.
func (q *queue) PendingCount() int {
	return len(q.ready)
}

// Close gracefully stops subscriptions, cancels timers, and closes delivery channel.
func (q *queue) Close() error {
	q.closeOnce.Do(func() {
		q.muSubs.Lock()
		for _, w := range q.subs {
			w.stop()
		}
		for _, w := range q.subs {
			w.wg.Wait()
		}
		q.subs = map[string]*subWorker{}
		q.muSubs.Unlock()

		// stop timers
		q.muTimers.Lock()
		for _, t := range q.timers {
			if t != nil {
				t.Stop()
			}
		}
		q.timers = nil
		q.muTimers.Unlock()

		q.cancel()
		q.muSubs.Lock()
		q.closed = true
		q.muSubs.Unlock()
		close(q.ready)
	})
	return nil
}

func (q *queue) isClosed() bool {
	q.muSubs.Lock()
	defer q.muSubs.Unlock()
	return q.closed
}

// DroppedCount returns the number of messages dropped due to a full queue.
func (q *queue) DroppedCount() uint64 {
	return atomic.LoadUint64(&q.droppedCount)
}
