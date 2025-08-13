# microqueue

`microqueue` is a lightweight, in-memory message queue library for Go, supporting both **manual** and **automatic acknowledgment modes**, delayed messages, and competing-consumer subscriptions. It is designed for high throughput with minimal blocking while providing basic observability.

## Features

* Multiple named queues
* Immediate and delayed message publishing
* Competing-consumer subscription
* Manual (`AckManual`) and automatic (`AckAuto`) acknowledgment modes
* Message retry on handler errors (in automatic mode)
* Graceful shutdown
* Metrics for dropped and pending messages
* Configurable buffer sizes and internal timers

## Installation

```bash
go get github.com/hootrhino/microqueue
```

## Usage

### Creating a Queue

```go
import "github.com/hootrhino/microqueue"

q := microqueue.NewQueue(
    "myqueue",
    microqueue.AckManual, // or AckAuto
    microqueue.WithBuffer(1024),
    microqueue.WithMaxTimers(1000),
)
defer q.Close()
```

### Publishing Messages

```go
msg := microqueue.Message{
    ID:   "msg-1",
    Body: []byte("hello world"),
}

// Immediate delivery
q.Publish(msg)

// Delayed delivery
q.PublishWithDelay(msg, 5*time.Second)
```

### Subscribing as a Consumer

```go
unsubscribe, err := q.Subscribe("consumer1", func(msg microqueue.Message) error {
    fmt.Println("Received:", string(msg.Body))
    return nil // or return error to trigger retry (AckAuto)
})
defer unsubscribe()
```

### Pulling Messages Manually

```go
msg, err := q.ConsumeOne("consumer1", 2*time.Second)
if err != nil {
    // handle timeout or closed queue
}

// Manual acknowledgment
q.Ack("consumer1", msg.ID)

// Negative acknowledgment with requeue
q.Nack("consumer1", msg.ID, true)
```

### Observability

```go
fmt.Println("Pending messages:", q.PendingCount())
fmt.Println("Dropped messages:", q.DroppedCount())
```

### Graceful Shutdown

```go
q.Close()
```

## Queue Options

* `WithBuffer(n int)` — set delivery buffer size.
* `WithMaxTimers(n int)` — set maximum internal timer count to prevent memory growth.

## Acknowledgment Modes

* `AckManual`: Consumer must call `Ack`/`Nack` explicitly.
* `AckAuto`: Messages are automatically acknowledged after handler returns; failed handlers requeue the message.

## Notes

* If the ready queue is full, the oldest message is dropped to make room for new messages.
* Delayed messages are scheduled using Go timers and are delivered after the specified delay.
* Subscriptions run in separate goroutines and compete for messages from the same queue.
