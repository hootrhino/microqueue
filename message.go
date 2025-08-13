package microqueue

import "time"

// AckMode controls message acknowledgment behavior.
type AckMode int

const (
	// AckAuto means the system marks messages as acknowledged automatically.
	// In Subscribe mode:
	//   - If handler returns nil, the message is considered done.
	//   - If handler returns error, the message is re-queued (Retry++).
	// In ConsumeOne mode:
	//   - Delivery marks in-flight; caller does not need to call Ack().
	//     Ack/Nack calls are no-op for AckAuto.
	AckAuto AckMode = iota

	// AckManual requires explicit Ack/Nack by consumer.
	AckManual
)

// Message is the user-facing message envelope.
type Message struct {
	ID        string        // Globally unique message identifier (caller-provided).
	Topic     string        // Queue name.
	Payload   []byte        // Arbitrary bytes.
	Timestamp time.Time     // Creation time (set by Publish).
	Delay     time.Duration // Optional delivery delay (set by PublishWithDelay).
	Retry     int           // Retry count (incremented on requeue).
}
