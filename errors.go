package microqueue

import "errors"

var (
	ErrQueueExists    = errors.New("queue already exists")
	ErrQueueNotFound  = errors.New("queue not found")
	ErrClosed         = errors.New("queue is closed")
	ErrTimeout        = errors.New("timeout")
	ErrMsgNotInFlight = errors.New("message not in-flight")
	ErrWrongConsumer  = errors.New("message is owned by another consumer")
	ErrEmptyID        = errors.New("message ID is empty")
	ErrAckModeAuto    = errors.New("operation not allowed in AckAuto mode")
)
