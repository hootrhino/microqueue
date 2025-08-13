package microqueue

import (
	"context"
	"sync"
)

// QueueManager manages multiple named queues.
type QueueManager interface {
	CreateQueue(name string, ackMode AckMode, opts ...QueueOption) error
	DeleteQueue(name string) error
	ListQueues() []string
	GetQueue(name string) (Queue, error)
	Close() error
}

type memoryManager struct {
	mu     sync.RWMutex
	queues map[string]*queue
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMemoryQueueManager creates a manager with lifecycle context.
func NewMemoryQueueManager() QueueManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &memoryManager{
		queues: make(map[string]*queue),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (m *memoryManager) CreateQueue(name string, ackMode AckMode, opts ...QueueOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return ErrQueueExists
	}
	q := newQueue(m.ctx, name, ackMode, opts...)
	m.queues[name] = q
	return nil
}

func (m *memoryManager) DeleteQueue(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	q, ok := m.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	q.Close()
	delete(m.queues, name)
	return nil
}

func (m *memoryManager) ListQueues() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.queues))
	for k := range m.queues {
		out = append(out, k)
	}
	return out
}

func (m *memoryManager) GetQueue(name string) (Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

func (m *memoryManager) Close() error {
	m.cancel()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, q := range m.queues {
		q.Close()
	}
	m.queues = map[string]*queue{}
	return nil
}
