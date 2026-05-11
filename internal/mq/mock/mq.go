// Package mock provides an in-memory implementation of mq.Publisher and
// mq.Consumer for use in tests and MOCK=true runs. No external broker is required.
package mock

import (
	"context"
	"fmt"
	"sync"

	"github.com/keyork/rc_keyork/internal/model"
)

// Queue is a channel-backed in-memory message queue. It implements both
// mq.Publisher and mq.Consumer so a single instance can be shared between
// the API layer (publisher) and the worker (consumer).
type Queue struct {
	mu         sync.Mutex
	main       chan model.DeliveryMessage
	deadLetter []model.DeliveryMessage
	retries    map[int][]model.DeliveryMessage // retry level → messages
}

// New creates a Queue with a buffer of 1024 messages. Exceeding the buffer
// returns an error from Publish rather than blocking.
func New() *Queue {
	return &Queue{
		main:    make(chan model.DeliveryMessage, 1024),
		retries: make(map[int][]model.DeliveryMessage),
	}
}

// --- Publisher ---

// Publish enqueues a message onto the main queue.
// Returns an error if the buffer is full rather than blocking.
func (q *Queue) Publish(_ context.Context, msg model.DeliveryMessage) error {
	select {
	case q.main <- msg:
		return nil
	default:
		return fmt.Errorf("mock queue: main channel buffer full (capacity %d)", cap(q.main))
	}
}

// PublishRetry records the message in the retry log for the given level.
// Messages are NOT automatically re-enqueued — tests that need re-delivery
// must call Requeue() explicitly. This keeps test behaviour deterministic.
func (q *Queue) PublishRetry(_ context.Context, msg model.DeliveryMessage, retryLevel int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.retries[retryLevel] = append(q.retries[retryLevel], msg)
	return nil
}

// PublishDeadLetter appends the message to the dead-letter log.
func (q *Queue) PublishDeadLetter(_ context.Context, msg model.DeliveryMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deadLetter = append(q.deadLetter, msg)
	return nil
}

func (q *Queue) Close() error { return nil }

// --- Consumer ---

// Consume blocks on the main queue channel until ctx is cancelled.
// Each message is passed to handler; the return value is ignored in mock mode
// (there is no broker to ack/nack).
func (q *Queue) Consume(ctx context.Context, handler func(model.DeliveryMessage) bool) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-q.main:
			handler(msg)
		}
	}
}

// --- Test inspection helpers ---

// Requeue moves all recorded retry messages back onto the main queue.
// Useful in tests that want to observe retry re-delivery.
func (q *Queue) Requeue() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for level, msgs := range q.retries {
		for _, m := range msgs {
			q.main <- m
		}
		delete(q.retries, level)
	}
}

// DeadLetters returns a snapshot of all dead-lettered messages.
func (q *Queue) DeadLetters() []model.DeliveryMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]model.DeliveryMessage, len(q.deadLetter))
	copy(out, q.deadLetter)
	return out
}

// RetryMessages returns a snapshot of all retry messages for the given level.
func (q *Queue) RetryMessages(level int) []model.DeliveryMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]model.DeliveryMessage, len(q.retries[level]))
	copy(out, q.retries[level])
	return out
}

// Drain removes and returns all messages currently waiting in the main queue.
func (q *Queue) Drain() []model.DeliveryMessage {
	var msgs []model.DeliveryMessage
	for {
		select {
		case m := <-q.main:
			msgs = append(msgs, m)
		default:
			return msgs
		}
	}
}
