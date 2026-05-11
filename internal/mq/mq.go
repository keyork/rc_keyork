// Package mq defines the messaging interfaces used by the API and Worker layers.
// Concrete implementations live in sub-packages (rabbitmq, mock).
package mq

import (
	"context"

	"github.com/keyork/rc_keyork/internal/model"
)

// Publisher enqueues delivery messages onto the main or retry queues.
type Publisher interface {
	// Publish sends a message to the main delivery queue.
	// Implementations must not return until the broker has confirmed receipt
	// (publisher-confirm mode in RabbitMQ).
	Publish(ctx context.Context, msg model.DeliveryMessage) error

	// PublishRetry routes a message to the retry queue for the given level.
	// retryLevel is 1-based and maps to the exponential back-off schedule
	// (level 1 = 30 s, level 7 = 8 h). Values outside [1, 7] are clamped.
	PublishRetry(ctx context.Context, msg model.DeliveryMessage, retryLevel int) error

	// PublishDeadLetter routes a message that has exhausted all retries to
	// the dead-letter queue for manual inspection.
	PublishDeadLetter(ctx context.Context, msg model.DeliveryMessage) error

	Close() error
}

// Consumer receives delivery messages from the main queue.
type Consumer interface {
	// Consume blocks until ctx is cancelled, calling handler for each message.
	// Returning true from handler acks the message; returning false nacks it
	// (the broker will redeliver it).
	Consume(ctx context.Context, handler func(msg model.DeliveryMessage) bool) error

	Close() error
}
