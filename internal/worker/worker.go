// Package worker implements the asynchronous delivery engine.
// It consumes DeliveryMessages from the MQ, calls the target vendor API,
// handles retries and circuit-breaking, sends optional result callbacks,
// and recovers zombie notifications that got stuck in "processing".
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/keyork/rc_keyork/internal/circuitbreaker"
	"github.com/keyork/rc_keyork/internal/db"
	"github.com/keyork/rc_keyork/internal/model"
	"github.com/keyork/rc_keyork/internal/mq"
)

// retryDelays maps retry level (1-based) to the TTL of the corresponding
// RabbitMQ delay queue.  Level 8 routes to the dead-letter queue instead.
var retryDelays = []time.Duration{
	30 * time.Second, // level 1
	1 * time.Minute,  // level 2
	5 * time.Minute,  // level 3
	30 * time.Minute, // level 4
	2 * time.Hour,    // level 5
	4 * time.Hour,    // level 6
	8 * time.Hour,    // level 7
}

// Config is the runtime configuration for Pool and ZombieRecovery.
type Config struct {
	// Concurrency is the maximum number of deliveries in flight at once.
	Concurrency int
	// HTTPTimeout is the per-call timeout for outbound HTTP requests.
	HTTPTimeout time.Duration
	// CB is the circuit-breaker manager shared across all worker goroutines.
	CB *circuitbreaker.Manager
	// CallbackDelays is the sequence of wait durations between callback attempts.
	// Element 0 is the delay before the first attempt (normally 0); subsequent
	// elements are delays before retry 1, 2, …
	CallbackDelays []time.Duration
	// ZombieInterval is how often the zombie-recovery sweep runs.
	ZombieInterval time.Duration
	// ZombieThreshold is the number of minutes a notification must be stuck
	// in "processing" before it is treated as a zombie.
	ZombieThreshold int
}

func (c *Config) applyDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 100
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 30 * time.Second
	}
	if len(c.CallbackDelays) == 0 {
		c.CallbackDelays = []time.Duration{0, time.Second, 5 * time.Second, 30 * time.Second}
	}
	if c.ZombieInterval <= 0 {
		c.ZombieInterval = 5 * time.Minute
	}
	if c.ZombieThreshold <= 0 {
		c.ZombieThreshold = 10
	}
}

// Pool runs a bounded goroutine pool that consumes and delivers notifications.
type Pool struct {
	store      db.Store
	consumer   mq.Consumer
	publisher  mq.Publisher
	cb         *circuitbreaker.Manager
	httpClient *http.Client
	cfg        Config
}

// NewPool constructs a Pool. Missing cfg fields are filled with safe defaults.
func NewPool(store db.Store, consumer mq.Consumer, pub mq.Publisher, cfg Config) *Pool {
	cfg.applyDefaults()
	return &Pool{
		store:      store,
		consumer:   consumer,
		publisher:  pub,
		cb:         cfg.CB,
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		cfg:        cfg,
	}
}

// Run starts consuming from the MQ and dispatches each message to a goroutine
// in the pool. It blocks until ctx is cancelled, then waits for all in-flight
// deliveries to finish before returning.
//
// NOTE: In the mock implementation, messages are acked immediately after
// dispatch rather than after delivery. A real RabbitMQ consumer must ack only
// after p.handle() completes to honour the at-least-once guarantee.
func (p *Pool) Run(ctx context.Context) error {
	slog.Info("worker pool starting",
		"component", "worker",
		"concurrency", p.cfg.Concurrency,
		"http_timeout", p.cfg.HTTPTimeout,
	)

	sem := make(chan struct{}, p.cfg.Concurrency)
	var wg sync.WaitGroup

	err := p.consumer.Consume(ctx, func(msg model.DeliveryMessage) bool {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			p.handle(ctx, msg)
		}()
		return true
	})

	wg.Wait()
	slog.Info("worker pool stopped", "component", "worker")
	return err
}

// handle is the per-message delivery pipeline:
//  1. Load notification from DB.
//  2. Check circuit breaker; re-queue with minimum delay if open.
//  3. Mark notification as "processing".
//  4. Execute outbound HTTP call.
//  5. Route outcome: success / retryable error / non-retryable error.
func (p *Pool) handle(ctx context.Context, msg model.DeliveryMessage) {
	n, err := p.store.Get(ctx, msg.NotificationID)
	if err != nil {
		slog.Warn("notification not found, dropping message",
			"component", "worker",
			"notification_id", msg.NotificationID,
			"error", err,
		)
		return
	}

	domain := n.TargetDomain

	if !p.cb.Allow(domain) {
		slog.Info("circuit open — requeueing without consuming retry slot",
			"component", "worker",
			"notification_id", n.ID,
			"domain", domain,
		)
		if err := p.publisher.PublishRetry(ctx, msg, 1); err != nil {
			slog.Error("PublishRetry failed while circuit is open",
				"component", "worker",
				"notification_id", n.ID,
				"domain", domain,
				"error", err,
			)
		}
		return
	}

	n.Status = model.StatusProcessing
	n.UpdatedAt = time.Now().UTC()
	if err := p.store.Update(ctx, n); err != nil {
		// Log and continue — worst case zombie recovery will catch it.
		slog.Error("store.Update (→processing) failed",
			"component", "worker",
			"notification_id", n.ID,
			"error", err,
		)
	}

	slog.Debug("delivering notification",
		"component", "worker",
		"notification_id", n.ID,
		"target_url", n.TargetURL,
		"method", n.Method,
		"retry_count", msg.RetryCount,
	)

	statusCode, deliveryErr := p.deliver(n)

	switch {
	case deliveryErr == nil && statusCode >= 200 && statusCode < 300:
		p.cb.Record(domain, true)
		p.markSuccess(ctx, n, statusCode)

	case deliveryErr == nil && !isRetryable(statusCode):
		// Non-retryable 4xx — the request itself is bad; retrying won't help.
		p.cb.Record(domain, false)
		p.markFailed(ctx, n, statusCode, fmt.Errorf("HTTP %d", statusCode))

	default:
		// Retryable: network error, 429, or 5xx.
		p.cb.Record(domain, false)
		p.scheduleRetry(ctx, msg, n, statusCode, deliveryErr)
	}
}

// scheduleRetry increments the retry counter and routes the message to the
// appropriate delay queue, or to the dead-letter queue if retries are exhausted.
func (p *Pool) scheduleRetry(ctx context.Context, msg model.DeliveryMessage, n *model.Notification, statusCode int, deliveryErr error) {
	nextRetry := msg.RetryCount + 1

	if nextRetry > n.MaxRetries {
		var ferr error
		if deliveryErr != nil {
			ferr = deliveryErr
		} else {
			ferr = fmt.Errorf("HTTP %d", statusCode)
		}
		slog.Warn("retries exhausted, moving to dead-letter",
			"component", "worker",
			"notification_id", n.ID,
			"retry_count", n.RetryCount,
			"max_retries", n.MaxRetries,
			"last_http_status", statusCode,
		)
		p.markFailed(ctx, n, statusCode, ferr)
		if err := p.publisher.PublishDeadLetter(ctx, msg); err != nil {
			slog.Error("PublishDeadLetter failed",
				"component", "worker",
				"notification_id", n.ID,
				"error", err,
			)
		}
		return
	}

	delay := retryDelay(nextRetry)
	slog.Info("scheduling retry",
		"component", "worker",
		"notification_id", n.ID,
		"retry_level", nextRetry,
		"delay", delay,
		"last_http_status", statusCode,
	)

	retryMsg := model.DeliveryMessage{NotificationID: n.ID, RetryCount: nextRetry}
	if err := p.publisher.PublishRetry(ctx, retryMsg, nextRetry); err != nil {
		slog.Error("PublishRetry failed",
			"component", "worker",
			"notification_id", n.ID,
			"retry_level", nextRetry,
			"error", err,
		)
	}

	nextAt := time.Now().Add(delay)
	n.Status = model.StatusPending
	n.RetryCount = nextRetry
	n.NextRetryAt = &nextAt
	n.LastHTTPStatus = statusCode
	if deliveryErr != nil {
		n.LastError = deliveryErr.Error()
	} else {
		n.LastError = fmt.Sprintf("HTTP %d", statusCode)
	}
	n.UpdatedAt = time.Now().UTC()
	if err := p.store.Update(ctx, n); err != nil {
		slog.Error("store.Update (→retry) failed",
			"component", "worker",
			"notification_id", n.ID,
			"error", err,
		)
	}
}

// deliver constructs and executes the outbound HTTP call.
// Returns (statusCode, nil) on any completed HTTP exchange (even 4xx/5xx).
// Returns (0, err) on network/connection errors.
func (p *Pool) deliver(n *model.Notification) (int, error) {
	req, err := http.NewRequest(n.Method, n.TargetURL, strings.NewReader(n.Body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	for k, v := range n.Headers {
		req.Header.Set(k, v)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func (p *Pool) markSuccess(ctx context.Context, n *model.Notification, code int) {
	now := time.Now().UTC()
	n.Status = model.StatusSuccess
	n.LastHTTPStatus = code
	n.UpdatedAt = now
	n.CompletedAt = &now
	if err := p.store.Update(ctx, n); err != nil {
		slog.Error("store.Update (→success) failed",
			"component", "worker",
			"notification_id", n.ID,
			"error", err,
		)
	}
	slog.Info("notification delivered successfully",
		"component", "worker",
		"notification_id", n.ID,
		"http_status", code,
		"retry_count", n.RetryCount,
		"domain", n.TargetDomain,
	)
	if n.CallbackURL != "" {
		// Detached context: callback must finish even if the pool is shutting down.
		go p.sendCallback(context.Background(), n)
	}
}

func (p *Pool) markFailed(ctx context.Context, n *model.Notification, code int, deliveryErr error) {
	now := time.Now().UTC()
	n.Status = model.StatusFailed
	n.LastHTTPStatus = code
	if deliveryErr != nil {
		n.LastError = deliveryErr.Error()
	}
	n.UpdatedAt = now
	n.CompletedAt = &now
	if err := p.store.Update(ctx, n); err != nil {
		slog.Error("store.Update (→failed) failed",
			"component", "worker",
			"notification_id", n.ID,
			"error", err,
		)
	}
	slog.Warn("notification delivery failed permanently",
		"component", "worker",
		"notification_id", n.ID,
		"retry_count", n.RetryCount,
		"last_http_status", code,
		"error", deliveryErr,
	)
	if n.CallbackURL != "" {
		go p.sendCallback(context.Background(), n)
	}
}

// sendCallback posts the terminal delivery result to the caller-supplied callback URL.
// It retries according to cfg.CallbackDelays and gives up silently after the
// last attempt — callers should poll GET /notifications/{id} as a fallback.
func (p *Pool) sendCallback(ctx context.Context, n *model.Notification) {
	payload, err := json.Marshal(map[string]any{
		"notification_id": n.ID,
		"status":          n.Status,
		"target_url":      n.TargetURL,
		"attempted_at":    n.UpdatedAt,
		"retry_count":     n.RetryCount,
		"error":           n.LastError,
	})
	if err != nil {
		slog.Error("callback payload marshal failed",
			"component", "worker",
			"notification_id", n.ID,
			"error", err,
		)
		return
	}

	for attempt, delay := range p.cfg.CallbackDelays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}

		slog.Debug("sending callback",
			"component", "worker",
			"notification_id", n.ID,
			"callback_url", n.CallbackURL,
			"attempt", attempt+1,
		)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.CallbackURL, bytes.NewReader(payload))
		if err != nil {
			slog.Error("callback request build failed",
				"component", "worker",
				"notification_id", n.ID,
				"error", err,
			)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := p.httpClient.Do(req)
		if err == nil && resp.StatusCode < 300 {
			resp.Body.Close()
			slog.Info("callback delivered",
				"component", "worker",
				"notification_id", n.ID,
				"attempt", attempt+1,
				"http_status", resp.StatusCode,
			)
			return
		}
		if resp != nil {
			slog.Warn("callback attempt failed",
				"component", "worker",
				"notification_id", n.ID,
				"attempt", attempt+1,
				"http_status", resp.StatusCode,
			)
			resp.Body.Close()
		} else {
			slog.Warn("callback attempt failed",
				"component", "worker",
				"notification_id", n.ID,
				"attempt", attempt+1,
				"error", err,
			)
		}
	}
	slog.Warn("all callback attempts exhausted",
		"component", "worker",
		"notification_id", n.ID,
		"callback_url", n.CallbackURL,
	)
}

// isRetryable returns true for HTTP status codes that warrant a retry.
// Only 4xx codes (other than 429) are non-retryable because they indicate
// a problem with the request itself that retrying cannot fix.
func isRetryable(code int) bool {
	return code == 0 || code == 429 || code >= 500
}

// retryDelay returns the delay duration for the given 1-based retry level.
// Levels outside [1, len(retryDelays)] are clamped to the nearest bound.
func retryDelay(level int) time.Duration {
	if level < 1 {
		return retryDelays[0]
	}
	if level > len(retryDelays) {
		return retryDelays[len(retryDelays)-1]
	}
	return retryDelays[level-1]
}

// ZombieRecovery periodically scans for notifications stuck in "processing"
// and re-queues them. This handles the case where a worker crashes after
// writing "processing" to the DB but before acking the MQ message.
type ZombieRecovery struct {
	store     db.Store
	publisher mq.Publisher
	cfg       Config
}

// NewZombieRecovery constructs a ZombieRecovery. Missing cfg fields are filled
// with the same defaults used by NewPool.
func NewZombieRecovery(store db.Store, pub mq.Publisher, cfg Config) *ZombieRecovery {
	cfg.applyDefaults()
	return &ZombieRecovery{store: store, publisher: pub, cfg: cfg}
}

// SetInterval overrides the sweep interval. Intended for use in tests only.
func (z *ZombieRecovery) SetInterval(d time.Duration) { z.cfg.ZombieInterval = d }

// Run starts the periodic sweep and blocks until ctx is cancelled.
func (z *ZombieRecovery) Run(ctx context.Context) {
	slog.Info("zombie recovery started",
		"component", "recovery",
		"interval", z.cfg.ZombieInterval,
		"threshold_min", z.cfg.ZombieThreshold,
	)
	ticker := time.NewTicker(z.cfg.ZombieInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("zombie recovery stopped", "component", "recovery")
			return
		case <-ticker.C:
			z.sweep(ctx)
		}
	}
}

func (z *ZombieRecovery) sweep(ctx context.Context) {
	slog.Debug("zombie sweep started",
		"component", "recovery",
		"threshold_min", z.cfg.ZombieThreshold,
	)

	stuck, err := z.store.StuckProcessing(ctx, z.cfg.ZombieThreshold)
	if err != nil {
		slog.Error("StuckProcessing query failed",
			"component", "recovery",
			"error", err,
		)
		return
	}

	if len(stuck) == 0 {
		slog.Debug("zombie sweep complete — no zombies found", "component", "recovery")
		return
	}

	slog.Info("zombie sweep found stuck notifications",
		"component", "recovery",
		"count", len(stuck),
	)

	for _, n := range stuck {
		n.Status = model.StatusPending
		n.UpdatedAt = time.Now().UTC()
		if err := z.store.Update(ctx, n); err != nil {
			slog.Error("store.Update failed for zombie",
				"component", "recovery",
				"notification_id", n.ID,
				"error", err,
			)
			continue
		}
		msg := model.DeliveryMessage{NotificationID: n.ID, RetryCount: n.RetryCount}
		if err := z.publisher.Publish(ctx, msg); err != nil {
			slog.Error("Publish failed for zombie",
				"component", "recovery",
				"notification_id", n.ID,
				"error", err,
			)
			continue
		}
		slog.Info("zombie requeued",
			"component", "recovery",
			"notification_id", n.ID,
			"retry_count", n.RetryCount,
		)
	}
}
