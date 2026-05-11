package worker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/keyork/rc_keyork/internal/circuitbreaker"
	dbmock "github.com/keyork/rc_keyork/internal/db/mock"
	"github.com/keyork/rc_keyork/internal/model"
	mqmock "github.com/keyork/rc_keyork/internal/mq/mock"
	"github.com/keyork/rc_keyork/internal/worker"
)

// defaultCB returns a circuit breaker with a high trip threshold so it does
// not interfere with tests that only fire a small number of requests.
func defaultCB() *circuitbreaker.Manager {
	return circuitbreaker.NewManager(circuitbreaker.Config{
		WindowDur:    5 * time.Minute,
		MinRequests:  100,
		FailureRatio: 0.99,
		OpenDur:      60 * time.Second,
	})
}

func defaultWorkerCfg(cb *circuitbreaker.Manager) worker.Config {
	return worker.Config{
		Concurrency:     1,
		HTTPTimeout:     5 * time.Second,
		CB:              cb,
		CallbackDelays:  []time.Duration{0},
		ZombieInterval:  5 * time.Minute,
		ZombieThreshold: 10,
	}
}

func notification(id, targetURL string) *model.Notification {
	return &model.Notification{
		ID:           id,
		TargetURL:    targetURL,
		Method:       "POST",
		Status:       model.StatusPending,
		MaxRetries:   8,
		TargetDomain: "127.0.0.1",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

// runPool runs the pool for dur then waits for completion.
func runPool(store *dbmock.Store, queue *mqmock.Queue, cfg worker.Config, dur time.Duration) {
	pool := worker.NewPool(store, queue, queue, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	_ = pool.Run(ctx)
}

func TestDeliverySuccess(t *testing.T) {
	// Plain HTTP target — HTTPS validation is enforced at the API layer, not the worker.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	store := dbmock.New()
	queue := mqmock.New()
	cb := defaultCB()
	ctx := context.Background()

	n := notification("ntf_ok", target.URL)
	_ = store.Create(ctx, n)
	_ = queue.Publish(ctx, model.DeliveryMessage{NotificationID: "ntf_ok"})

	runPool(store, queue, defaultWorkerCfg(cb), 500*time.Millisecond)

	got, err := store.Get(ctx, "ntf_ok")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.StatusSuccess {
		t.Fatalf("want success got %s (lastErr=%s)", got.Status, got.LastError)
	}
}

func TestDelivery4xxNoRetry(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // 400 — non-retryable
	}))
	defer target.Close()

	store := dbmock.New()
	queue := mqmock.New()
	ctx := context.Background()

	n := notification("ntf_400", target.URL)
	_ = store.Create(ctx, n)
	_ = queue.Publish(ctx, model.DeliveryMessage{NotificationID: "ntf_400"})

	runPool(store, queue, defaultWorkerCfg(defaultCB()), 500*time.Millisecond)

	got, _ := store.Get(ctx, "ntf_400")
	if got.Status != model.StatusFailed {
		t.Fatalf("want failed got %s", got.Status)
	}
	if len(queue.RetryMessages(1)) != 0 {
		t.Fatal("4xx must not produce any retry messages")
	}
}

func TestDelivery5xxQueuesRetry(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // 500 — retryable
	}))
	defer target.Close()

	store := dbmock.New()
	queue := mqmock.New()
	ctx := context.Background()

	n := notification("ntf_500", target.URL)
	_ = store.Create(ctx, n)
	_ = queue.Publish(ctx, model.DeliveryMessage{NotificationID: "ntf_500"})

	runPool(store, queue, defaultWorkerCfg(defaultCB()), 500*time.Millisecond)

	got, _ := store.Get(ctx, "ntf_500")
	if got.Status != model.StatusPending {
		t.Fatalf("want pending (awaiting retry) got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Fatalf("want retry_count=1 got %d", got.RetryCount)
	}
	if len(queue.RetryMessages(1)) == 0 {
		t.Fatal("expected a message queued at retry level 1")
	}
}

func TestZombieRecovery(t *testing.T) {
	store := dbmock.New()
	queue := mqmock.New()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	n := &model.Notification{
		ID:           "ntf_zombie",
		TargetURL:    "https://example.com",
		Method:       "POST",
		Status:       model.StatusProcessing,
		MaxRetries:   8,
		TargetDomain: "example.com",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now().Add(-20 * time.Minute), // stuck for 20 min
	}
	_ = store.Create(ctx, n)

	cfg := worker.Config{ZombieInterval: 50 * time.Millisecond, ZombieThreshold: 10}
	z := worker.NewZombieRecovery(store, queue, cfg)
	go z.Run(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()

	got, _ := store.Get(context.Background(), "ntf_zombie")
	if got.Status != model.StatusPending {
		t.Fatalf("zombie should be reset to pending, got %s", got.Status)
	}
}
