// Package config loads service configuration from environment variables.
// All variables have safe defaults so the service can start with MOCK=true
// and zero additional configuration.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config is the top-level configuration container. It is populated once at
// startup by Load() and treated as read-only thereafter.
type Config struct {
	// Role controls which subsystems are started: "api", "worker", or "all".
	Role string
	// Mock replaces real RabbitMQ and PostgreSQL with in-memory fakes.
	Mock bool

	Log struct {
		// Level sets the minimum log level: "debug", "info", "warn", "error".
		Level string
		// Format selects the output format: "text" (human-readable) or "json".
		Format string
	}

	HTTP struct {
		Addr string
	}

	DB struct {
		DSN string
	}

	MQ struct {
		URL string
	}

	Notification struct {
		// MaxRetries is the maximum number of delivery attempts before a
		// notification is moved to the dead-letter queue.
		MaxRetries int
		// DefaultPageSize is the default number of results returned by List
		// when the caller does not specify a page size.
		DefaultPageSize int
	}

	Worker struct {
		// Concurrency is the size of the goroutine pool that runs deliveries.
		Concurrency int
		// HTTPTimeout is the per-request timeout for outbound HTTP calls to
		// external vendor APIs (connect + read combined).
		HTTPTimeout time.Duration
		// ZombieInterval is how often the zombie-recovery sweep runs.
		ZombieInterval time.Duration
		// ZombieThreshold is how many minutes a notification must be stuck in
		// "processing" before it is considered a zombie and re-queued.
		ZombieThreshold int
		// CallbackDelays is the sequence of waits between callback attempts.
		// The first element is always 0 (immediate). Subsequent elements are
		// the delays before retry 1, 2, 3, …
		CallbackDelays []time.Duration
	}

	CB struct {
		// WindowDur is the width of the sliding window used to measure failure rate.
		WindowDur time.Duration
		// MinRequests is the minimum number of calls within the window before the
		// breaker can trip.
		MinRequests int
		// FailureRatio is the fraction of failed calls that triggers the breaker.
		FailureRatio float64
		// OpenDur is how long the breaker stays open before allowing a probe
		// request through (half-open state).
		OpenDur time.Duration
	}

	Shutdown struct {
		// GracePeriod is the maximum time to wait for in-flight HTTP requests to
		// complete after a SIGTERM / SIGINT is received.
		GracePeriod time.Duration
	}
}

// Load reads all configuration from environment variables and returns a fully
// populated Config. Unknown or unparseable variable values are logged and the
// default is used in their place.
func Load() *Config {
	c := &Config{}

	c.Role = getenv("ROLE", "all")
	c.Mock = getenv("MOCK", "false") == "true"

	c.Log.Level = getenv("LOG_LEVEL", "info")
	c.Log.Format = getenv("LOG_FORMAT", "text")

	c.HTTP.Addr = getenv("HTTP_ADDR", ":8080")

	// Credentials are intentionally left out of defaults; operators must
	// supply DB_DSN and MQ_URL in non-mock deployments.
	c.DB.DSN = getenv("DB_DSN", "")
	c.MQ.URL = getenv("MQ_URL", "")

	c.Notification.MaxRetries = getenvInt("NOTIFICATION_MAX_RETRIES", 8)
	c.Notification.DefaultPageSize = getenvInt("NOTIFICATION_PAGE_SIZE", 50)

	c.Worker.Concurrency = getenvInt("WORKER_CONCURRENCY", 100)
	c.Worker.HTTPTimeout = getenvDur("WORKER_HTTP_TIMEOUT", 30*time.Second)
	c.Worker.ZombieInterval = getenvDur("WORKER_ZOMBIE_INTERVAL", 5*time.Minute)
	c.Worker.ZombieThreshold = getenvInt("WORKER_ZOMBIE_THRESHOLD", 10)
	c.Worker.CallbackDelays = []time.Duration{0, time.Second, 5 * time.Second, 30 * time.Second}

	c.CB.WindowDur = getenvDur("CB_WINDOW", 5*time.Minute)
	c.CB.MinRequests = getenvInt("CB_MIN_REQUESTS", 10)
	c.CB.FailureRatio = getenvFloat("CB_FAILURE_RATIO", 0.8)
	c.CB.OpenDur = getenvDur("CB_OPEN_DUR", 60*time.Second)

	c.Shutdown.GracePeriod = getenvDur("SHUTDOWN_GRACE_PERIOD", 30*time.Second)

	return c
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid env var, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return n
}

func getenvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		slog.Warn("invalid env var, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return f
}

func getenvDur(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid env var, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return d
}
