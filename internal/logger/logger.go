// Package logger initialises the process-wide structured logger (log/slog).
//
// Call Init once at startup (typically from main). All other packages use the
// slog package-level functions (slog.Info, slog.Warn, slog.Error, slog.Debug)
// which delegate to whatever handler Init installed.
//
// Log levels (controlled by LOG_LEVEL env var):
//
//	DEBUG  – per-request traces, delivery attempts, sweep ticks (verbose)
//	INFO   – normal operational events: deliveries succeeded, service started
//	WARN   – recoverable problems: bad env var, circuit open, callback failed
//	ERROR  – data-loss risk or unexpected failures: DB write failed, MQ unavailable
//
// Log formats (controlled by LOG_FORMAT env var):
//
//	text  – human-readable key=value lines (default, good for local dev)
//	json  – structured JSON objects (good for log aggregators like Loki / CloudWatch)
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// Init sets the default slog logger according to the supplied level and format.
// It must be called before any other package emits log output.
func Init(level, format string) {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// parseLevel maps a string such as "debug" or "WARN" to the corresponding
// slog.Level. Unknown values default to INFO.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
