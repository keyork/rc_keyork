// Package api implements the HTTP layer: request validation, routing, and
// JSON serialisation. It depends on db.Store and mq.Publisher but has no
// knowledge of the delivery logic that lives in the worker package.
package api

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/keyork/rc_keyork/internal/db"
	"github.com/keyork/rc_keyork/internal/model"
	"github.com/keyork/rc_keyork/internal/mq"
)

// HandlerConfig holds values the Handler reads at runtime.
type HandlerConfig struct {
	// MaxRetries is the maximum delivery attempts before dead-lettering.
	MaxRetries int
	// DefaultPageSize is used when the caller omits the ?size= query parameter.
	DefaultPageSize int
}

// Handler holds shared dependencies for all HTTP handlers.
type Handler struct {
	store     db.Store
	publisher mq.Publisher
	cfg       HandlerConfig
}

// NewHandler wires a Handler with the given store, publisher, and config.
func NewHandler(store db.Store, pub mq.Publisher, cfg HandlerConfig) *Handler {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 8
	}
	if cfg.DefaultPageSize <= 0 {
		cfg.DefaultPageSize = 50
	}
	return &Handler{store: store, publisher: pub, cfg: cfg}
}

// Submit handles POST /api/v1/notifications.
// It validates the request, persists the notification, and publishes it to the
// delivery queue, returning 202 Accepted immediately.
func (h *Handler) Submit(w http.ResponseWriter, r *http.Request) {
	var req model.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	slog.Debug("submit request received",
		"source_system", req.SourceSystem,
		"target_url", req.TargetURL,
		"method", req.Method,
	)

	domain, err := validateTargetURL(req.TargetURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Method == "" {
		req.Method = "POST"
	}
	if err := validateMethod(req.Method); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateHeaders(req.Headers); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateBody(req.Body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	now := time.Now().UTC()
	n := &model.Notification{
		ID:           newID(),
		TargetURL:    req.TargetURL,
		Method:       strings.ToUpper(req.Method),
		Headers:      req.Headers,
		Body:         req.Body,
		Status:       model.StatusPending,
		MaxRetries:   h.cfg.MaxRetries,
		CallbackURL:  req.CallbackURL,
		SourceSystem: req.SourceSystem,
		TargetDomain: domain,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	ctx := r.Context()
	if err := h.store.Create(ctx, n); err != nil {
		slog.Error("store.Create failed",
			"component", "api",
			"notification_id", n.ID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "failed to persist notification")
		return
	}

	msg := model.DeliveryMessage{NotificationID: n.ID, RetryCount: 0}
	if err := h.publisher.Publish(ctx, msg); err != nil {
		// The notification exists in DB as "pending". Zombie recovery will
		// re-enqueue it within the configured threshold, so delivery is not lost.
		slog.Error("mq.Publish failed after store.Create — zombie recovery will requeue",
			"component", "api",
			"notification_id", n.ID,
			"error", err,
		)
		writeError(w, http.StatusServiceUnavailable, "message queue unavailable")
		return
	}

	slog.Info("notification accepted",
		"component", "api",
		"notification_id", n.ID,
		"target_domain", domain,
		"source_system", req.SourceSystem,
	)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"notification_id": n.ID,
		"status":          "accepted",
	})
}

// Get handles GET /api/v1/notifications/{id}.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v1/notifications/{id}
	// Segments after trim: [api, v1, notifications, {id}] → index 3.
	id := pathSegment(r.URL.Path, 3)

	slog.Debug("get notification", "component", "api", "notification_id", id)

	n, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("notification %s not found", id))
		return
	}
	writeJSON(w, http.StatusOK, n)
}

// List handles GET /api/v1/notifications with optional query filters.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := model.ListFilter{
		Status: q.Get("status"),
		Domain: q.Get("domain"),
		Page:   parseIntQ(q.Get("page"), 1),
		Size:   parseIntQ(q.Get("size"), h.cfg.DefaultPageSize),
	}
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.From = t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.To = t
		}
	}

	slog.Debug("list notifications",
		"component", "api",
		"status", f.Status,
		"domain", f.Domain,
		"page", f.Page,
		"size", f.Size,
	)

	items, err := h.store.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	if items == nil {
		items = []*model.Notification{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

// Retry handles POST /api/v1/notifications/{id}/retry.
// Only notifications with status "failed" may be retried; other statuses
// return 409 Conflict.
func (h *Handler) Retry(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v1/notifications/{id}/retry
	// Segments after trim: [api, v1, notifications, {id}, retry] → index 3.
	id := pathSegment(r.URL.Path, 3)
	ctx := r.Context()

	slog.Debug("manual retry requested", "component", "api", "notification_id", id)

	n, err := h.store.Get(ctx, id)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("notification %s not found", id))
		return
	}
	if n.Status != model.StatusFailed {
		slog.Warn("retry rejected: wrong status",
			"component", "api",
			"notification_id", id,
			"current_status", n.Status,
		)
		writeError(w, http.StatusConflict, "only failed notifications can be retried")
		return
	}

	n.Status = model.StatusPending
	n.RetryCount = 0
	n.NextRetryAt = nil
	n.UpdatedAt = time.Now().UTC()
	if err := h.store.Update(ctx, n); err != nil {
		slog.Error("store.Update failed on manual retry",
			"component", "api",
			"notification_id", n.ID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "failed to update notification")
		return
	}

	msg := model.DeliveryMessage{NotificationID: n.ID, RetryCount: 0}
	if err := h.publisher.Publish(ctx, msg); err != nil {
		slog.Error("mq.Publish failed on manual retry — zombie recovery will requeue",
			"component", "api",
			"notification_id", n.ID,
			"error", err,
		)
		writeError(w, http.StatusServiceUnavailable, "message queue unavailable")
		return
	}

	slog.Info("notification requeued via manual retry",
		"component", "api",
		"notification_id", n.ID,
	)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"notification_id": n.ID,
		"status":          "requeued",
	})
}

// Health handles GET /health and is used by load balancers for readiness checks.
func Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- internal helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Status code is already written; we can only log at this point.
		slog.Error("response encode error", "component", "api", "error", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// pathSegment returns the n-th path segment (0-based) after stripping leading
// and trailing slashes. Returns "" if the index is out of range.
func pathSegment(path string, n int) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if n >= len(parts) {
		return ""
	}
	return parts[n]
}

func parseIntQ(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil && v > 0 {
		return v
	}
	return def
}

// newID generates a collision-resistant notification ID using crypto/rand.
// Format: ntf_<32 hex chars>
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is extremely rare; fall back to timestamp-based ID.
		slog.Warn("crypto/rand failed, using timestamp fallback ID",
			"component", "api",
			"error", err,
		)
		return fmt.Sprintf("ntf_fallback_%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("ntf_%x", b)
}
