package api

import (
	"net/http"
	"strings"
)

// NewServeMux wires all routes onto a fresh ServeMux.
func NewServeMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", Health)
	mux.HandleFunc("/api/v1/notifications", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			h.Submit(w, r)
		case http.MethodGet:
			h.List(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	mux.HandleFunc("/api/v1/notifications/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/retry") && r.Method == http.MethodPost {
			h.Retry(w, r)
			return
		}
		if r.Method == http.MethodGet {
			h.Get(w, r)
			return
		}
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	})

	return mux
}
