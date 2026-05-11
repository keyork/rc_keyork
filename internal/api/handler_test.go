package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/keyork/rc_keyork/internal/api"
	dbmock "github.com/keyork/rc_keyork/internal/db/mock"
	mqmock "github.com/keyork/rc_keyork/internal/mq/mock"
	"github.com/keyork/rc_keyork/internal/model"
)

func newTestServer() (*httptest.Server, *dbmock.Store, *mqmock.Queue) {
	store := dbmock.New()
	queue := mqmock.New()
	h := api.NewHandler(store, queue, api.HandlerConfig{MaxRetries: 8, DefaultPageSize: 50})
	return httptest.NewServer(api.NewServeMux(h)), store, queue
}

func TestSubmitAccepted(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	body, _ := json.Marshal(model.SubmitRequest{
		TargetURL:    "https://httpbin.org/post",
		Method:       "POST",
		SourceSystem: "test",
	})
	resp, err := http.Post(srv.URL+"/api/v1/notifications", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202 got %d", resp.StatusCode)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if result["notification_id"] == "" {
		t.Fatal("expected notification_id in response")
	}
}

func TestSubmitRejectsBadMethod(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	body, _ := json.Marshal(model.SubmitRequest{
		TargetURL: "https://httpbin.org/get",
		Method:    "GET",
	})
	resp, _ := http.Post(srv.URL+"/api/v1/notifications", "application/json", bytes.NewReader(body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 got %d", resp.StatusCode)
	}
}

func TestSubmitRejectsHTTP(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	body, _ := json.Marshal(model.SubmitRequest{
		TargetURL: "http://example.com/api",
		Method:    "POST",
	})
	resp, _ := http.Post(srv.URL+"/api/v1/notifications", "application/json", bytes.NewReader(body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 got %d", resp.StatusCode)
	}
}

// TestGetExisting verifies the previously broken pathSegment index bug is fixed:
// GET /api/v1/notifications/{id} must return the notification, not 404.
func TestGetExisting(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	// Submit a notification to get an ID.
	body, _ := json.Marshal(model.SubmitRequest{
		TargetURL: "https://httpbin.org/post",
		Method:    "POST",
	})
	r, err := http.Post(srv.URL+"/api/v1/notifications", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var sub map[string]string
	json.NewDecoder(r.Body).Decode(&sub)
	id := sub["notification_id"]

	// Now GET it — this used to always return 404 due to wrong segment index.
	resp, err := http.Get(srv.URL + "/api/v1/notifications/" + id)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 got %d", resp.StatusCode)
	}
	var n model.Notification
	json.NewDecoder(resp.Body).Decode(&n)
	if n.ID != id {
		t.Fatalf("want id=%s got %s", id, n.ID)
	}
}

func TestGetNotFound(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/api/v1/notifications/ntf_doesnotexist")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404 got %d", resp.StatusCode)
	}
}

func TestListEmpty(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/notifications")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 got %d", resp.StatusCode)
	}
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"].(float64) != 0 {
		t.Fatal("expected empty list")
	}
}

func TestRetryOnlyAllowedForFailed(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	body, _ := json.Marshal(model.SubmitRequest{
		TargetURL: "https://httpbin.org/post",
		Method:    "POST",
	})
	r, _ := http.Post(srv.URL+"/api/v1/notifications", "application/json", bytes.NewReader(body))
	var sub map[string]string
	json.NewDecoder(r.Body).Decode(&sub)
	id := sub["notification_id"]

	// Notification is "pending" — retry should return 409.
	resp, _ := http.Post(srv.URL+"/api/v1/notifications/"+id+"/retry", "application/json", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("want 409 got %d", resp.StatusCode)
	}
}

func TestRetryOnFailed(t *testing.T) {
	srv, store, _ := newTestServer()
	defer srv.Close()

	// Manually insert a failed notification.
	n := &model.Notification{
		ID:           "ntf_manual_fail",
		TargetURL:    "https://httpbin.org/post",
		Method:       "POST",
		Status:       model.StatusFailed,
		MaxRetries:   8,
		TargetDomain: "httpbin.org",
	}
	_ = store.Create(context.Background(), n)

	resp, _ := http.Post(srv.URL+"/api/v1/notifications/ntf_manual_fail/retry", "application/json", nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202 got %d", resp.StatusCode)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 got %d", resp.StatusCode)
	}
}
