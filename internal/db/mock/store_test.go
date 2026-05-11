package mock_test

import (
	"context"
	"testing"
	"time"

	"github.com/keyork/rc_keyork/internal/db/mock"
	"github.com/keyork/rc_keyork/internal/model"
)

func makeN(id string) *model.Notification {
	return &model.Notification{
		ID:           id,
		TargetURL:    "https://example.com",
		Method:       "POST",
		Status:       model.StatusPending,
		TargetDomain: "example.com",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

func TestCreateAndGet(t *testing.T) {
	s := mock.New()
	ctx := context.Background()

	n := makeN("ntf_1")
	if err := s.Create(ctx, n); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(ctx, "ntf_1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "ntf_1" {
		t.Fatalf("want ntf_1 got %s", got.ID)
	}
}

func TestDuplicateCreateFails(t *testing.T) {
	s := mock.New()
	ctx := context.Background()
	n := makeN("ntf_dup")
	_ = s.Create(ctx, n)
	if err := s.Create(ctx, n); err == nil {
		t.Fatal("expected error on duplicate create")
	}
}

func TestUpdate(t *testing.T) {
	s := mock.New()
	ctx := context.Background()
	n := makeN("ntf_upd")
	_ = s.Create(ctx, n)

	n.Status = model.StatusSuccess
	if err := s.Update(ctx, n); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "ntf_upd")
	if got.Status != model.StatusSuccess {
		t.Fatalf("want success got %s", got.Status)
	}
}

func TestListFilter(t *testing.T) {
	s := mock.New()
	ctx := context.Background()

	n1 := makeN("ntf_a")
	n1.Status = model.StatusFailed
	n1.TargetDomain = "a.com"
	_ = s.Create(ctx, n1)

	n2 := makeN("ntf_b")
	n2.Status = model.StatusSuccess
	n2.TargetDomain = "b.com"
	_ = s.Create(ctx, n2)

	items, err := s.List(ctx, model.ListFilter{Status: "failed", Page: 1, Size: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != "ntf_a" {
		t.Fatalf("expected ntf_a, got %v", items)
	}
}

func TestStuckProcessing(t *testing.T) {
	s := mock.New()
	ctx := context.Background()

	n := makeN("ntf_stuck")
	n.Status = model.StatusProcessing
	n.UpdatedAt = time.Now().Add(-20 * time.Minute)
	_ = s.Create(ctx, n)

	stuck, err := s.StuckProcessing(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 1 {
		t.Fatalf("expected 1 stuck notification, got %d", len(stuck))
	}
}
