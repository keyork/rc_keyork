// Package mock provides a thread-safe, in-memory implementation of db.Store.
// It is used by default when the service is started with MOCK=true, and is
// the backing store for all unit and integration tests.
package mock

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/keyork/rc_keyork/internal/model"
)

// Store is the in-memory implementation of db.Store.
type Store struct {
	mu    sync.RWMutex
	items map[string]*model.Notification
}

// New creates an empty Store.
func New() *Store {
	return &Store{items: make(map[string]*model.Notification)}
}

// Create inserts n. Returns an error if a notification with the same ID already exists.
func (s *Store) Create(_ context.Context, n *model.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[n.ID]; ok {
		return fmt.Errorf("notification %s already exists", n.ID)
	}
	cp := *n
	s.items[n.ID] = &cp
	return nil
}

// Get returns a copy of the notification with the given id.
func (s *Store) Get(_ context.Context, id string) (*model.Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.items[id]
	if !ok {
		return nil, fmt.Errorf("notification %s not found", id)
	}
	cp := *n
	return &cp, nil
}

// Update replaces the stored notification. Returns an error if the id does not exist.
func (s *Store) Update(_ context.Context, n *model.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[n.ID]; !ok {
		return fmt.Errorf("notification %s not found", n.ID)
	}
	cp := *n
	s.items[n.ID] = &cp
	return nil
}

// List returns a paginated, filtered slice. It always returns a non-nil slice
// so callers do not need to handle the nil case separately.
func (s *Store) List(_ context.Context, f model.ListFilter) ([]*model.Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var filtered []*model.Notification
	for _, n := range s.items {
		if f.Status != "" && string(n.Status) != f.Status {
			continue
		}
		if f.Domain != "" && n.TargetDomain != f.Domain {
			continue
		}
		if !f.From.IsZero() && n.CreatedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && n.CreatedAt.After(f.To) {
			continue
		}
		cp := *n
		filtered = append(filtered, &cp)
	}

	// Sort by created_at descending for deterministic pagination in tests.
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})

	size := f.Size
	if size <= 0 {
		size = 50
	}
	page := f.Page
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * size
	if start >= len(filtered) {
		return []*model.Notification{}, nil
	}
	end := start + size
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], nil
}

// StuckProcessing returns all notifications in "processing" state whose
// UpdatedAt is older than thresholdMinutes ago.
func (s *Store) StuckProcessing(_ context.Context, thresholdMinutes int) ([]*model.Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cutoff := time.Now().Add(-time.Duration(thresholdMinutes) * time.Minute)
	var out []*model.Notification
	for _, n := range s.items {
		if n.Status == model.StatusProcessing && n.UpdatedAt.Before(cutoff) {
			cp := *n
			out = append(out, &cp)
		}
	}
	return out, nil
}
