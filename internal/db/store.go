package db

import (
	"context"

	"github.com/keyork/rc_keyork/internal/model"
)

// Store is the persistence interface used by both API and Worker.
type Store interface {
	Create(ctx context.Context, n *model.Notification) error
	Get(ctx context.Context, id string) (*model.Notification, error)
	Update(ctx context.Context, n *model.Notification) error
	List(ctx context.Context, f model.ListFilter) ([]*model.Notification, error)
	// StuckProcessing returns notifications stuck in "processing" beyond the given threshold.
	StuckProcessing(ctx context.Context, thresholdMinutes int) ([]*model.Notification, error)
}
