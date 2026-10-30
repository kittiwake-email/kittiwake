package queue

import (
	"context"
	"github.com/google/uuid"
	"time"
)

// PurgeScheduler indexes due mailbox work in the registry. The development
// backend retains periodic discovery; production avoids scanning idle mailboxes
// at every purge interval.
type PurgeScheduler interface {
	SchedulePurge(context.Context, uuid.UUID, time.Time) error
	// EnqueueDuePurges enqueues purge tasks for the namespace's due
	// mailboxes; ReconcilePurgeSchedule walks the namespace's mailboxes.
	EnqueueDuePurges(ctx context.Context, namespace string, limit int) (int, error)
	ReconcilePurgeSchedule(ctx context.Context, namespace string, limit int, every time.Duration) error
}

func SchedulePurge(ctx context.Context, q Store, mb uuid.UUID, at time.Time) error {
	if s, ok := q.(PurgeScheduler); ok {
		return s.SchedulePurge(ctx, mb, at)
	}
	return nil
}
