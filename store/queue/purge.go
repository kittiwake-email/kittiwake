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
	EnqueueDuePurges(context.Context, int) (int, error)
	ReconcilePurgeSchedule(context.Context, int, time.Duration) error
}

func SchedulePurge(ctx context.Context, q Store, mb uuid.UUID, at time.Time) error {
	if s, ok := q.(PurgeScheduler); ok {
		return s.SchedulePurge(ctx, mb, at)
	}
	return nil
}
