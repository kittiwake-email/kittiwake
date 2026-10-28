package mariadb_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkPurgeScheduler10k(b *testing.B) {
	db := open(b)
	ctx := context.Background()
	now := time.Now().UTC()
	for chunk := 0; chunk < 10; chunk++ {
		var values []string
		var args []any
		for i := 0; i < 1000; i++ {
			id := uuid.New()
			values = append(values, "(?,'active',?,?,?)")
			args = append(args, id[:], now, now, now)
		}
		_, err := db.SQL().ExecContext(ctx, "INSERT INTO mailboxes (uuid,state,state_since,created,updated) VALUES "+strings.Join(values, ","), args...)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Run("full-registry", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.ListMailboxes(ctx, func(id uuid.UUID) error {
				return db.EnqueuePeriodic(ctx, "purge-trash", id.String(), map[string]uuid.UUID{"mailbox": id})
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("due-index-idle", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if n, err := db.EnqueueDuePurges(ctx, 512); err != nil || n != 0 {
				b.Fatalf("idle: %d %v", n, err)
			}
		}
	})
}
