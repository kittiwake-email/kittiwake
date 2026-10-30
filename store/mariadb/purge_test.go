package mariadb_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/store/identity"
)

func TestDuePurgeSignalsSurviveLeasedWork(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	ids := identity.NewWith(db)
	ids.Now = func() time.Time { return now }
	mb, err := ids.CreateMailbox(ctx, identity.Mailbox{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SchedulePurge(ctx, mb.ID, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, err := db.EnqueueDuePurges(ctx, "", 10); err != nil || n != 0 {
		t.Fatalf("future: %d %v", n, err)
	}
	if err := db.SchedulePurge(ctx, mb.ID, now); err != nil {
		t.Fatal(err)
	}
	if n, err := db.EnqueueDuePurges(ctx, "", 10); err != nil || n != 1 {
		t.Fatalf("due: %d %v", n, err)
	}
	tasks, err := db.Claim(ctx, "purge-trash", "worker", 1, time.Hour)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %v", tasks, err)
	}
	if err := db.SchedulePurge(ctx, mb.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.EnqueueDuePurges(ctx, "", 10); err != nil {
		t.Fatal(err)
	}
	stats, err := db.Stats(ctx, "purge-trash")
	if err != nil || stats.Pending != 0 || stats.Leased != 1 {
		t.Fatalf("duplicate: %v %v", stats, err)
	}
	if err := db.Complete(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if n, err := db.EnqueueDuePurges(ctx, "", 10); err != nil || n != 1 {
		t.Fatalf("successor lost: %d %v", n, err)
	}
}
func TestPurgeDiscoveryPersistsBoundedProgress(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	ids := identity.NewWith(db)
	var mailboxes []uuid.UUID
	for i := 0; i < 5; i++ {
		mb, err := ids.CreateMailbox(ctx, identity.Mailbox{})
		if err != nil {
			t.Fatal(err)
		}
		mailboxes = append(mailboxes, mb.ID)
	}
	count := func() int {
		var n int
		if err := db.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM mailboxes WHERE purge_after IS NOT NULL").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, want := range []int{2, 4, 5} {
		if err := db.ReconcilePurgeSchedule(ctx, "", 2, 7*24*time.Hour); err != nil {
			t.Fatal(err)
		}
		if got := count(); got != want {
			t.Fatalf("bounded progress: got %d want %d", got, want)
		}
	}
	if _, err := db.SQL().ExecContext(ctx, "UPDATE mailboxes SET purge_after=NULL"); err != nil {
		t.Fatal(err)
	}
	if err := db.ReconcilePurgeSchedule(ctx, "", 2, 7*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 0 {
		t.Fatalf("discovery restarted before interval: %d", got)
	}
}
