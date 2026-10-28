package mariadb_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPeriodicTasksCoalesceAcrossLeases(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.EnqueuePeriodic(ctx, "purge", "mailbox", struct{}{}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	stats, err := db.Stats(ctx, "purge")
	if err != nil || stats.Pending != 1 {
		t.Fatalf("pending: %v %v", stats, err)
	}
	tasks, err := db.Claim(ctx, "purge", "w", 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %v", tasks, err)
	}
	if err := db.EnqueuePeriodic(ctx, "purge", "mailbox", struct{}{}); err != nil {
		t.Fatal(err)
	}
	stats, err = db.Stats(ctx, "purge")
	if err != nil || stats.Pending != 0 || stats.Leased != 1 {
		t.Fatalf("leased duplicate: %v %v", stats, err)
	}
	if err := db.Complete(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}
	if err := db.EnqueuePeriodic(ctx, "purge", "mailbox", struct{}{}); err != nil {
		t.Fatal(err)
	}
	tasks, err = db.Claim(ctx, "purge", "w", 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("next interval: %v %v", tasks, err)
	}
	db.MaxAttempts = 1
	if dead, err := db.Fail(ctx, tasks[0], errors.New("test")); err != nil || !dead {
		t.Fatalf("dead: %v %v", dead, err)
	}
	if err := db.EnqueuePeriodic(ctx, "purge", "mailbox", struct{}{}); err != nil {
		t.Fatal(err)
	}
	stats, err = db.Stats(ctx, "purge")
	if err != nil || stats.Pending != 1 || stats.Dead != 1 {
		t.Fatalf("dead must not block next interval: %v %v", stats, err)
	}
}
