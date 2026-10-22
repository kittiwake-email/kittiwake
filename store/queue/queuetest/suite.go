// Package queuetest is the conformance suite every queue store must pass.
package queuetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"dovetail/store/queue"
)

// Factory returns a fresh, empty store. now is the suite's clock: a
// backend whose lease expiry is judged by another clock (the RADOS memory
// client) must read it too.
type Factory func(t *testing.T, now *time.Time) queue.Store

// Run runs every conformance test against stores from f.
func Run(t *testing.T, f Factory) {
	t.Run("QueueLifecycle", func(t *testing.T) { testQueueLifecycle(t, f) })
	t.Run("Scheduler", func(t *testing.T) { testScheduler(t, f) })
}

func testQueueLifecycle(t *testing.T, f Factory) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	s := f(t, &now)
	s.SetNow(func() time.Time { return now })
	s.SetRetryPolicy(2, func(int) time.Duration { return time.Minute })
	ctx := context.Background()

	id1, err := s.Enqueue(ctx, "purge", map[string]string{"mailbox": "a"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Enqueue(ctx, "purge", map[string]string{"mailbox": "b"}, now.Add(time.Hour))
	st, _ := s.Stats(ctx, "purge")
	if st.Pending != 2 {
		t.Fatalf("stats: %+v", st)
	}
	// Only the due task is claimed.
	tasks, err := s.Claim(ctx, "purge", "w1", 10, time.Minute)
	if err != nil || len(tasks) != 1 || tasks[0].ID != id1 || tasks[0].Attempts != 1 || tasks[0].LeaseKey == "" {
		t.Fatalf("claim: %+v %v", tasks, err)
	}
	st, _ = s.Stats(ctx, "purge")
	if st.Pending != 1 || st.Leased != 1 {
		t.Fatalf("stats after claim: %+v", st)
	}
	// Another worker gets nothing.
	tasks2, _ := s.Claim(ctx, "purge", "w2", 10, time.Minute)
	if len(tasks2) != 0 {
		t.Fatal("double claim")
	}
	// Fail -> retry with backoff.
	dead, err := s.Fail(ctx, tasks[0], errors.New("boom"))
	if err != nil || dead {
		t.Fatal(err)
	}
	st, _ = s.Stats(ctx, "purge")
	if st.Pending != 2 || st.Leased != 0 {
		t.Fatalf("stats after fail: %+v", st)
	}
	if tasks, _ := s.Claim(ctx, "purge", "w1", 10, time.Minute); len(tasks) != 0 {
		t.Fatal("backoff not respected")
	}
	now = now.Add(2 * time.Minute)
	tasks, _ = s.Claim(ctx, "purge", "w1", 10, time.Minute)
	if len(tasks) != 1 || tasks[0].Attempts != 2 || tasks[0].LastError != "boom" {
		t.Fatalf("second claim: %+v", tasks)
	}
	// Fail again -> dead letter.
	dead, _ = s.Fail(ctx, tasks[0], errors.New("boom2"))
	if !dead {
		t.Fatal("expected dead letter")
	}
	dl, _ := s.Dead(ctx, "purge")
	if len(dl) != 1 || dl[0].ID != id1 {
		t.Fatalf("dead: %+v", dl)
	}
	if err := s.Retry(ctx, "purge", id1); err != nil {
		t.Fatal(err)
	}
	if err := s.Retry(ctx, "purge", id1); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("retry twice: %v", err)
	}
	tasks, _ = s.Claim(ctx, "purge", "w1", 10, time.Minute)
	if len(tasks) != 1 || tasks[0].Attempts != 1 {
		t.Fatalf("after retry: %+v", tasks)
	}
	if err := s.Complete(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}
	// Crashed worker: lease expires and task is reclaimed.
	now = now.Add(time.Hour)
	tasks, _ = s.Claim(ctx, "purge", "w1", 10, time.Minute)
	if len(tasks) != 1 {
		t.Fatalf("second task due: %+v", tasks)
	}
	now = now.Add(2 * time.Minute) // lease expired, never completed
	tasks, _ = s.Claim(ctx, "purge", "w3", 10, time.Minute)
	if len(tasks) != 1 || tasks[0].Attempts != 2 {
		t.Fatalf("reclaim: %+v", tasks)
	}
	_ = s.Complete(ctx, tasks[0])
	st, _ = s.Stats(ctx, "purge")
	if st.Pending != 0 || st.Leased != 0 || st.Dead != 0 {
		t.Fatalf("final stats: %+v", st)
	}
	st, _ = s.Stats(ctx, "unknown")
	if st.Pending != 0 {
		t.Fatal("unknown queue")
	}
}

func testScheduler(t *testing.T, f Factory) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	s := f(t, &now)
	s.SetNow(func() time.Time { return now })
	ctx := context.Background()
	if ok, err := s.Lead(ctx, "w1", time.Minute); err != nil || !ok {
		t.Fatalf("first lead: %v %v", ok, err)
	}
	if ok, _ := s.Lead(ctx, "w2", time.Minute); ok {
		t.Fatal("second worker led while the lease is fresh")
	}
	if ok, _ := s.Lead(ctx, "w1", time.Minute); !ok {
		t.Fatal("holder could not renew")
	}
	now = now.Add(2 * time.Minute)
	if ok, _ := s.Lead(ctx, "w2", time.Minute); !ok {
		t.Fatal("expired lease not taken over")
	}
	if due, err := s.Due(ctx, "purge", time.Hour); err != nil || !due {
		t.Fatalf("first due: %v %v", due, err)
	}
	if due, _ := s.Due(ctx, "purge", time.Hour); due {
		t.Fatal("due again within the interval")
	}
	now = now.Add(2 * time.Hour)
	if due, _ := s.Due(ctx, "purge", time.Hour); !due {
		t.Fatal("not due after the interval")
	}
}
