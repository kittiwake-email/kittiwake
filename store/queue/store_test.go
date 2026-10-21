package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"dovetail/rados"
)

func TestQueueLifecycle(t *testing.T) {
	c := rados.NewMemoryClient()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	c.Now = func() time.Time { return now }
	io, _ := c.Pool("index")
	s := New(io)
	s.Now = func() time.Time { return now }
	s.MaxAttempts = 2
	s.Backoff = func(int) time.Duration { return time.Minute }
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
	if err := s.Retry(ctx, "purge", id1); !errors.Is(err, ErrNotFound) {
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
