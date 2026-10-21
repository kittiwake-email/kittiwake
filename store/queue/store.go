// Package queue is the RADOS-backed task queue for workers: one object per
// task type (`queue/<type>`) in the index pool, tasks as omap entries, and a
// RADOS lock so concurrent workers never claim the same task. Every state
// move is a single atomic omap transaction. See README.md "Workers".
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("queue: not found")
	ErrLocked   = errors.New("queue: locked by another worker")
)

// Task is a unit of background work.
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	RunAfter  time.Time       `json:"run_after"`
	Attempts  int             `json:"attempts"`
	LastError string          `json:"last_error,omitempty"`
	Created   time.Time       `json:"created"`
	// LeaseKey is set on a claimed task and must be passed to Complete/Fail.
	LeaseKey string `json:"-"`
}

// Store is the queue store.
type Store struct {
	io  rados.IOContext
	Now func() time.Time
	// MaxAttempts before a task is dead-lettered.
	MaxAttempts int
	// Backoff returns the delay before retry attempt n (1-based).
	Backoff func(attempt int) time.Duration
}

// New returns a store on the index pool context.
func New(indexPool rados.IOContext) *Store {
	return &Store{
		io: indexPool, Now: time.Now, MaxAttempts: 8,
		Backoff: func(n int) time.Duration {
			d := time.Duration(1<<uint(min(n, 10))) * 30 * time.Second
			if d > 6*time.Hour {
				d = 6 * time.Hour
			}
			return d
		},
	}
}

func queueOID(typ string) string { return "queue/" + typ }

func pendingKey(runAfter time.Time, id string) string {
	return fmt.Sprintf("pending/%020d/%s", runAfter.Unix(), id)
}
func leaseKey(expiry time.Time, id string) string {
	return fmt.Sprintf("lease/%020d/%s", expiry.Unix(), id)
}
func deadKey(id string) string { return "dead/" + id }

// Enqueue adds a task. A zero RunAfter means now. Returns the task id.
func (s *Store) Enqueue(ctx context.Context, typ string, payload any, runAfter time.Time) (string, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	now := s.Now().UTC()
	if runAfter.IsZero() {
		runAfter = now
	}
	t := Task{ID: uuid.NewString(), Type: typ, Payload: b, RunAfter: runAfter.UTC(), Created: now}
	data, _ := json.Marshal(&t)
	_, err = s.io.Write(ctx, queueOID(typ), rados.NewWriteTx().OmapSet(map[string][]byte{pendingKey(t.RunAfter, t.ID): data}))
	return t.ID, err
}

// Claim takes up to max due tasks for the worker, leasing them for lease.
// Expired leases of crashed workers are reclaimed first.
func (s *Store) Claim(ctx context.Context, typ, worker string, max int, lease time.Duration) ([]Task, error) {
	oid := queueOID(typ)
	if err := s.io.Lock(ctx, oid, "claim", worker, 30*time.Second); err != nil {
		if errors.Is(err, rados.ErrLocked) {
			return nil, ErrLocked
		}
		return nil, err
	}
	defer func() { _ = s.io.Unlock(ctx, oid, "claim", worker) }()

	now := s.Now().UTC()
	// Reclaim expired leases.
	kv, _, err := s.io.OmapList(ctx, oid, "lease/", "", 0)
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return nil, err
	}
	reclaim := map[string][]byte{}
	var rmLease []string
	for _, e := range kv {
		var exp int64
		var id string
		if _, err := fmt.Sscanf(strings.TrimPrefix(e.Key, "lease/"), "%d/%s", &exp, &id); err != nil {
			continue
		}
		if time.Unix(exp, 0).After(now) {
			continue
		}
		var t Task
		if err := json.Unmarshal(e.Value, &t); err != nil {
			continue
		}
		t.RunAfter = now
		b, _ := json.Marshal(&t)
		reclaim[pendingKey(now, t.ID)] = b
		rmLease = append(rmLease, e.Key)
	}
	if len(rmLease) > 0 {
		if _, err := s.io.Write(ctx, oid, rados.NewWriteTx().OmapRemove(rmLease...).OmapSet(reclaim)); err != nil {
			return nil, err
		}
	}
	// Claim due tasks.
	kv, _, err = s.io.OmapList(ctx, oid, "pending/", "", max)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var tasks []Task
	set := map[string][]byte{}
	var rm []string
	expiry := now.Add(lease)
	for _, e := range kv {
		var t Task
		if err := json.Unmarshal(e.Value, &t); err != nil {
			continue
		}
		if t.RunAfter.After(now) {
			break // keys are ordered by run_after
		}
		t.LeaseKey = leaseKey(expiry, t.ID)
		t.Attempts++
		b, _ := json.Marshal(&t)
		set[t.LeaseKey] = b
		rm = append(rm, e.Key)
		tasks = append(tasks, t)
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	if _, err := s.io.Write(ctx, oid, rados.NewWriteTx().OmapRemove(rm...).OmapSet(set)); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Complete removes a claimed task.
func (s *Store) Complete(ctx context.Context, t Task) error {
	_, err := s.io.Write(ctx, queueOID(t.Type), rados.NewWriteTx().OmapRemove(t.LeaseKey))
	return err
}

// Fail records the error and re-queues with backoff, or dead-letters the
// task after MaxAttempts. Returns true if dead-lettered.
func (s *Store) Fail(ctx context.Context, t Task, cause error) (bool, error) {
	t.LastError = cause.Error()
	now := s.Now().UTC()
	tx := rados.NewWriteTx().OmapRemove(t.LeaseKey)
	dead := t.Attempts >= s.MaxAttempts
	if dead {
		b, _ := json.Marshal(&t)
		tx.OmapSet(map[string][]byte{deadKey(t.ID): b})
	} else {
		t.RunAfter = now.Add(s.Backoff(t.Attempts))
		b, _ := json.Marshal(&t)
		tx.OmapSet(map[string][]byte{pendingKey(t.RunAfter, t.ID): b})
	}
	_, err := s.io.Write(ctx, queueOID(t.Type), tx)
	return dead, err
}

// Stats summarises a queue.
type Stats struct {
	Pending int
	Leased  int
	Dead    int
}

// Stats counts entries per state.
func (s *Store) Stats(ctx context.Context, typ string) (Stats, error) {
	var st Stats
	for _, p := range []struct {
		prefix string
		n      *int
	}{{"pending/", &st.Pending}, {"lease/", &st.Leased}, {"dead/", &st.Dead}} {
		kv, _, err := s.io.OmapList(ctx, queueOID(typ), p.prefix, "", 0)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				return st, nil
			}
			return st, err
		}
		*p.n = len(kv)
	}
	return st, nil
}

// Dead lists dead-lettered tasks.
func (s *Store) Dead(ctx context.Context, typ string) ([]Task, error) {
	kv, _, err := s.io.OmapList(ctx, queueOID(typ), "dead/", "", 0)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var out []Task
	for _, e := range kv {
		var t Task
		if err := json.Unmarshal(e.Value, &t); err == nil {
			out = append(out, t)
		}
	}
	return out, nil
}

// Retry moves a dead task back to pending with attempts reset.
func (s *Store) Retry(ctx context.Context, typ, id string) error {
	m, err := s.io.OmapGet(ctx, queueOID(typ), []string{deadKey(id)})
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	v, ok := m[deadKey(id)]
	if !ok {
		return ErrNotFound
	}
	var t Task
	if err := json.Unmarshal(v, &t); err != nil {
		return err
	}
	t.Attempts = 0
	t.LastError = ""
	t.RunAfter = s.Now().UTC()
	b, _ := json.Marshal(&t)
	_, err = s.io.Write(ctx, queueOID(typ), rados.NewWriteTx().OmapRemove(deadKey(id)).OmapSet(map[string][]byte{pendingKey(t.RunAfter, t.ID): b}))
	return err
}
