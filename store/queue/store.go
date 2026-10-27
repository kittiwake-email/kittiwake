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

	"kittiwake/rados"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("queue: not found")
	ErrLocked   = errors.New("queue: locked by another worker")
	// ErrLeaseLost is returned by Extend when the task is no longer leased
	// to the caller: the lease expired and another worker reclaimed it.
	ErrLeaseLost = errors.New("queue: lease lost")
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

// Store is what workers and services need from a task queue. Two
// backends exist: MariaDB (production) and RADOS (development, tests).
// Lead and Due back the scheduler: one worker leads at a time and
// enqueues periodic tasks whose interval has elapsed.
type Store interface {
	SetNow(func() time.Time)
	// SetRetryPolicy sets attempts before dead-lettering and the backoff.
	SetRetryPolicy(maxAttempts int, backoff func(attempt int) time.Duration)
	Enqueue(ctx context.Context, typ string, payload any, runAfter time.Time) (string, error)
	Claim(ctx context.Context, typ, worker string, max int, lease time.Duration) ([]Task, error)
	Complete(ctx context.Context, t Task) error
	Fail(ctx context.Context, t Task, cause error) (bool, error)
	// Extend renews the lease of a running task for another lease
	// duration (updating t.LeaseKey) or returns ErrLeaseLost.
	Extend(ctx context.Context, t *Task, lease time.Duration) error
	// Release hands a claimed task back unchanged, due now, without
	// counting a failure: a worker shutting down calls it so the next
	// worker continues at once instead of after the lease expires.
	Release(ctx context.Context, t Task) error
	Stats(ctx context.Context, typ string) (Stats, error)
	Dead(ctx context.Context, typ string) ([]Task, error)
	Retry(ctx context.Context, typ, id string) error
	// Lead tries to take (or renew) the scheduler lease for the worker.
	Lead(ctx context.Context, worker string, ttl time.Duration) (bool, error)
	// Due reports whether the periodic task `name` has not run within
	// `every`, and records now as its last run when it is due.
	Due(ctx context.Context, name string, every time.Duration) (bool, error)
}

// DefaultBackoff is the retry delay before attempt n (1-based).
func DefaultBackoff(n int) time.Duration {
	d := time.Duration(1<<uint(min(n, 10))) * 30 * time.Second
	if d > 6*time.Hour {
		d = 6 * time.Hour
	}
	return d
}

// DefaultMaxAttempts before a task is dead-lettered.
const DefaultMaxAttempts = 8

// Rados is the queue on a RADOS index pool.
type Rados struct {
	io  rados.IOContext
	Now func() time.Time
	// MaxAttempts before a task is dead-lettered.
	MaxAttempts int
	// Backoff returns the delay before retry attempt n (1-based).
	Backoff func(attempt int) time.Duration
}

// New returns the RADOS queue on the index pool context.
func New(indexPool rados.IOContext) *Rados {
	return &Rados{io: indexPool, Now: time.Now, MaxAttempts: DefaultMaxAttempts, Backoff: DefaultBackoff}
}

// SetNow installs the clock.
func (s *Rados) SetNow(now func() time.Time) { s.Now = now }

// SetRetryPolicy sets attempts before dead-lettering and the backoff.
func (s *Rados) SetRetryPolicy(maxAttempts int, backoff func(int) time.Duration) {
	s.MaxAttempts, s.Backoff = maxAttempts, backoff
}

const schedulerOID = "scheduler"

// Lead takes the scheduler lock object for ttl (renewable by the holder).
func (s *Rados) Lead(ctx context.Context, worker string, ttl time.Duration) (bool, error) {
	err := s.io.Lock(ctx, schedulerOID, "leader", worker, ttl)
	if errors.Is(err, rados.ErrLocked) {
		return false, nil
	}
	return err == nil, err
}

// Due keeps last-run timestamps in the scheduler object's omap so a new
// leader continues where the old one stopped.
func (s *Rados) Due(ctx context.Context, name string, every time.Duration) (bool, error) {
	now := s.Now()
	m, err := s.io.OmapGet(ctx, schedulerOID, []string{name})
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return false, err
	}
	if v, ok := m[name]; ok {
		var last time.Time
		if last.UnmarshalText(v) == nil && now.Sub(last) < every {
			return false, nil
		}
	}
	b, _ := now.UTC().MarshalText()
	_, err = s.io.Write(ctx, schedulerOID, rados.NewWriteTx().OmapSet(map[string][]byte{name: b}))
	return err == nil, err
}

func queueOID(typ string) string { return "queue/" + typ }

func pendingKey(runAfter time.Time, id string) string {
	return fmt.Sprintf("pending/%020d/%s", runAfter.Unix(), id)
}
func leaseKey(expiry time.Time, id string) string {
	return fmt.Sprintf("lease/%020d/%s", expiry.Unix(), id+"/"+uuid.NewString())
}
func deadKey(id string) string { return "dead/" + id }

// Enqueue adds a task. A zero RunAfter means now. Returns the task id.
func (s *Rados) Enqueue(ctx context.Context, typ string, payload any, runAfter time.Time) (string, error) {
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
func (s *Rados) Claim(ctx context.Context, typ, worker string, max int, lease time.Duration) ([]Task, error) {
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
func (s *Rados) Complete(ctx context.Context, t Task) error {
	return s.writeLeased(ctx, t, rados.NewWriteTx().OmapRemove(t.LeaseKey))
}

// Fail records the error and re-queues with backoff, or dead-letters the
// task after MaxAttempts. Returns true if dead-lettered.
func (s *Rados) Fail(ctx context.Context, t Task, cause error) (bool, error) {
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
	err := s.writeLeased(ctx, t, tx)
	return dead, err
}

// Extend renews the lease: the lease entry moves to a key with the new
// expiry. A missing entry means the lease expired and was reclaimed.
func (s *Rados) Extend(ctx context.Context, t *Task, lease time.Duration) error {
	next := leaseKey(s.Now().UTC().Add(lease), t.ID)
	b, _ := json.Marshal(t)
	err := s.writeLeased(ctx, *t, rados.NewWriteTx().OmapRemove(t.LeaseKey).OmapSet(map[string][]byte{next: b}))
	if err == nil {
		t.LeaseKey = next
	}
	return err
}

func (s *Rados) Release(ctx context.Context, t Task) error {
	t.RunAfter = s.Now().UTC()
	b, _ := json.Marshal(&t)
	return s.writeLeased(ctx, t, rados.NewWriteTx().OmapRemove(t.LeaseKey).OmapSet(map[string][]byte{pendingKey(t.RunAfter, t.ID): b}))
}

func (s *Rados) writeLeased(ctx context.Context, t Task, tx *rados.WriteTx) error {
	oid, cookie := queueOID(t.Type), uuid.NewString()
	if err := s.io.Lock(ctx, oid, "claim", cookie, 30*time.Second); err != nil {
		return err
	}
	defer func() { _ = s.io.Unlock(ctx, oid, "claim", cookie) }()
	var expiry int64
	if _, err := fmt.Sscanf(t.LeaseKey, "lease/%d/", &expiry); err != nil || !time.Unix(expiry, 0).After(s.Now()) {
		return ErrLeaseLost
	}
	st, err := s.io.Stat(ctx, oid)
	if err != nil {
		return err
	}
	cur, err := s.io.OmapGet(ctx, oid, []string{t.LeaseKey})
	if err != nil {
		return err
	}
	if _, ok := cur[t.LeaseKey]; !ok {
		return ErrLeaseLost
	}
	_, err = s.io.Write(ctx, oid, rados.NewWriteTx().AssertVersion(st.Version).Append(tx))
	if errors.Is(err, rados.ErrVersionMismatch) {
		return ErrLocked
	}
	return err
}

// Stats summarises a queue.
type Stats struct {
	Pending int
	Leased  int
	Dead    int
}

// Stats counts entries per state.
func (s *Rados) Stats(ctx context.Context, typ string) (Stats, error) {
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
func (s *Rados) Dead(ctx context.Context, typ string) ([]Task, error) {
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
func (s *Rados) Retry(ctx context.Context, typ, id string) error {
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
