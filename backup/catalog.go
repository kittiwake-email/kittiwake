package backup

import (
	"context"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Catalog is the metadata side of backups: what runs exist, which blobs
// the target holds, which mailbox is due when, and the restore/export
// jobs. MariaDB implements it in production; Memory serves the memory
// backend and tests. A catalog is an accelerator: every fact in it can be
// rebuilt from the manifests on the target.
type Catalog interface {
	// Schedule sets the effective interval and next due time of a mailbox
	// (every 0 = disabled, due ignored).
	Schedule(ctx context.Context, mb uuid.UUID, every time.Duration, due time.Time) error
	// ScheduleMaintenance sets when verification and GC next run.
	ScheduleMaintenance(ctx context.Context, mb uuid.UUID, due time.Time) error
	// BackupState returns the mailbox's backup state.
	BackupState(ctx context.Context, mb uuid.UUID) (State, error)
	// FinishRun records a completed run and the mailbox's next due time.
	FinishRun(ctx context.Context, run Run, every time.Duration, due time.Time) error
	// SkipRun records that nothing changed since the last run.
	SkipRun(ctx context.Context, mb uuid.UUID, every time.Duration, due time.Time) error
	// FailRun records a failed attempt; the mailbox is due again at retry.
	FailRun(ctx context.Context, mb uuid.UUID, msg string, every time.Duration, retry time.Time) error
	// ForceFull makes the next run rewrite every folder entry.
	ForceFull(ctx context.Context, mb uuid.UUID, force bool) error
	// SetVerified records a verification outcome.
	SetVerified(ctx context.Context, mb uuid.UUID, at time.Time, errText string) error

	GetRun(ctx context.Context, mb uuid.UUID, id string) (Run, error)
	// ListRuns returns the runs of a mailbox, newest first.
	ListRuns(ctx context.Context, mb uuid.UUID, max int) ([]Run, error)
	DeleteRun(ctx context.Context, mb uuid.UUID, id string) error
	// ExpiredRuns returns runs whose expiry has passed.
	ExpiredRuns(ctx context.Context, before time.Time, max int) ([]Run, error)
	// RunMailboxes lists every mailbox with at least one run, batched.
	RunMailboxes(ctx context.Context, fn func(uuid.UUID) error) error

	// HaveBlobs reports which of the names the target is known to hold.
	HaveBlobs(ctx context.Context, names []string) (map[string]Blob, error)
	AddBlobs(ctx context.Context, blobs []Blob) error
	// AddBlobRefs records that a mailbox's backups reference the blobs.
	AddBlobRefs(ctx context.Context, mb uuid.UUID, names []string) error
	// DropBlobRefs releases every blob reference of a mailbox.
	DropBlobRefs(ctx context.Context, mb uuid.UUID) error
	// UnreferencedBlobs returns blobs no mailbox references, older than before.
	UnreferencedBlobs(ctx context.Context, before time.Time, max int) ([]Blob, error)
	DeleteBlob(ctx context.Context, name string) error

	PutJob(ctx context.Context, j Job) error
	GetJob(ctx context.Context, id string) (Job, error)
	// ListJobs returns jobs, newest first, optionally for one mailbox or kind.
	ListJobs(ctx context.Context, mb uuid.UUID, kind string, max int) ([]Job, error)
	// ExpiredJobs returns finished jobs past their expiry (exports to delete).
	ExpiredJobs(ctx context.Context, before time.Time, max int) ([]Job, error)
	DeleteJob(ctx context.Context, id string) error

	// EnqueueDue enqueues backup tasks for due mailboxes and maintenance
	// tasks for mailboxes whose maintenance is due, up to limit each.
	EnqueueDue(ctx context.Context, limit int, enqueue func(ctx context.Context, typ string, mb uuid.UUID) error) (int, error)
	// Reconcile schedules mailboxes that have no backup state yet (new
	// mailboxes, mailboxes from before backups existed), spreading their
	// first run over the default interval. It walks the registry in
	// bounded steps, like the purge reconciliation.
	Reconcile(ctx context.Context, limit int, every time.Duration, defaults func(ctx context.Context, mb uuid.UUID) time.Duration) error
	// LeadBackups takes or renews the backup scheduler lease.
	LeadBackups(ctx context.Context, holder string, ttl time.Duration) (bool, error)
	Summary(ctx context.Context) (Summary, error)
}

// Jitter returns d plus up to 10% random spread.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d + time.Duration(rand.Int64N(int64(d/10)+1))
}

// FirstDue spreads the first run of a mailbox over one interval.
func FirstDue(now time.Time, every time.Duration) time.Time {
	if every <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(rand.Int64N(int64(every))))
}

// ---- memory catalog ----

// Memory is the in-process catalog for tests and the memory backend.
type Memory struct {
	mu     sync.Mutex
	Now    func() time.Time
	states map[uuid.UUID]*State
	runs   map[uuid.UUID]map[string]Run
	blobs  map[string]Blob
	refs   map[string]map[uuid.UUID]bool
	jobs   map[string]Job
	lease  struct {
		holder string
		until  time.Time
	}
	// Mailboxes, when set, lists the mailboxes Reconcile considers.
	Mailboxes func(ctx context.Context, fn func(uuid.UUID) error) error
}

// NewMemory returns an empty memory catalog.
func NewMemory() *Memory {
	return &Memory{Now: time.Now, states: map[uuid.UUID]*State{}, runs: map[uuid.UUID]map[string]Run{}, blobs: map[string]Blob{}, refs: map[string]map[uuid.UUID]bool{}, jobs: map[string]Job{}}
}

func (m *Memory) state(mb uuid.UUID) *State {
	s, ok := m.states[mb]
	if !ok {
		s = &State{Mailbox: mb}
		m.states[mb] = s
	}
	return s
}

func (m *Memory) Schedule(_ context.Context, mb uuid.UUID, every time.Duration, due time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state(mb)
	s.Every, s.Scheduled = every, true
	s.Due = due
	if every <= 0 {
		s.Due = time.Time{}
	}
	return nil
}

func (m *Memory) ScheduleMaintenance(_ context.Context, mb uuid.UUID, due time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state(mb).MaintainDue = due
	return nil
}

func (m *Memory) BackupState(_ context.Context, mb uuid.UUID) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.states[mb]; ok {
		return *s, nil
	}
	return State{Mailbox: mb}, nil
}

func (m *Memory) FinishRun(_ context.Context, run Run, every time.Duration, due time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runs[run.Mailbox] == nil {
		m.runs[run.Mailbox] = map[string]Run{}
	}
	m.runs[run.Mailbox][run.ID] = run
	s := m.state(run.Mailbox)
	s.LastRun, s.LastModSeq, s.LastAt, s.Status, s.Error, s.ForceFull = run.ID, run.ModSeq, run.Finished, "ok", "", false
	s.Every, s.Scheduled, s.Due = every, true, due
	return nil
}

func (m *Memory) SkipRun(_ context.Context, mb uuid.UUID, every time.Duration, due time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state(mb)
	s.Status, s.Error = "skipped", ""
	s.Every, s.Scheduled, s.Due = every, true, due
	return nil
}

func (m *Memory) FailRun(_ context.Context, mb uuid.UUID, msg string, every time.Duration, retry time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state(mb)
	s.Status, s.Error = "failed", msg
	s.Every, s.Scheduled, s.Due = every, true, retry
	return nil
}

func (m *Memory) ForceFull(_ context.Context, mb uuid.UUID, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state(mb).ForceFull = force
	return nil
}

func (m *Memory) SetVerified(_ context.Context, mb uuid.UUID, at time.Time, errText string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state(mb)
	s.Verified, s.VerifyError = at, errText
	return nil
}

func (m *Memory) GetRun(_ context.Context, mb uuid.UUID, id string) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.runs[mb][id]; ok {
		return r, nil
	}
	return Run{}, ErrNotFound
}

func (m *Memory) ListRuns(_ context.Context, mb uuid.UUID, max int) ([]Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Run
	for _, r := range m.runs[mb] {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (m *Memory) DeleteRun(_ context.Context, mb uuid.UUID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runs[mb], id)
	if len(m.runs[mb]) == 0 {
		delete(m.runs, mb)
	}
	return nil
}

func (m *Memory) ExpiredRuns(_ context.Context, before time.Time, max int) ([]Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Run
	for _, rs := range m.runs {
		for _, r := range rs {
			if !r.Expires.IsZero() && !r.Expires.After(before) {
				out = append(out, r)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Expires.Before(out[j].Expires) })
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (m *Memory) RunMailboxes(_ context.Context, fn func(uuid.UUID) error) error {
	m.mu.Lock()
	var ids []uuid.UUID
	for mb := range m.runs {
		ids = append(ids, mb)
	}
	m.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (m *Memory) HaveBlobs(_ context.Context, names []string) (map[string]Blob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]Blob{}
	for _, n := range names {
		if b, ok := m.blobs[n]; ok {
			out[n] = b
		}
	}
	return out, nil
}

func (m *Memory) AddBlobs(_ context.Context, blobs []Blob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range blobs {
		if _, ok := m.blobs[b.Name]; !ok {
			m.blobs[b.Name] = b
		}
	}
	return nil
}

func (m *Memory) AddBlobRefs(_ context.Context, mb uuid.UUID, names []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range names {
		if m.refs[n] == nil {
			m.refs[n] = map[uuid.UUID]bool{}
		}
		m.refs[n][mb] = true
	}
	return nil
}

func (m *Memory) DropBlobRefs(_ context.Context, mb uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for n, refs := range m.refs {
		delete(refs, mb)
		if len(refs) == 0 {
			delete(m.refs, n)
		}
	}
	return nil
}

func (m *Memory) UnreferencedBlobs(_ context.Context, before time.Time, max int) ([]Blob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Blob
	for n, b := range m.blobs {
		if len(m.refs[n]) == 0 && !b.Created.After(before) {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (m *Memory) DeleteBlob(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, name)
	return nil
}

func (m *Memory) PutJob(_ context.Context, j Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}

func (m *Memory) GetJob(_ context.Context, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[id]; ok {
		return j, nil
	}
	return Job{}, ErrNotFound
}

func (m *Memory) ListJobs(_ context.Context, mb uuid.UUID, kind string, max int) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		if (mb == uuid.Nil || j.Mailbox == mb) && (kind == "" || j.Kind == kind) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created.After(out[j].Created) })
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (m *Memory) ExpiredJobs(_ context.Context, before time.Time, max int) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		if !j.Expires.IsZero() && !j.Expires.After(before) {
			out = append(out, j)
		}
	}
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (m *Memory) DeleteJob(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
	return nil
}

func (m *Memory) EnqueueDue(ctx context.Context, limit int, enqueue func(context.Context, string, uuid.UUID) error) (int, error) {
	now := m.Now().UTC()
	m.mu.Lock()
	var backups, maint []uuid.UUID
	for mb, s := range m.states {
		if s.Scheduled && s.Every > 0 && !s.Due.IsZero() && !s.Due.After(now) && len(backups) < limit {
			backups = append(backups, mb)
			s.Due = now.Add(s.Every) // reset by the task; a lost task retries next interval
		}
		if !s.MaintainDue.IsZero() && !s.MaintainDue.After(now) && len(maint) < limit {
			maint = append(maint, mb)
			s.MaintainDue = now.Add(24 * time.Hour)
		}
	}
	m.mu.Unlock()
	n := 0
	for _, mb := range backups {
		if err := enqueue(ctx, TaskBackup, mb); err != nil {
			return n, err
		}
		n++
	}
	for _, mb := range maint {
		if err := enqueue(ctx, TaskMaintain, mb); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (m *Memory) Reconcile(ctx context.Context, limit int, every time.Duration, defaults func(context.Context, uuid.UUID) time.Duration) error {
	if m.Mailboxes == nil {
		return nil
	}
	now := m.Now().UTC()
	return m.Mailboxes(ctx, func(mb uuid.UUID) error {
		m.mu.Lock()
		s, ok := m.states[mb]
		scheduled := ok && s.Scheduled
		m.mu.Unlock()
		if scheduled {
			return nil
		}
		d := defaults(ctx, mb)
		return m.Schedule(ctx, mb, d, FirstDue(now, d))
	})
}

func (m *Memory) LeadBackups(_ context.Context, holder string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.Now()
	if m.lease.holder == "" || m.lease.holder == holder || !m.lease.until.After(now) {
		m.lease.holder, m.lease.until = holder, now.Add(ttl)
		return true, nil
	}
	return false, nil
}

func (m *Memory) Summary(_ context.Context) (Summary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.Now().UTC()
	var out Summary
	for _, s := range m.states {
		out.Mailboxes++
		if !s.Scheduled {
			continue
		}
		if s.Every <= 0 {
			out.Disabled++
			continue
		}
		out.Scheduled++
		out.Rate += float64(time.Hour) / float64(s.Every)
		if !s.Due.IsZero() && s.Due.Before(now) {
			out.Overdue++
			if out.OldestDue.IsZero() || s.Due.Before(out.OldestDue) {
				out.OldestDue = s.Due
			}
		}
		if s.Status == "failed" {
			out.Failed++
		}
		if s.VerifyError != "" {
			out.VerifyFailed++
		}
	}
	for _, rs := range m.runs {
		for _, r := range rs {
			out.Runs++
			out.Bytes += r.Bytes
		}
	}
	out.Blobs = int64(len(m.blobs))
	for _, j := range m.jobs {
		if j.State == JobPending || j.State == JobRunning {
			out.Jobs++
		}
	}
	return out, nil
}

var _ Catalog = (*Memory)(nil)
