package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/metrics"
	"kittiwake/service/actor"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/queue"
)

// RunnerConfig tunes the backup mode.
type RunnerConfig struct {
	PollInterval  time.Duration
	LeaseDuration time.Duration
	// Concurrency: tasks running at once in this process.
	Concurrency int
	// Batch: due mailboxes enqueued per scheduler tick (sized to what the
	// backup nodes drain in a few ticks, so the queue never holds more
	// than the next moments of work).
	Batch int
	// ExpireInterval between expiry passes (per namespace).
	ExpireInterval time.Duration
	// Namespaces this node serves; nil = every namespace. Scheduling
	// (leases, reconcile, due tasks, expiry) runs per namespace.
	Namespaces []string
}

// nsPayload is the payload of a namespace-wide periodic task.
type nsPayload struct {
	Namespace string `json:"namespace"`
}

// Runner is the `backup` mode: it drains the backup task queues and, when
// it holds the backup scheduler lease, feeds them from the catalog.
type Runner struct {
	ID    string
	cfg   RunnerConfig
	svc   *Service
	queue queue.Store
	log   *slog.Logger
	Now   func() time.Time
	sem   chan struct{}
}

// BackupTask is the payload of a backup task.
type BackupTask struct {
	Mailbox uuid.UUID `json:"mailbox"`
	Force   bool      `json:"force,omitempty"`
	Full    bool      `json:"full,omitempty"`
}

// JobTask names a restore or export job.
type JobTask struct {
	Job string `json:"job"`
}

// NewRunner returns the runner.
func NewRunner(cfg RunnerConfig, svc *Service, q queue.Store, log *slog.Logger) *Runner {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 10 * time.Minute
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 100
	}
	if cfg.ExpireInterval <= 0 {
		cfg.ExpireInterval = time.Hour
	}
	if log == nil {
		log = slog.Default()
	}
	host, _ := os.Hostname()
	return &Runner{ID: fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8]), cfg: cfg, svc: svc, queue: q, log: log, Now: time.Now, sem: make(chan struct{}, cfg.Concurrency)}
}

// Run drains the queues and schedules until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("backup runner starting", slog.String("runner", r.ID), slog.Int("concurrency", r.cfg.Concurrency))
	t := time.NewTicker(r.cfg.PollInterval)
	defer t.Stop()
	var wg sync.WaitGroup
	for {
		r.Schedule(ctx)
		busy := r.RunOnce(ctx, &wg)
		if busy {
			continue
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			r.log.Info("backup runner stopping", slog.String("runner", r.ID))
			return nil
		case <-t.C:
		}
	}
}

// RunOnce claims as many due tasks as there are free slots and runs
// them concurrently. It reports whether anything was claimed.
func (r *Runner) RunOnce(ctx context.Context, wg *sync.WaitGroup) bool {
	busy := false
	for _, typ := range AllTasks {
		free := r.cfg.Concurrency - len(r.sem)
		if free <= 0 {
			return true
		}
		tasks, err := r.queue.Claim(ctx, typ, r.ID, free, r.cfg.LeaseDuration)
		if err != nil {
			if !errors.Is(err, queue.ErrLocked) && ctx.Err() == nil {
				r.log.Warn("claim failed", slog.String("type", typ), slog.String("error", err.Error()))
			}
			continue
		}
		for _, task := range tasks {
			busy = true
			r.sem <- struct{}{}
			wg.Add(1)
			go func(task queue.Task) {
				defer wg.Done()
				defer func() { <-r.sem }()
				r.execute(ctx, task)
			}(task)
		}
	}
	return busy
}

// Wait blocks until running tasks finish (tests).
func (r *Runner) Drain(ctx context.Context) {
	var wg sync.WaitGroup
	for r.RunOnce(ctx, &wg) {
	}
	wg.Wait()
}

// execute mirrors the worker's task execution: lease renewal while the
// task runs, release on shutdown, retry with backoff on failure.
func (r *Runner) execute(ctx context.Context, task queue.Task) {
	start := r.Now()
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	lost := false
	done := make(chan struct{})
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		t := time.NewTicker(r.cfg.LeaseDuration / 4)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-tctx.Done():
				return
			case <-t.C:
			}
			mu.Lock()
			err := r.queue.Extend(ctx, &task, r.cfg.LeaseDuration)
			if errors.Is(err, queue.ErrLeaseLost) {
				lost = true
			}
			mu.Unlock()
			if lost {
				r.log.Warn("task lease lost; stopping", slog.String("type", task.Type), slog.String("id", task.ID))
				cancel()
				return
			}
		}
	}()
	err := r.handle(tctx, task)
	close(done)
	<-renewed
	mu.Lock()
	defer mu.Unlock()
	qctx, qcancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer qcancel()
	if lost {
		return
	}
	if err != nil {
		if ctx.Err() != nil {
			_ = r.queue.Release(qctx, task)
			return
		}
		dead, ferr := r.queue.Fail(qctx, task, err)
		res := "retry"
		if dead {
			res = "dead"
		}
		metrics.Tasks.WithLabelValues(task.Type, res).Inc()
		r.log.Warn("backup task failed", slog.String("type", task.Type), slog.String("id", task.ID), slog.Int("attempt", task.Attempts), slog.Bool("dead", dead), slog.String("error", err.Error()), slog.Any("requeue_error", ferr))
		return
	}
	if err := r.queue.Complete(qctx, task); err != nil {
		r.log.Warn("complete failed", slog.String("type", task.Type), slog.String("error", err.Error()))
	}
	metrics.Tasks.WithLabelValues(task.Type, "ok").Inc()
	r.log.Debug("backup task done", slog.String("type", task.Type), slog.String("id", task.ID), slog.Duration("took", r.Now().Sub(start)))
}

func (r *Runner) handle(ctx context.Context, task queue.Task) error {
	ctx = identity.WithPrimaryReads(actor.With(ctx, actor.System(audit.ProtoCLI)))
	switch task.Type {
	case TaskBackup:
		var bt BackupTask
		if err := json.Unmarshal(task.Payload, &bt); err != nil || bt.Mailbox == uuid.Nil {
			return fmt.Errorf("bad payload: %v", err)
		}
		_, err := r.svc.Backup(ctx, bt.Mailbox, RunOptions{Force: bt.Force, Full: bt.Full})
		if errors.Is(err, ErrDisabled) || errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	case TaskMaintain:
		var bt BackupTask
		if err := json.Unmarshal(task.Payload, &bt); err != nil || bt.Mailbox == uuid.Nil {
			return fmt.Errorf("bad payload: %v", err)
		}
		_, err := r.svc.Maintain(ctx, bt.Mailbox)
		return err
	case TaskExpire:
		var np nsPayload
		_ = json.Unmarshal(task.Payload, &np)
		_, err := r.svc.Expire(ctx, np.Namespace, 1000)
		return err
	case TaskJob:
		var jt JobTask
		if err := json.Unmarshal(task.Payload, &jt); err != nil || jt.Job == "" {
			return fmt.Errorf("bad payload: %v", err)
		}
		return r.job(ctx, jt.Job)
	}
	return fmt.Errorf("unknown task type %q", task.Type)
}

// job runs a restore or export job and records its outcome.
func (r *Runner) job(ctx context.Context, id string) error {
	cat := r.svc.Catalog()
	j, err := cat.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if j.State == JobDone {
		return nil
	}
	j.State = JobRunning
	if err := cat.PutJob(ctx, j); err != nil {
		return err
	}
	var runErr error
	switch j.Kind {
	case JobRestore:
		var res RestoreResult
		res, runErr = r.svc.Restore(ctx, j.Mailbox, j.RunID, RestoreOptions{})
		j.Target, j.Messages = res.Mailbox, res.Messages
		j.Detail, _ = json.Marshal(res)
	case JobRestoreMessage:
		var res RestoreResult
		res, runErr = r.svc.RestoreMessage(ctx, j.Mailbox, j.RunID, j.MessageID, j.Target)
		j.Target, j.Messages = res.Mailbox, res.Messages
	case JobExport:
		var res ExportResult
		res, runErr = r.svc.Export(ctx, j.Mailbox, j.RunID, j.Requested)
		j.ObjectKey, j.Bytes, j.Messages = res.Key, res.Bytes, res.Messages
		j.Expires = r.Now().UTC().Add(r.svc.cfg.ExportExpiry)
		if runErr == nil {
			j.URL = r.svc.DownloadURL(j.Namespace, res.Key, r.svc.cfg.ExportExpiry)
		}
	case JobVerify:
		var rep Report
		rep, runErr = r.svc.Verify(ctx, j.Mailbox, VerifyOptions{Full: true})
		j.Detail, _ = json.Marshal(rep)
		if runErr == nil && !rep.OK {
			j.Error = truncate(rep.problem(), 250)
		}
	case JobForget:
		runErr = r.svc.Forget(ctx, j.Mailbox)
	default:
		runErr = fmt.Errorf("unknown job kind %q", j.Kind)
	}
	if runErr != nil {
		if ctx.Err() != nil {
			j.State = JobPending
			_ = cat.PutJob(ctx, j)
			return runErr
		}
		j.State, j.Error = JobFailed, truncate(runErr.Error(), 250)
		j.Finished = r.Now().UTC()
		if errors.Is(runErr, ErrNotFound) || errors.Is(runErr, ErrForbidden) {
			_ = cat.PutJob(ctx, j)
			return nil // permanent: no retry
		}
		_ = cat.PutJob(ctx, j)
		return runErr
	}
	j.State, j.Finished = JobDone, r.Now().UTC()
	return cat.PutJob(ctx, j)
}

// Schedule runs the scheduler for every namespace this node serves.
func (r *Runner) Schedule(ctx context.Context) {
	sctx := identity.WithPrimaryReads(actor.With(ctx, actor.System(audit.ProtoCLI)))
	nss := r.cfg.Namespaces
	if len(nss) == 0 {
		all, err := r.svc.st.Identity.AllNamespaces(sctx)
		if err != nil {
			r.log.Warn("listing namespaces failed", slog.String("error", err.Error()))
			return
		}
		nss = all
	}
	for _, ns := range nss {
		if !r.svc.Configured(ns) {
			continue // no target for this namespace on this node
		}
		r.scheduleNamespace(sctx, ns)
	}
}

// scheduleNamespace, when this runner leads the namespace, reconciles
// its new mailboxes into the schedule, enqueues due backups and
// maintenance, and the periodic expiry pass.
func (r *Runner) scheduleNamespace(sctx context.Context, ns string) {
	cat := r.svc.Catalog()
	if lead, err := cat.LeadBackups(sctx, ns, r.ID, 2*r.cfg.PollInterval+time.Minute); err != nil || !lead {
		return
	}
	defaults := func(ctx context.Context, mb uuid.UUID) time.Duration {
		mv, err := r.svc.st.Identity.GetMailbox(ctx, mb)
		if err != nil {
			return 0
		}
		every, _ := r.svc.Policy(ctx, mv.Value)
		return every
	}
	if err := cat.Reconcile(sctx, ns, r.cfg.Batch, r.svc.cfg.DefaultInterval, defaults); err != nil {
		r.log.Warn("backup reconcile failed", slog.String("namespace", ns), slog.String("error", err.Error()))
	}
	n, err := cat.EnqueueDue(sctx, ns, r.cfg.Batch, func(ctx context.Context, typ string, mb uuid.UUID) error {
		return r.enqueuePeriodic(ctx, typ, mb.String(), BackupTask{Mailbox: mb})
	})
	if err != nil {
		r.log.Warn("backup scheduling failed", slog.String("namespace", ns), slog.String("error", err.Error()))
	} else if n > 0 {
		r.log.Debug("scheduled backup tasks", slog.String("namespace", ns), slog.Int("tasks", n))
	}
	if due, err := r.queue.Due(sctx, ns, TaskExpire, r.cfg.ExpireInterval); err == nil && due {
		_ = r.enqueuePeriodic(sctx, TaskExpire, ns, nsPayload{Namespace: ns})
	}
}

func (r *Runner) enqueuePeriodic(ctx context.Context, typ, scope string, payload any) error {
	if q, ok := r.queue.(interface {
		EnqueuePeriodic(context.Context, string, string, any) error
	}); ok {
		return q.EnqueuePeriodic(ctx, typ, scope, payload)
	}
	_, err := r.queue.Enqueue(ctx, typ, payload, time.Time{})
	return err
}
