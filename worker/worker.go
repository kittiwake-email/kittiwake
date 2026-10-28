// Package worker runs background tasks from the RADOS task queue: retention
// purge, attachment garbage collection, mailbox removal, quota
// recalculation, audit purge, orphan sweep and search-index events. One
// worker at a time (elected with a RADOS lock) also acts as the scheduler
// that enqueues the periodic tasks. See README.md "Workers and background
// tasks".
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/mail"
	"kittiwake/metrics"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/service/export"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
	"kittiwake/store/storage"
)

// Task types handled here (plus those defined by services).
const (
	TaskPurgeTrash    = admin.TaskPurgeTrash
	TaskRemoveMailbox = admin.TaskRemoveMailbox
	TaskUploadSweep   = "upload-sweep"
	TaskRecalcQuota   = admin.TaskRecalcQuota
	TaskSearchIndex   = delivery.TaskSearchIndex
	TaskAttachmentGC  = "attachment-gc"
	TaskPurgeAudit    = "purge-audit"
	TaskOrphanSweep   = "orphan-sweep"
	TaskExportMailbox = "export-mailbox"
)

// AllTypes lists every queue the worker drains.
var AllTypes = []string{TaskPurgeTrash, TaskRemoveMailbox, TaskRecalcQuota, TaskSearchIndex, TaskAttachmentGC, TaskPurgeAudit, TaskOrphanSweep, TaskUploadSweep, TaskExportMailbox}

// ExportTask is the payload of an export-mailbox task.
type ExportTask struct {
	Mailbox   uuid.UUID `json:"mailbox"`
	Gzip      bool      `json:"gzip"`
	Requested string    `json:"requested_by"`
}

// ExportResult is written next to the archive as <name>.json.
type ExportResult struct {
	Mailbox   uuid.UUID `json:"mailbox"`
	File      string    `json:"file"`
	Bytes     int64     `json:"bytes"`
	Messages  int       `json:"messages"`
	Audit     int       `json:"audit"`
	Errors    []string  `json:"errors,omitempty"`
	Started   time.Time `json:"started"`
	Finished  time.Time `json:"finished"`
	Requested string    `json:"requested_by"`
}

// Config for the worker.
type Config struct {
	// PostHoldGrace delays purges after a hold is released.
	PostHoldGrace time.Duration
	// AuditRetention is how long audit objects are kept.
	AuditRetention time.Duration
	// AttachmentGCGrace: unreferenced attachments younger than this are kept
	// (a delivery may still be in flight).
	AttachmentGCGrace time.Duration
	// OrphanAge: message objects older than this without an index entry
	// are removed.
	OrphanAge time.Duration
	// PollInterval between queue checks when idle.
	PollInterval time.Duration
	// LeaseDuration for claimed tasks. A running task renews its lease
	// every quarter of it, so a crashed worker's task is taken over by
	// another worker within one lease duration.
	LeaseDuration time.Duration
	// Schedule intervals for periodic tasks.
	PurgeInterval time.Duration // per-mailbox trash purge
	GCInterval    time.Duration // attachment GC, audit purge
	// OrphanSweepInterval: the orphan sweep lists every message object on
	// every storage (one stat per object), so it runs rarely; the write
	// paths undo their own failures and the sweep only catches crashes.
	OrphanSweepInterval time.Duration
	ScanBatch           int
	// ExportDir receives mailbox export archives (export-mailbox tasks).
	ExportDir string
}

// Worker drains the task queues.
type Worker struct {
	ID    string
	cfg   Config
	ids   *identity.Store
	idx   *index.Store
	msgs  *message.Store
	atts  *attach.Store
	audit *audit.Store
	queue queue.Store
	aud   *auditor.Auditor
	log   *slog.Logger
	Now   func() time.Time
	// Export is optional; without it export-mailbox tasks fail.
	Export *export.Service
	// afterStep is a test hook called after each message a removal purged.
	afterStep func(n int)
}

// Stores needed by the worker.
type Stores struct {
	Identity    *identity.Store
	Index       *index.Store
	Messages    *message.Store
	Attachments *attach.Store
	Audit       *audit.Store
	Queue       queue.Store
}

// New returns a worker.
func New(cfg Config, st Stores, aud *auditor.Auditor, log *slog.Logger) *Worker {
	st.Attachments.BindIndex(st.Index)
	if cfg.PostHoldGrace <= 0 {
		cfg.PostHoldGrace = 7 * 24 * time.Hour
	}
	if cfg.AuditRetention <= 0 {
		cfg.AuditRetention = 365 * 24 * time.Hour
	}
	if cfg.AttachmentGCGrace <= 0 {
		cfg.AttachmentGCGrace = 24 * time.Hour
	}
	if cfg.OrphanAge <= 0 {
		cfg.OrphanAge = 24 * time.Hour
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 2 * time.Minute
	}
	if cfg.PurgeInterval <= 0 {
		cfg.PurgeInterval = time.Hour
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = 24 * time.Hour
	}
	if cfg.OrphanSweepInterval <= 0 {
		cfg.OrphanSweepInterval = 7 * 24 * time.Hour
	}
	if cfg.ScanBatch <= 0 {
		cfg.ScanBatch = 500
	}
	if log == nil {
		log = slog.Default()
	}
	host, _ := os.Hostname()
	return &Worker{
		ID: fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8]), cfg: cfg,
		ids: st.Identity, idx: st.Index, msgs: st.Messages, atts: st.Attachments, audit: st.Audit, queue: st.Queue,
		aud: aud, log: log, Now: time.Now,
	}
}

// Run drains queues and schedules periodic tasks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker starting", slog.String("worker", w.ID))
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	for {
		busy := w.RunOnce(ctx)
		w.schedule(ctx)
		if busy {
			continue // drain without waiting
		}
		select {
		case <-ctx.Done():
			w.log.Info("worker stopping", slog.String("worker", w.ID))
			return nil
		case <-t.C:
		}
	}
}

// RunOnce claims and executes due tasks from every queue once. Returns
// whether any task ran.
func (w *Worker) RunOnce(ctx context.Context) bool {
	busy := false
	for _, typ := range AllTypes {
		tasks, err := w.queue.Claim(ctx, typ, w.ID, 1, w.cfg.LeaseDuration)
		if err != nil {
			if !errors.Is(err, queue.ErrLocked) && ctx.Err() == nil {
				w.log.Warn("claim failed", slog.String("type", typ), slog.String("error", err.Error()))
			}
			continue
		}
		for _, task := range tasks {
			busy = true
			w.execute(ctx, task)
		}
	}
	return busy
}

// execute runs one claimed task. While it runs, the lease is renewed
// every quarter of its duration; a lease that cannot be renewed (another
// worker took the task over after this one looked dead) cancels the run.
// A run cut short by the worker shutting down hands the task back to the
// queue, due at once, so the next worker continues where this one stopped;
// every task is written to be resumable.
func (w *Worker) execute(ctx context.Context, task queue.Task) {
	start := w.Now()
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex // guards task.LeaseKey, which renewal may change
	lost := false
	done := make(chan struct{})
	renewed := make(chan struct{})
	run := task
	go func() {
		defer close(renewed)
		t := time.NewTicker(w.cfg.LeaseDuration / 4)
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
			err := w.queue.Extend(ctx, &task, w.cfg.LeaseDuration)
			if errors.Is(err, queue.ErrLeaseLost) {
				lost = true
			}
			mu.Unlock()
			if lost {
				w.log.Warn("task lease lost; stopping", slog.String("type", task.Type), slog.String("id", task.ID))
				cancel()
				return
			}
			if err != nil {
				w.log.Warn("task lease renewal failed", slog.String("type", task.Type), slog.String("id", task.ID), slog.String("error", err.Error()))
			}
		}
	}()
	err := w.handle(tctx, run)
	close(done)
	<-renewed
	mu.Lock()
	defer mu.Unlock()
	// Queue bookkeeping must outlive a cancelled run context.
	qctx, qcancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer qcancel()
	if lost {
		return // the task belongs to another worker, even if handle returned success
	}
	if err != nil {
		if ctx.Err() != nil {
			if rerr := w.queue.Release(qctx, task); rerr != nil {
				w.log.Warn("task release failed", slog.String("type", task.Type), slog.String("id", task.ID), slog.String("error", rerr.Error()))
				return
			}
			w.log.Info("task released for the next worker", slog.String("type", task.Type), slog.String("id", task.ID), slog.Duration("ran", w.Now().Sub(start)))
			return
		}
		dead, ferr := w.queue.Fail(qctx, task, err)
		res := "retry"
		if dead {
			res = "dead"
		}
		metrics.Tasks.WithLabelValues(task.Type, res).Inc()
		w.log.Warn("task failed", slog.String("type", task.Type), slog.String("id", task.ID), slog.Int("attempt", task.Attempts),
			slog.Bool("dead", dead), slog.String("error", err.Error()), slog.Any("requeue_error", ferr))
		return
	}
	if err := w.queue.Complete(qctx, task); err != nil {
		w.log.Warn("complete failed", slog.String("type", task.Type), slog.String("error", err.Error()))
	}
	metrics.Tasks.WithLabelValues(task.Type, "ok").Inc()
	w.log.Debug("task done", slog.String("type", task.Type), slog.String("id", task.ID), slog.Duration("took", w.Now().Sub(start)))
}

type mailboxPayload struct {
	Mailbox uuid.UUID `json:"mailbox"`
	// StorageID pins the storage for a mailbox whose record is gone
	// (remove-mailbox); zero means "look it up".
	StorageID int64 `json:"storage_id,omitempty"`
	// All makes purge-trash ignore the purge dates: an admin purge removes
	// the whole retention set now.
	All bool `json:"all,omitempty"`
}

func (w *Worker) handle(ctx context.Context, task queue.Task) error {
	ctx = identity.WithPrimaryReads(actor.With(ctx, actor.System(audit.ProtoCLI)))
	var mp mailboxPayload
	switch task.Type {
	case TaskPurgeTrash, TaskRemoveMailbox, TaskRecalcQuota:
		if err := json.Unmarshal(task.Payload, &mp); err != nil || mp.Mailbox == uuid.Nil {
			return fmt.Errorf("bad payload: %v", err)
		}
	}
	switch task.Type {
	case TaskPurgeTrash:
		return w.purgeTrash(ctx, mp.Mailbox, mp.All)
	case TaskRemoveMailbox:
		if mp.StorageID != 0 {
			ctx = storage.WithID(ctx, mp.StorageID)
		}
		if err := w.removeMailbox(ctx, mp.Mailbox); err != nil {
			return err
		}
		return w.ids.FinishMailboxRemoval(ctx, mp.Mailbox)
	case TaskRecalcQuota:
		_, err := w.idx.Recalculate(ctx, mp.Mailbox)
		if errors.Is(err, index.ErrNotFound) {
			return nil
		}
		return err
	case TaskSearchIndex:
		return nil // no search backend configured
	case TaskAttachmentGC:
		return w.attachmentGC(ctx)
	case TaskPurgeAudit:
		return w.purgeAudit(ctx)
	case TaskUploadSweep:
		n, err := w.msgs.SweepUploads(ctx)
		if n > 0 {
			w.log.Info("upload sweep", slog.Int("expired", n))
		}
		return err
	case TaskOrphanSweep:
		if n, err := w.ids.PurgeNodes(ctx, w.Now().Add(-24*time.Hour)); err == nil && n > 0 {
			w.log.Info("purged dead node registrations", slog.Int("rows", n))
		}
		return w.orphanSweep(ctx)
	case TaskExportMailbox:
		var et ExportTask
		if err := json.Unmarshal(task.Payload, &et); err != nil || et.Mailbox == uuid.Nil {
			return fmt.Errorf("bad payload: %v", err)
		}
		return w.exportMailbox(ctx, et)
	}
	return fmt.Errorf("unknown task type %q", task.Type)
}

// exportMailbox writes an archive into ExportDir and a JSON result beside it.
func (w *Worker) exportMailbox(ctx context.Context, et ExportTask) error {
	if w.Export == nil || w.cfg.ExportDir == "" {
		return errors.New("export not configured (export.directory)")
	}
	if err := os.MkdirAll(w.cfg.ExportDir, 0o750); err != nil {
		return err
	}
	started := w.Now().UTC()
	name := fmt.Sprintf("%s-%s.tar", et.Mailbox, started.Format("20060102T150405Z"))
	if et.Gzip {
		name += ".gz"
	}
	final := filepath.Join(w.cfg.ExportDir, name)
	tmp := final + ".part"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	sum, err := w.Export.ExportMailbox(ctx, et.Mailbox, f, export.Options{Gzip: et.Gzip})
	cerr := f.Close()
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if cerr != nil {
		_ = os.Remove(tmp)
		return cerr
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	st, _ := os.Stat(final)
	res := ExportResult{Mailbox: et.Mailbox, File: name, Messages: sum.Messages, Audit: sum.Audit, Errors: sum.Errors, Started: started, Finished: w.Now().UTC(), Requested: et.Requested}
	if st != nil {
		res.Bytes = st.Size()
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	return os.WriteFile(final+".json", b, 0o640)
}

// ---- handlers ----

// purgeTrash deletes the message objects of due retention entries unless
// the mailbox is on hold or within the post-hold grace period.
// purgeTrash deletes the retention entries of a mailbox whose purge date
// has passed, or all of them when all is set (admin purge). Resumable like
// every task: each pass lists what is still there.
func (w *Worker) purgeTrash(ctx context.Context, mb uuid.UUID, all bool) error {
	if v, err := w.ids.GetMailbox(ctx, mb); err == nil {
		if v.Value.Hold != nil {
			w.log.Debug("purge skipped: on hold", slog.String("mailbox", mb.String()))
			return queue.SchedulePurge(ctx, w.queue, mb, w.Now().Add(w.cfg.PurgeInterval))
		}
		if v.Value.Released != nil && w.Now().Sub(*v.Value.Released) < w.cfg.PostHoldGrace {
			w.log.Debug("purge skipped: post-hold grace", slog.String("mailbox", mb.String()))
			return queue.SchedulePurge(ctx, w.queue, mb, v.Value.Released.Add(w.cfg.PostHoldGrace))
		}
	} else if !errors.Is(err, identity.ErrNotFound) {
		return err
	}
	before := w.Now()
	if all {
		before = time.Time{}
	}
	purged := 0
	for {
		entries, keys, more, err := w.idx.ListRetention(ctx, mb, before, w.cfg.ScanBatch)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			live, err := w.idx.IsLive(ctx, mb, e.MessageID, index.Location{FolderID: e.FolderID, UID: e.UID}.String())
			if err != nil {
				return err
			}
			if live {
				return fmt.Errorf("retention entry still indexed: %s", e.MessageID)
			}
			// A long batch must observe holds established after it started.
			v, err := w.ids.GetMailbox(ctx, mb)
			if err != nil && !errors.Is(err, identity.ErrNotFound) {
				return err
			}
			if err == nil && (v.Value.Hold != nil || (v.Value.Released != nil && w.Now().Sub(*v.Value.Released) < w.cfg.PostHoldGrace)) {
				return queue.SchedulePurge(ctx, w.queue, mb, w.Now().Add(w.cfg.PurgeInterval))
			}
			if _, err := w.deleteMessage(ctx, mb, e.MessageID); err != nil {
				return err
			}
			purged++
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := w.dropRetention(ctx, mb, entries, keys); err != nil {
			return err
		}
		if !more {
			break
		}
	}
	if purged > 0 {
		detail := fmt.Sprintf("messages=%d", purged)
		if all {
			detail += " ahead of retention (admin purge)"
		}
		w.aud.Log(ctx, auditor.Event{Mailbox: mb, Action: "purge", Detail: detail})
	}
	if _, ok := w.queue.(queue.PurgeScheduler); ok {
		next, err := w.idx.NextRetention(ctx, mb)
		if err != nil {
			return err
		}
		return queue.SchedulePurge(ctx, w.queue, mb, next)
	}
	return nil
}

// deleteMessage removes attachment references and the message objects.
// It reports whether the message was still there: a resumed task meets
// index records whose objects an earlier pass already deleted.
func (w *Worker) deleteMessage(ctx context.Context, mb, msg uuid.UUID) (bool, error) {
	data, err := w.msgs.GetDoc(ctx, mb, msg)
	existed := err == nil
	if err == nil {
		var doc mail.Document
		if json.Unmarshal(data, &doc) == nil {
			w.log.Info("purged message", slog.String("message_id", doc.MessageID), slog.String("mailbox", mb.String()), slog.String("msg", msg.String()))
		}
	} else if !errors.Is(err, message.ErrNotFound) {
		return false, err
	}
	return existed, w.msgs.Delete(ctx, mb, msg)
}

// removeMailbox purges every object of a mailbox whose record is gone.
// It is resumable: every pass lists what is still there, so a run that
// was cut short (worker restart, lease lost) continues where it stopped
// when the task is claimed again.
func (w *Worker) removeMailbox(ctx context.Context, mb uuid.UUID) error {
	if v, err := w.ids.GetMailbox(ctx, mb); err == nil {
		if v.Value.Hold != nil {
			return fmt.Errorf("mailbox %s on hold; refusing removal", mb)
		}
		return fmt.Errorf("mailbox %s still exists; refusing removal", mb)
	} else if !errors.Is(err, identity.ErrNotFound) {
		return err
	}
	n := 0
	step := func() error {
		if w.afterStep != nil {
			w.afterStep(n)
		}
		return ctx.Err()
	}
	root, err := w.idx.GetRoot(ctx, mb)
	if err == nil {
		for _, f := range root.Folders {
			for {
				recs, _, err := w.idx.ListFolderDesc(ctx, mb, f.ID, 0, w.cfg.ScanBatch)
				if err != nil {
					return err
				}
				if len(recs) == 0 {
					break
				}
				for _, r := range recs {
					existed, err := w.deleteMessage(ctx, mb, r.MessageID)
					if err != nil {
						return err
					}
					if _, err := w.idx.Remove(ctx, mb, f.ID, r.UID, "remove-mailbox", w.Now()); err != nil {
						return err
					}
					if existed {
						n++
					}
					if err := step(); err != nil {
						return err
					}
				}
			}
		}
		for {
			entries, keys, more, err := w.idx.ListRetention(ctx, mb, time.Time{}, w.cfg.ScanBatch)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				break
			}
			for _, e := range entries {
				existed, err := w.deleteMessage(ctx, mb, e.MessageID)
				if err != nil {
					return err
				}
				if existed {
					n++
				}
				if err := step(); err != nil {
					return err
				}
			}
			if err := w.dropRetention(ctx, mb, entries, keys); err != nil {
				return err
			}
			if !more {
				break
			}
		}
		if err := w.idx.DeleteAll(ctx, mb); err != nil {
			return err
		}
	} else if !errors.Is(err, index.ErrNotFound) {
		return err
	}
	if _, err := w.msgs.DeleteUploads(ctx, mb); err != nil {
		return err
	}
	// Anything left in the messages pool for this mailbox (orphans).
	var leftover []message.Info
	if err := w.msgs.List(ctx, func(i message.Info) error {
		if i.Mailbox == mb {
			leftover = append(leftover, i)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, i := range leftover {
		if _, err := w.deleteMessage(storage.WithID(ctx, i.StorageID), mb, i.Message); err != nil {
			return err
		}
		n++
		if err := step(); err != nil {
			return err
		}
	}
	// The mailbox's own audit objects (one per day on its index pool) would
	// otherwise outlive it: the audit purge only walks live mailboxes.
	if err := w.audit.DeleteAll(ctx, mb); err != nil {
		return err
	}
	// Platform-level event (admin log), not the mailbox log that is gone.
	w.aud.Log(ctx, auditor.Event{Action: "remove-mailbox", Target: mb.String(), Detail: fmt.Sprintf("messages=%d", n)})
	return nil
}

// attachmentGC deletes attachment objects nobody references any more, on
// every connected storage.
func (w *Worker) attachmentGC(ctx context.Context) error {
	return w.atts.Candidates(ctx, func(cid int64, hash string) (bool, error) { return w.collectAttachment(ctx, cid, hash) })
}

func (w *Worker) collectAttachment(ctx context.Context, cid int64, hash string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	info, err := w.atts.GCInfo(ctx, cid, hash)
	if errors.Is(err, attach.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if w.Now().Sub(info.Touched) < w.cfg.AttachmentGCGrace {
		return false, nil
	}
	deleted, err := w.atts.DeleteIfUnreferenced(ctx, cid, hash, w.cfg.AttachmentGCGrace)
	return err == nil && (deleted || info.Refs > 0), err
}

// orphanSweep removes message objects that no index entry and no retention
// entry refers to (and lone .raw objects), once they are older than
// OrphanAge. A mailbox that had an orphan gets its quota recalculated: the
// orphan is the trace of a torn write that may have left the counters off.
func (w *Worker) orphanSweep(ctx context.Context) error {
	cutoff := w.Now().Add(-w.cfg.OrphanAge)
	inRetention := func(cid int64, mb, msg uuid.UUID) (bool, error) {
		return w.idx.IsRetained(storage.WithID(ctx, cid), mb, msg)
	}
	protected := func(i message.Info) (bool, error) {
		cctx := storage.WithID(ctx, i.StorageID)
		m, err := w.ids.GetMailbox(ctx, i.Mailbox)
		if err == nil && m.Value.Hold != nil {
			return true, nil
		}
		if err != nil && !errors.Is(err, identity.ErrNotFound) {
			return false, err
		}
		live, err := w.idx.IsLive(cctx, i.Mailbox, i.Message, i.Location)
		if err != nil || live {
			return live, err
		}
		return inRetention(i.StorageID, i.Mailbox, i.Message)
	}
	touched := map[uuid.UUID]bool{}
	removed := 0
	err := w.msgs.List(ctx, func(i message.Info) error {
		if i.Created.After(cutoff) {
			return nil
		}
		keep, err := protected(i)
		if err != nil {
			return err
		}
		if keep {
			return nil
		}
		// Revalidate immediately before deletion without retaining a pool-sized
		// candidate list. A transient lookup failure aborts the sweep.
		keep, err = protected(i)
		if err != nil {
			return err
		}
		if keep {
			return nil
		}
		if _, err := w.deleteMessage(storage.WithID(ctx, i.StorageID), i.Mailbox, i.Message); err != nil {
			return err
		}
		touched[i.Mailbox] = true
		removed++
		return nil
	})
	if err != nil {
		return err
	}
	for mb := range touched {
		if _, err := w.ids.GetMailbox(ctx, mb); err == nil {
			_, _ = w.queue.Enqueue(ctx, TaskRecalcQuota, mailboxPayload{Mailbox: mb}, time.Time{})
		}
	}
	w.log.Info("orphan sweep", slog.Int("removed", removed), slog.Int("mailboxes_recalculated", len(touched)))
	// The infrequent sweep repairs candidates lost across process crashes.
	return w.atts.List(ctx, func(cid int64, hash string) error { _, err := w.collectAttachment(ctx, cid, hash); return err })
}

// purgeAudit drops audit objects older than the retention: the admin log
// and every live mailbox's daily objects (removed mailboxes lose theirs
// with the mailbox).
func (w *Worker) purgeAudit(ctx context.Context) error {
	if maintenance, ok := w.ids.Backend().(interface{ PurgeExpiredSessions(context.Context) error }); ok {
		if err := maintenance.PurgeExpiredSessions(ctx); err != nil {
			return err
		}
	}
	cutoff := w.Now().Add(-w.cfg.AuditRetention)
	days := 60
	n, err := w.audit.PurgeBefore(ctx, uuid.Nil, cutoff, days)
	if err != nil {
		return err
	}
	err = w.ids.ListMailboxes(ctx, func(id uuid.UUID) error {
		// Mailboxes on hold keep everything.
		v, err := w.ids.GetMailbox(ctx, id)
		if errors.Is(err, identity.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if v.Value.Hold != nil {
			return nil
		}
		m, err := w.audit.PurgeBefore(ctx, id, cutoff, days)
		n += m
		return err
	})
	if err != nil {
		return err
	}
	w.log.Info("audit purge", slog.Int("objects", n))
	return nil
}

// ---- scheduler ----

// schedule, when this worker holds the scheduler lease, enqueues periodic
// tasks whose interval has elapsed. Last-run times live in the queue
// store so a new leader continues where the old one stopped.
func (w *Worker) schedule(ctx context.Context) {
	if lead, err := w.queue.Lead(ctx, w.ID, 2*w.cfg.PollInterval+time.Minute); err != nil || !lead {
		return // somebody else leads
	}
	due := func(name string, every time.Duration) bool {
		ok, err := w.queue.Due(ctx, name, every)
		return err == nil && ok
	}
	if scheduler, ok := w.queue.(queue.PurgeScheduler); ok {
		if err := scheduler.ReconcilePurgeSchedule(ctx, w.cfg.ScanBatch, w.cfg.OrphanSweepInterval); err != nil {
			w.log.Warn("purge discovery failed", slog.String("error", err.Error()))
		}
		if _, err := scheduler.EnqueueDuePurges(ctx, w.cfg.ScanBatch); err != nil {
			w.log.Warn("due purge scheduling failed", slog.String("error", err.Error()))
		}
	} else if due("purge", w.cfg.PurgeInterval) {
		n := 0
		_ = w.ids.ListMailboxes(ctx, func(id uuid.UUID) error {
			if err := w.enqueuePeriodic(ctx, TaskPurgeTrash, id.String(), mailboxPayload{Mailbox: id}); err == nil {
				n++
			}
			return nil
		})
		w.log.Info("scheduled trash purges", slog.Int("mailboxes", n))
	}
	for _, typ := range []string{TaskAttachmentGC, TaskPurgeAudit} {
		if due(typ, w.cfg.GCInterval) {
			_ = w.enqueuePeriodic(ctx, typ, "", struct{}{})
			w.log.Info("scheduled", slog.String("task", typ))
		}
	}
	if due(TaskOrphanSweep, w.cfg.OrphanSweepInterval) {
		_ = w.enqueuePeriodic(ctx, TaskOrphanSweep, "", struct{}{})
		w.log.Info("scheduled", slog.String("task", TaskOrphanSweep))
	}
	if due(TaskUploadSweep, w.cfg.PurgeInterval) {
		_ = w.enqueuePeriodic(ctx, TaskUploadSweep, "", struct{}{})
	}
}

// Production queues atomically coalesce periodic ticks across scheduler leaders.
func (w *Worker) enqueuePeriodic(ctx context.Context, typ, scope string, payload any) error {
	if q, ok := w.queue.(interface {
		EnqueuePeriodic(context.Context, string, string, any) error
	}); ok {
		return q.EnqueuePeriodic(ctx, typ, scope, payload)
	}
	_, err := w.queue.Enqueue(ctx, typ, payload, time.Time{})
	return err
}

func (w *Worker) dropRetention(ctx context.Context, mb uuid.UUID, entries []index.RetentionEntry, keys []string) error {
	var refs []index.AttachmentRef
	for _, entry := range entries {
		refs = append(refs, entry.Attachments...)
	}
	if err := w.atts.MarkGC(ctx, mb, refs); err != nil {
		return err
	}
	if err := w.idx.DropRetention(ctx, mb, keys...); err != nil {
		return err
	}
	return w.atts.MarkGC(ctx, mb, refs)
}
