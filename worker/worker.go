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
	"kittiwake/store/cluster"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

// Task types handled here (plus those defined by services).
const (
	TaskPurgeTrash    = admin.TaskPurgeTrash
	TaskRemoveMailbox = admin.TaskRemoveMailbox
	TaskRecalcQuota   = admin.TaskRecalcQuota
	TaskSearchIndex   = delivery.TaskSearchIndex
	TaskAttachmentGC  = "attachment-gc"
	TaskPurgeAudit    = "purge-audit"
	TaskOrphanSweep   = "orphan-sweep"
	TaskExportMailbox = "export-mailbox"
)

// AllTypes lists every queue the worker drains.
var AllTypes = []string{TaskPurgeTrash, TaskRemoveMailbox, TaskRecalcQuota, TaskSearchIndex, TaskAttachmentGC, TaskPurgeAudit, TaskOrphanSweep, TaskExportMailbox}

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
	// LeaseDuration for claimed tasks.
	LeaseDuration time.Duration
	// Schedule intervals for periodic tasks.
	PurgeInterval time.Duration // per-mailbox trash purge
	GCInterval    time.Duration // attachment GC, audit purge, orphan sweep
	ScanBatch     int
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
		cfg.LeaseDuration = 10 * time.Minute
	}
	if cfg.PurgeInterval <= 0 {
		cfg.PurgeInterval = time.Hour
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = 24 * time.Hour
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
		tasks, err := w.queue.Claim(ctx, typ, w.ID, 10, w.cfg.LeaseDuration)
		if err != nil {
			if !errors.Is(err, queue.ErrLocked) {
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

func (w *Worker) execute(ctx context.Context, task queue.Task) {
	start := w.Now()
	err := w.handle(ctx, task)
	if err != nil {
		dead, ferr := w.queue.Fail(ctx, task, err)
		res := "retry"
		if dead {
			res = "dead"
		}
		metrics.Tasks.WithLabelValues(task.Type, res).Inc()
		w.log.Warn("task failed", slog.String("type", task.Type), slog.String("id", task.ID), slog.Int("attempt", task.Attempts),
			slog.Bool("dead", dead), slog.String("error", err.Error()), slog.Any("requeue_error", ferr))
		return
	}
	if err := w.queue.Complete(ctx, task); err != nil {
		w.log.Warn("complete failed", slog.String("type", task.Type), slog.String("error", err.Error()))
	}
	metrics.Tasks.WithLabelValues(task.Type, "ok").Inc()
	w.log.Debug("task done", slog.String("type", task.Type), slog.String("id", task.ID), slog.Duration("took", w.Now().Sub(start)))
}

type mailboxPayload struct {
	Mailbox uuid.UUID `json:"mailbox"`
	// ClusterID pins the cluster for a mailbox whose record is gone
	// (remove-mailbox); zero means "look it up".
	ClusterID int64 `json:"cluster_id,omitempty"`
}

func (w *Worker) handle(ctx context.Context, task queue.Task) error {
	ctx = actor.With(ctx, actor.System(audit.ProtoCLI))
	var mp mailboxPayload
	switch task.Type {
	case TaskPurgeTrash, TaskRemoveMailbox, TaskRecalcQuota:
		if err := json.Unmarshal(task.Payload, &mp); err != nil || mp.Mailbox == uuid.Nil {
			return fmt.Errorf("bad payload: %v", err)
		}
	}
	switch task.Type {
	case TaskPurgeTrash:
		return w.purgeTrash(ctx, mp.Mailbox)
	case TaskRemoveMailbox:
		if mp.ClusterID != 0 {
			ctx = cluster.WithID(ctx, mp.ClusterID)
		}
		return w.removeMailbox(ctx, mp.Mailbox)
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
func (w *Worker) purgeTrash(ctx context.Context, mb uuid.UUID) error {
	if v, err := w.ids.GetMailbox(ctx, mb); err == nil {
		if v.Value.Hold != nil {
			w.log.Debug("purge skipped: on hold", slog.String("mailbox", mb.String()))
			return nil
		}
		if v.Value.Released != nil && w.Now().Sub(*v.Value.Released) < w.cfg.PostHoldGrace {
			w.log.Debug("purge skipped: post-hold grace", slog.String("mailbox", mb.String()))
			return nil
		}
	} else if !errors.Is(err, identity.ErrNotFound) {
		return err
	}
	purged := 0
	for {
		entries, keys, more, err := w.idx.ListRetention(ctx, mb, w.Now(), w.cfg.ScanBatch)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for i, e := range entries {
			if err := w.deleteMessage(ctx, mb, e.MessageID); err != nil {
				return err
			}
			if err := w.idx.DropRetention(ctx, mb, keys[i]); err != nil {
				return err
			}
			purged++
		}
		if !more {
			break
		}
	}
	if purged > 0 {
		w.aud.Log(ctx, auditor.Event{Mailbox: mb, Action: "purge", Detail: fmt.Sprintf("messages=%d", purged)})
	}
	return nil
}

// deleteMessage removes attachment references and the message objects.
func (w *Worker) deleteMessage(ctx context.Context, mb, msg uuid.UUID) error {
	data, err := w.msgs.GetDoc(ctx, mb, msg)
	if err == nil {
		var doc mail.Document
		if json.Unmarshal(data, &doc) == nil {
			for _, h := range mail.AttachmentHashes(&doc) {
				if err := w.atts.RemoveRef(ctx, h, mb, msg); err != nil {
					return err
				}
			}
			w.log.Info("purged message", slog.String("message_id", doc.MessageID), slog.String("mailbox", mb.String()), slog.String("msg", msg.String()))
		}
	} else if !errors.Is(err, message.ErrNotFound) {
		return err
	}
	return w.msgs.Delete(ctx, mb, msg)
}

// removeMailbox purges every object of a mailbox whose record is gone.
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
	root, err := w.idx.GetRoot(ctx, mb)
	if err == nil {
		for _, f := range root.Folders {
			for {
				recs, _, err := w.idx.ListFolder(ctx, mb, f.ID, 0, w.cfg.ScanBatch)
				if err != nil || len(recs) == 0 {
					break
				}
				for _, r := range recs {
					if err := w.deleteMessage(ctx, mb, r.MessageID); err != nil {
						return err
					}
					if _, err := w.idx.Remove(ctx, mb, f.ID, r.UID, "remove-mailbox", w.Now()); err != nil {
						return err
					}
					n++
				}
			}
		}
		entries, keys, _, err := w.idx.ListRetention(ctx, mb, time.Time{}, 0)
		if err != nil {
			return err
		}
		for i, e := range entries {
			if err := w.deleteMessage(ctx, mb, e.MessageID); err != nil {
				return err
			}
			if err := w.idx.DropRetention(ctx, mb, keys[i]); err != nil {
				return err
			}
			n++
		}
		if err := w.idx.DeleteAll(ctx, mb); err != nil {
			return err
		}
	} else if !errors.Is(err, index.ErrNotFound) {
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
		if err := w.deleteMessage(cluster.WithID(ctx, i.ClusterID), mb, i.Message); err != nil {
			return err
		}
		n++
	}
	w.aud.Log(ctx, auditor.Event{Mailbox: mb, Action: "remove-mailbox", Detail: fmt.Sprintf("messages=%d", n)})
	return nil
}

// attachmentGC deletes attachment objects nobody references any more, on
// every connected cluster.
func (w *Worker) attachmentGC(ctx context.Context) error {
	type ref struct {
		cluster int64
		hash    string
	}
	var hashes []ref
	if err := w.atts.List(ctx, func(cid int64, h string) error { hashes = append(hashes, ref{cid, h}); return nil }); err != nil {
		return err
	}
	deleted := 0
	for _, h := range hashes {
		ok, err := w.atts.DeleteIfUnreferenced(ctx, h.cluster, h.hash, w.cfg.AttachmentGCGrace)
		if err != nil {
			return err
		}
		if ok {
			deleted++
		}
	}
	w.log.Info("attachment gc", slog.Int("scanned", len(hashes)), slog.Int("deleted", deleted))
	return nil
}

// purgeAudit drops audit objects past the retention period.
func (w *Worker) purgeAudit(ctx context.Context) error {
	cutoff := w.Now().Add(-w.cfg.AuditRetention)
	days := 60
	n, err := w.audit.PurgeBefore(ctx, uuid.Nil, cutoff, days)
	if err != nil {
		return err
	}
	err = w.ids.ListMailboxes(ctx, func(id uuid.UUID) error {
		// Mailboxes on hold keep everything.
		if v, err := w.ids.GetMailbox(ctx, id); err == nil && v.Value.Hold != nil {
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

// orphanSweep removes message objects that never reached an index and are
// not waiting in a retention set.
func (w *Worker) orphanSweep(ctx context.Context) error {
	cutoff := w.Now().Add(-w.cfg.OrphanAge)
	retention := map[uuid.UUID]map[uuid.UUID]bool{}
	inRetention := func(cid int64, mb, msg uuid.UUID) bool {
		set, ok := retention[mb]
		if !ok {
			set = map[uuid.UUID]bool{}
			entries, _, _, err := w.idx.ListRetention(cluster.WithID(ctx, cid), mb, time.Time{}, 0)
			if err == nil {
				for _, e := range entries {
					set[e.MessageID] = true
				}
			}
			retention[mb] = set
		}
		return set[msg]
	}
	var orphans []message.Info
	err := w.msgs.List(ctx, func(i message.Info) error {
		if i.Created.After(cutoff) {
			return nil
		}
		// The object's own cluster answers; the mailbox record may be gone.
		cctx := cluster.WithID(ctx, i.ClusterID)
		if loc, err := index.ParseLocation(i.Location); err == nil {
			if rec, err := w.idx.GetRecord(cctx, i.Mailbox, loc.FolderID, loc.UID); err == nil && rec.MessageID == i.Message {
				return nil
			} else if err != nil && !errors.Is(err, index.ErrNotFound) {
				return err
			}
		}
		if inRetention(i.ClusterID, i.Mailbox, i.Message) {
			return nil
		}
		orphans = append(orphans, i)
		return nil
	})
	if err != nil {
		return err
	}
	for _, o := range orphans {
		if err := w.deleteMessage(cluster.WithID(ctx, o.ClusterID), o.Mailbox, o.Message); err != nil {
			return err
		}
	}
	w.log.Info("orphan sweep", slog.Int("removed", len(orphans)))
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
	if due("purge", w.cfg.PurgeInterval) {
		n := 0
		_ = w.ids.ListMailboxes(ctx, func(id uuid.UUID) error {
			if _, err := w.queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: id}, time.Time{}); err == nil {
				n++
			}
			return nil
		})
		w.log.Info("scheduled trash purges", slog.Int("mailboxes", n))
	}
	for _, typ := range []string{TaskAttachmentGC, TaskPurgeAudit, TaskOrphanSweep} {
		if due(typ, w.cfg.GCInterval) {
			_, _ = w.queue.Enqueue(ctx, typ, struct{}{}, time.Time{})
			w.log.Info("scheduled", slog.String("task", typ))
		}
	}
}
