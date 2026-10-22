package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/mail"
	"kittiwake/rados"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	exportsvc "kittiwake/service/export"
	"kittiwake/service/mailbox"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

type fixture struct {
	w    *Worker
	st   Stores
	del  *delivery.Service
	mbx  *mailbox.Service
	adm  *admin.Service
	mb   identity.Mailbox
	user context.Context
	adm_ context.Context
	now  time.Time
	c    *rados.MemoryClient
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	f := &fixture{now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC), c: c}
	now := func() time.Time { return f.now }
	c.Now = now
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f.st = Stores{Identity: identity.New(meta), Index: index.New(idxp), Messages: message.New(msgp), Attachments: attach.New(attp), Audit: audit.New(idxp), Queue: queue.New(idxp)}
	f.st.Index.Now, f.st.Messages.Now, f.st.Attachments.Now, f.st.Audit.Now, f.st.Identity.Now = now, now, now, now, now
	f.st.Queue.SetNow(now)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(f.st.Audit, log, nil)
	aud.Now = now
	f.del = delivery.New(f.st.Identity, f.st.Index, f.st.Messages, f.st.Attachments, f.st.Queue, aud, log, delivery.Config{InlineMax: 16})
	f.del.Now = now
	f.mbx = mailbox.New(f.st.Identity, f.st.Index, f.st.Messages, f.st.Attachments, f.st.Queue, aud, log, mailbox.Config{TrashRetention: 24 * time.Hour})
	f.mbx.Now = now
	f.adm = admin.New(f.st.Identity, f.st.Index, f.st.Queue, aud)
	f.adm.Now = now
	f.w = New(Config{PostHoldGrace: time.Hour, AttachmentGCGrace: time.Hour, OrphanAge: time.Hour, AuditRetention: 48 * time.Hour, PollInterval: time.Millisecond}, f.st, aud, log)
	f.w.Now = now
	ctx := context.Background()
	_, _ = f.st.Identity.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.st.Identity.CreateMailbox(ctx, identity.Mailbox{})
	acct, _ := f.st.Identity.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "write"}, Enabled: true})
	f.user = actor.With(ctx, actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: f.mb.ID, Roles: acct.Roles, Protocol: "jmap"})
	f.adm_ = actor.With(ctx, actor.Actor{AccountID: uuid.New(), Username: "root@example.org", Roles: []string{"admin"}, Protocol: "admin"})
	return f
}

const withAtt = "From: a@example.com\r\nTo: wido@example.org\r\nSubject: att\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nhi\r\n--B\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=\r\n--B--\r\n"

func (f *fixture) deliver(t *testing.T) (delivery.Delivered, string) {
	t.Helper()
	out, err := f.del.Deliver(context.Background(), delivery.Request{Recipient: "wido@example.org", Raw: []byte(withAtt)})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := f.st.Messages.GetDoc(context.Background(), f.mb.ID, out[0].MessageID)
	var doc mail.Document
	_ = json.Unmarshal(data, &doc)
	refs := mail.AttachmentHashes(&doc)
	if len(refs) != 1 {
		t.Fatalf("expected one attachment ref, got %v", refs)
	}
	return out[0], refs[0]
}

func (f *fixture) drain(t *testing.T) {
	t.Helper()
	for i := 0; i < 20 && f.w.RunOnce(context.Background()); i++ {
	}
}

func TestPurgeTrashAndAttachmentGC(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	d1, hash := f.deliver(t)
	d2, _ := f.deliver(t) // same attachment, refs=2
	if err := f.mbx.Destroy(f.user, f.mb.ID, d1.MessageID); err != nil {
		t.Fatal(err)
	}
	// Not due yet.
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d1.MessageID); err != nil {
		t.Fatal("purged before retention expired")
	}
	// Due.
	f.now = f.now.Add(25 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d1.MessageID); err == nil {
		t.Fatal("not purged")
	}
	info, _ := f.st.Attachments.Stat(ctx, f.mb.ID, hash)
	if info.Refs != 1 {
		t.Fatalf("refs after purge: %d", info.Refs)
	}
	ents, _, _, _ := f.st.Index.ListRetention(ctx, f.mb.ID, time.Time{}, 10)
	if len(ents) != 0 {
		t.Fatal("retention entry not dropped")
	}
	// GC keeps a referenced attachment, deletes an unreferenced one after grace.
	_ = f.mbx.Destroy(f.user, f.mb.ID, d2.MessageID)
	f.now = f.now.Add(25 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	_, _ = f.st.Queue.Enqueue(ctx, TaskAttachmentGC, struct{}{}, time.Time{})
	f.drain(t)
	if _, err := f.st.Attachments.Get(ctx, f.mb.ID, hash); err != nil {
		t.Fatal("deleted inside grace")
	}
	f.now = f.now.Add(2 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskAttachmentGC, struct{}{}, time.Time{})
	f.drain(t)
	if _, err := f.st.Attachments.Get(ctx, f.mb.ID, hash); err == nil {
		t.Fatal("unreferenced attachment not collected")
	}
	st, _ := f.st.Queue.Stats(ctx, TaskPurgeTrash)
	if st.Pending+st.Leased+st.Dead != 0 {
		t.Fatalf("queue not clean: %+v", st)
	}
}

// An admin purge (all=true) empties the retention set before the purge
// dates and leaves live messages alone.
func TestAdminPurgeIgnoresRetentionDates(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	d1, _ := f.deliver(t)
	d2, _ := f.deliver(t)
	if err := f.mbx.Destroy(f.user, f.mb.ID, d1.MessageID); err != nil {
		t.Fatal(err)
	}
	res, err := f.adm.PurgeMailbox(f.adm_, f.mb.ID)
	if err != nil || res.Retained != 1 || res.TaskID == "" {
		t.Fatalf("purge request: %+v %v", res, err)
	}
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d1.MessageID); err == nil {
		t.Fatal("deleted message survived the admin purge")
	}
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d2.MessageID); err != nil {
		t.Fatal("live message purged")
	}
	if ents, _, _, _ := f.st.Index.ListRetention(ctx, f.mb.ID, time.Time{}, 10); len(ents) != 0 {
		t.Fatal("retention entry not dropped")
	}
	// Refused on hold.
	if _, err := f.adm.Hold(f.adm_, f.mb.ID, admin.HoldRequest{Reason: "case"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.adm.PurgeMailbox(f.adm_, f.mb.ID); !errors.Is(err, identity.ErrConflict) {
		t.Fatalf("purge on hold: %v", err)
	}
}

func TestHoldBlocksPurge(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	d, _ := f.deliver(t)
	_ = f.mbx.Destroy(f.user, f.mb.ID, d.MessageID)
	if _, err := f.adm.Hold(f.adm_, f.mb.ID, admin.HoldRequest{Reason: "case"}); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(48 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d.MessageID); err != nil {
		t.Fatal("purged while on hold")
	}
	// Released: grace period, then purge.
	if _, err := f.adm.Release(f.adm_, f.mb.ID, ""); err != nil {
		t.Fatal(err)
	}
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d.MessageID); err != nil {
		t.Fatal("purged inside post-hold grace")
	}
	f.now = f.now.Add(2 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeTrash, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d.MessageID); err == nil {
		t.Fatal("not purged after grace")
	}
}

func TestRemoveMailbox(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	d1, hash := f.deliver(t)
	d2, _ := f.deliver(t)
	_ = f.mbx.Destroy(f.user, f.mb.ID, d2.MessageID)
	// Refused while the mailbox record exists.
	_, _ = f.st.Queue.Enqueue(ctx, TaskRemoveMailbox, mailboxPayload{Mailbox: f.mb.ID}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d1.MessageID); err != nil {
		t.Fatal("removed while mailbox exists")
	}
	st, _ := f.st.Queue.Stats(ctx, TaskRemoveMailbox)
	if st.Pending != 1 {
		t.Fatalf("expected retry pending: %+v", st)
	}
	// Delete the mailbox via admin: accounts first.
	acct, _ := f.st.Identity.GetAccountByUsername(ctx, "wido@example.org")
	_ = f.st.Identity.DeleteAccount(ctx, acct.Value.ID)
	if err := f.adm.DeleteMailbox(f.adm_, f.mb.ID); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(48 * time.Hour) // past the retry backoff
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, d1.MessageID); err == nil {
		t.Fatal("message objects remain")
	}
	if ok, _ := f.st.Index.Exists(ctx, f.mb.ID); ok {
		t.Fatal("index remains")
	}
	info, _ := f.st.Attachments.Stat(ctx, f.mb.ID, hash)
	if info.Refs != 0 {
		t.Fatalf("attachment refs remain: %d", info.Refs)
	}
	var left int
	_ = f.st.Messages.List(ctx, func(message.Info) error { left++; return nil })
	if left != 0 {
		t.Fatalf("leftover message objects: %d", left)
	}
}

// A worker that shuts down in the middle of a removal hands the task back
// at once (no retry backoff, no attempt counted as a failure) and the next
// worker finishes it: nothing is left in the index or the message pool.
func TestRemoveMailboxResumesAfterRestart(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	var msgs []delivery.Delivered
	for i := 0; i < 5; i++ {
		d, _ := f.deliver(t)
		msgs = append(msgs, d)
	}
	acct, _ := f.st.Identity.GetAccountByUsername(ctx, "wido@example.org")
	_ = f.st.Identity.DeleteAccount(ctx, acct.Value.ID)
	if err := f.adm.DeleteMailbox(f.adm_, f.mb.ID); err != nil {
		t.Fatal(err)
	}
	// First worker: SIGTERM after the second message.
	rctx, stop := context.WithCancel(ctx)
	f.w.afterStep = func(n int) {
		if n == 2 {
			stop()
		}
	}
	f.w.RunOnce(rctx)
	f.w.afterStep = nil
	st, _ := f.st.Queue.Stats(ctx, TaskRemoveMailbox)
	if st.Pending != 1 || st.Leased != 0 || st.Dead != 0 {
		t.Fatalf("task not handed back after shutdown: %+v", st)
	}
	var left int
	_ = f.st.Messages.List(ctx, func(message.Info) error { left++; return nil })
	if left != 3 {
		t.Fatalf("expected 3 messages left after the interrupted run, got %d", left)
	}
	// Next worker, same clock: the task is due at once, not after a backoff.
	tasks, _ := f.st.Queue.Claim(ctx, TaskRemoveMailbox, "peek", 1, time.Minute)
	if len(tasks) != 1 || tasks[0].Attempts != 2 || tasks[0].LastError != "" {
		t.Fatalf("expected the released task due now with attempts=2, got %+v", tasks)
	}
	_ = f.st.Queue.Release(ctx, tasks[0])
	f.drain(t)
	left = 0
	_ = f.st.Messages.List(ctx, func(message.Info) error { left++; return nil })
	if left != 0 {
		t.Fatalf("leftover message objects after resume: %d", left)
	}
	if ok, _ := f.st.Index.Exists(ctx, f.mb.ID); ok {
		t.Fatal("index remains")
	}
	st, _ = f.st.Queue.Stats(ctx, TaskRemoveMailbox)
	if st.Pending != 0 || st.Leased != 0 || st.Dead != 0 {
		t.Fatalf("task remains: %+v", st)
	}
}

// A task whose lease was taken over by another worker is not completed,
// failed or released by the worker that lost it.
func TestLostLeaseStopsTask(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		f.deliver(t)
	}
	acct, _ := f.st.Identity.GetAccountByUsername(ctx, "wido@example.org")
	_ = f.st.Identity.DeleteAccount(ctx, acct.Value.ID)
	_ = f.adm.DeleteMailbox(f.adm_, f.mb.ID)
	f.w.cfg.LeaseDuration = 4 * time.Millisecond // renewal every millisecond
	f.w.afterStep = func(n int) {
		if n == 1 {
			// Another worker reclaims the task: the lease expires on the clock.
			f.now = f.now.Add(time.Hour)
			other, _ := f.st.Queue.Claim(ctx, TaskRemoveMailbox, "other", 1, time.Hour)
			if len(other) != 1 {
				t.Error("other worker could not reclaim the expired lease")
			}
			time.Sleep(20 * time.Millisecond) // let renewal notice
		}
	}
	f.w.RunOnce(ctx)
	st, _ := f.st.Queue.Stats(ctx, TaskRemoveMailbox)
	if st.Leased != 1 || st.Pending != 0 || st.Dead != 0 {
		t.Fatalf("the other worker's lease must stay untouched: %+v", st)
	}
}

func TestOrphanSweepAndAuditPurge(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	live, _ := f.deliver(t)
	// An orphan: objects without index entry.
	orphan := uuid.New()
	_ = f.st.Messages.Put(ctx, f.mb.ID, orphan, []byte("{}"), "f1/999")
	_, _ = f.st.Queue.Enqueue(ctx, TaskOrphanSweep, struct{}{}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, orphan); err != nil {
		t.Fatal("young orphan removed")
	}
	f.now = f.now.Add(2 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskOrphanSweep, struct{}{}, time.Time{})
	f.drain(t)
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, orphan); err == nil {
		t.Fatal("old orphan not removed")
	}
	if _, err := f.st.Messages.GetDoc(ctx, f.mb.ID, live.MessageID); err != nil {
		t.Fatal("live message removed")
	}
	// Audit purge: old objects go, recent stay.
	recs, _, _ := f.st.Audit.Read(ctx, audit.Query{Mailbox: f.mb.ID, From: f.now.Add(-3 * time.Hour), To: f.now})
	if len(recs) == 0 {
		t.Fatal("expected audit records")
	}
	f.now = f.now.Add(72 * time.Hour)
	_, _ = f.st.Queue.Enqueue(ctx, TaskPurgeAudit, struct{}{}, time.Time{})
	f.drain(t)
	recs, _, _ = f.st.Audit.Read(ctx, audit.Query{Mailbox: f.mb.ID, From: f.now.Add(-75 * time.Hour), To: f.now})
	if len(recs) != 0 {
		t.Fatalf("audit not purged: %d", len(recs))
	}
}

func TestSchedulerEnqueuesPeriodicTasks(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.w.schedule(ctx)
	st, _ := f.st.Queue.Stats(ctx, TaskPurgeTrash)
	gc, _ := f.st.Queue.Stats(ctx, TaskAttachmentGC)
	if st.Pending != 1 || gc.Pending != 1 {
		t.Fatalf("not scheduled: purge=%+v gc=%+v", st, gc)
	}
	// Not again within the interval.
	f.w.schedule(ctx)
	st, _ = f.st.Queue.Stats(ctx, TaskPurgeTrash)
	if st.Pending != 1 {
		t.Fatal("scheduled twice")
	}
	// After the interval the leader schedules again and renews its lock.
	f.now = f.now.Add(90 * time.Minute)
	f.w.schedule(ctx)
	st, _ = f.st.Queue.Stats(ctx, TaskPurgeTrash)
	if st.Pending != 2 {
		t.Fatalf("leader did not reschedule: %+v", st)
	}
	// A second worker cannot lead while the lock is fresh...
	w2 := New(Config{PollInterval: time.Millisecond}, f.st, f.w.aud, f.w.log)
	w2.Now = f.w.Now
	w2.schedule(ctx)
	st, _ = f.st.Queue.Stats(ctx, TaskPurgeTrash)
	if st.Pending != 2 {
		t.Fatal("second worker scheduled while lock held")
	}
	// ...but takes over once the leader's lock expires (leader died).
	f.now = f.now.Add(2 * time.Hour)
	w2.schedule(ctx)
	st, _ = f.st.Queue.Stats(ctx, TaskPurgeTrash)
	if st.Pending != 3 {
		t.Fatalf("failover did not schedule: %+v", st)
	}
	// Run loop stops on cancel.
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := f.w.Run(cctx); err != nil {
		t.Fatal(err)
	}
}

func TestExportMailboxTask(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.deliver(t)
	dir := t.TempDir()
	f.w.cfg.ExportDir = dir
	// Without an export service the task fails and retries.
	_, _ = f.st.Queue.Enqueue(ctx, TaskExportMailbox, ExportTask{Mailbox: f.mb.ID, Gzip: true}, time.Time{})
	f.drain(t)
	if st, _ := f.st.Queue.Stats(ctx, TaskExportMailbox); st.Pending != 1 {
		t.Fatalf("expected retry: %+v", st)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.w.Export = exportsvc.New(f.st.Identity, f.st.Index, f.st.Messages, f.st.Attachments, f.st.Audit, f.del, f.w.aud, log)
	f.now = f.now.Add(2 * time.Hour)
	f.drain(t)
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 2 || !strings.HasSuffix(names[0], ".tar.gz") || !strings.HasSuffix(names[1], ".tar.gz.json") {
		t.Fatalf("export files: %v", names)
	}
	b, _ := os.ReadFile(filepath.Join(dir, names[1]))
	var res ExportResult
	if err := json.Unmarshal(b, &res); err != nil || res.Messages != 1 || res.Bytes == 0 || res.File != names[0] {
		t.Fatalf("result: %+v %v", res, err)
	}
}
