package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup/crypto"
	"kittiwake/backup/target"
	"kittiwake/mail"
	"kittiwake/rados"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/service/mailbox"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

type fixture struct {
	svc   *Service
	cat   *Memory
	tg    *target.Memory
	del   *delivery.Service
	mbx   *mailbox.Service
	adm   *admin.Service
	ids   *identity.Store
	idx   *index.Store
	msgs  *message.Store
	atts  *attach.Store
	q     queue.Store
	mb    identity.Mailbox
	cli   context.Context
	user  context.Context
	now   time.Time
	clock func() time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{ids: identity.New(meta), idx: index.New(idxp), now: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)}
	f.clock = func() time.Time { return f.now }
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auditSt := audit.New(idxp)
	auditSt.Now = f.clock
	aud := auditor.New(auditSt, log, nil)
	aud.Now = f.clock
	f.msgs, f.atts = message.New(msgp), attach.New(attp)
	f.q = queue.New(idxp)
	f.del = delivery.New(f.ids, f.idx, f.msgs, f.atts, f.q, aud, log, delivery.Config{InlineMax: 16})
	f.mbx = mailbox.New(f.ids, f.idx, f.msgs, f.atts, nil, aud, log, mailbox.Config{})
	f.adm = admin.New(f.ids, f.idx, f.q, aud)
	f.cat = NewMemory()
	f.cat.Now = f.clock
	f.cat.Mailboxes = f.ids.ListMailboxes
	f.tg = target.NewMemory()
	ring, _ := crypto.NewRing([]crypto.Key{{ID: 1, Key: bytes.Repeat([]byte{7}, 32)}}, 1)
	f.svc = New(Config{SpoolDir: t.TempDir(), BlobThreshold: 64, PackSize: 1 << 20, Prefix: "kw/", DefaultRetention: 48 * time.Hour, MinInterval: time.Minute},
		Stores{Identity: f.ids, Index: f.idx, Messages: f.msgs, Attachments: f.atts, Audit: auditSt}, f.cat, f.tg, ring, f.adm, aud, log)
	f.svc.Now = f.clock
	f.svc.Hostname = "host.test"
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	acct, _ := f.ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "write"}, Enabled: true})
	f.cli = actor.With(ctx, actor.System("cli"))
	f.user = actor.With(ctx, actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: f.mb.ID, Roles: acct.Roles, Protocol: "jmap"})
	return f
}

const withAtt = "From: a@example.com\r\nTo: wido@example.org\r\nSubject: att\r\nMessage-ID: <x@y>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nhi\r\n--B\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=\r\n--B--\r\n"

func (f *fixture) seed(t *testing.T) (uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	out, err := f.del.Deliver(f.cli, delivery.Request{Recipient: "wido@example.org", Raw: []byte(withAtt)})
	if err != nil {
		t.Fatal(err)
	}
	m1 := out[0].MessageID
	if _, err := f.mbx.SetFlags(f.user, f.mb.ID, m1, mailbox.FlagOp{Add: []string{"\\Seen", "\\Flagged", "$label1"}}); err != nil {
		t.Fatal(err)
	}
	nf, err := f.mbx.CreateFolder(f.user, f.mb.ID, "Projects/2026")
	if err != nil {
		t.Fatal(err)
	}
	d2, err := f.del.Import(f.user, f.mb.ID, nf.ID, []byte("From: b@example.com\r\nSubject: second\r\nMessage-ID: <2@y>\r\n\r\nbody\r\n"), []string{"\\Draft"}, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	// A big attachment (above the blob threshold) stored as a blob.
	big := strings.Repeat("Z", 300)
	raw := "From: c@example.com\r\nSubject: big\r\nMessage-ID: <3@y>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nhello\r\n--B\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=big.pdf\r\n\r\n" + big + "\r\n--B--\r\n"
	d3, err := f.del.Import(f.user, f.mb.ID, nf.ID, []byte(raw), nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return m1, d2.MessageID, d3.MessageID
}

func (f *fixture) collect(t *testing.T, mb uuid.UUID) map[string][]index.Record {
	t.Helper()
	root, err := f.idx.GetRoot(f.cli, mb)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]index.Record{}
	for _, fo := range root.Folders {
		recs, _, err := f.idx.ListFolder(f.cli, mb, fo.ID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		out[fo.Name] = recs
	}
	return out
}

func (f *fixture) rendered(t *testing.T, mb, msg uuid.UUID) []byte {
	t.Helper()
	b, err := f.msgs.GetDoc(f.cli, mb, msg)
	if err != nil {
		t.Fatal(err)
	}
	var doc mail.Document
	_ = json.Unmarshal(b, &doc)
	out, err := mail.Render(&doc, func(h string) ([]byte, error) { return f.atts.Get(f.cli, mb, h) })
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	f := newFixture(t)
	m1, _, m3 := f.seed(t)
	if _, err := f.svc.Backup(f.user, f.mb.ID, RunOptions{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("user may not back up: %v", err)
	}
	run, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if run.Messages != 3 || run.NewMessages != 3 || !run.Full || run.Fuzzy || len(run.Packs) != 1 {
		t.Fatalf("run: %+v", run)
	}
	var blobs, packs, manifests int
	for _, k := range f.tg.Keys() {
		switch {
		case strings.HasPrefix(k, "kw/blob/"):
			blobs++
		case strings.HasSuffix(k, ".pack"):
			packs++
		case strings.HasSuffix(k, ".manifest"):
			manifests++
		}
	}
	if blobs != 1 || packs != 1 || manifests != 1 {
		t.Fatalf("objects: blobs=%d packs=%d manifests=%d keys=%v", blobs, packs, manifests, f.tg.Keys())
	}
	st, _ := f.cat.BackupState(f.cli, f.mb.ID)
	if st.LastRun != run.ID || st.Status != "ok" || st.LastModSeq != run.ModSeq || st.Due.IsZero() {
		t.Fatalf("state: %+v", st)
	}
	// Nothing changed: the next run is a skip (one root read).
	f.now = f.now.Add(time.Hour)
	if r2, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{}); err != nil || r2.ID != "" {
		t.Fatalf("skip: %+v %v", r2, err)
	}
	if st, _ := f.cat.BackupState(f.cli, f.mb.ID); st.Status != "skipped" || st.LastRun != run.ID {
		t.Fatalf("skipped state: %+v", st)
	}
	// A new message in INBOX: incremental run reuses the other folders.
	f.now = f.now.Add(time.Hour)
	if _, err := f.del.Deliver(f.cli, delivery.Request{Recipient: "wido@example.org", Raw: []byte("From: d@example.com\r\nSubject: fourth\r\n\r\nx\r\n")}); err != nil {
		t.Fatal(err)
	}
	// And a flag change plus a move touches two folders without new data.
	if _, err := f.mbx.SetFlags(f.user, f.mb.ID, m3, mailbox.FlagOp{Add: []string{"\\Seen"}}); err != nil {
		t.Fatal(err)
	}
	run2, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if run2.Messages != 4 || run2.NewMessages != 1 || run2.Full || len(run2.Packs) != 2 {
		t.Fatalf("run2: %+v", run2)
	}
	man, err := f.svc.Manifest(f.cli, f.mb.ID, run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if man.Previous != run.ID || len(man.Folders) == 0 || len(man.Audit) == 0 {
		t.Fatalf("manifest: %+v", man)
	}
	prev, _ := f.svc.Manifest(f.cli, f.mb.ID, run.ID)
	reused, rewritten := 0, 0
	for _, fr := range man.Folders {
		for _, pf := range prev.Folders {
			if pf.ID == fr.ID {
				if pf.Records == fr.Records {
					reused++
				} else {
					rewritten++
				}
			}
		}
	}
	if reused == 0 || rewritten != 2 {
		t.Fatalf("reused=%d rewritten=%d", reused, rewritten)
	}

	// Restore side by side.
	res, err := f.svc.Restore(f.cli, f.mb.ID, run2.ID, RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mailbox == uuid.Nil || res.Mailbox == f.mb.ID || res.Messages != 4 || len(res.Errors) != 0 {
		t.Fatalf("restore: %+v", res)
	}
	src, dst := f.collect(t, f.mb.ID), f.collect(t, res.Mailbox)
	for name, recs := range src {
		got := dst[name]
		if len(got) != len(recs) {
			t.Fatalf("folder %s: %d restored, %d in source", name, len(got), len(recs))
		}
		for i := range recs {
			a, b := recs[i], got[i]
			if a.Subject != b.Subject || a.Size != b.Size || strings.Join(a.Flags, ",") != strings.Join(b.Flags, ",") || !a.InternalDate.Equal(b.InternalDate) || a.HasAttachments != b.HasAttachments {
				t.Fatalf("folder %s record %d differs:\n%+v\n%+v", name, i, a, b)
			}
			if !bytes.Equal(f.rendered(t, f.mb.ID, a.MessageID), f.rendered(t, res.Mailbox, b.MessageID)) {
				t.Fatalf("folder %s message %d renders differently", name, i)
			}
		}
	}
	_ = m1
	// Single-message restore into the live mailbox lands in the original folder.
	one, err := f.svc.RestoreMessage(f.cli, f.mb.ID, run2.ID, m3, uuid.Nil)
	if err != nil || one.Messages != 1 {
		t.Fatalf("restore message: %+v %v", one, err)
	}
	if got := f.collect(t, f.mb.ID)["Projects/2026"]; len(got) != 3 {
		t.Fatalf("restored copy missing: %d", len(got))
	}
	if _, err := f.svc.RestoreMessage(f.cli, f.mb.ID, run2.ID, uuid.New(), uuid.Nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown message: %v", err)
	}
}

func TestExportFromBackup(t *testing.T) {
	f := newFixture(t)
	f.seed(t)
	run, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.svc.Export(f.cli, f.mb.ID, run.ID, "root")
	if err != nil {
		t.Fatal(err)
	}
	if res.Messages != 3 || res.Bytes == 0 || !strings.HasPrefix(res.Key, "kw/export/"+f.mb.ID.String()+"/") || len(res.Errors) != 0 {
		t.Fatalf("export: %+v", res)
	}
	rc, size, err := f.svc.OpenExport(f.cli, res.Key)
	if err != nil || size != res.Bytes {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	files := map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			b, _ := io.ReadAll(tr)
			files[h.Name] = b
		}
	}
	prefix := "kittiwake-" + f.mb.ID.String() + "/"
	var em ExportManifest
	if err := json.Unmarshal(files[prefix+"manifest.json"], &em); err != nil {
		t.Fatal(err)
	}
	if len(em.Messages) != 3 || em.Run != run.ID || em.AuditCount == 0 || len(em.Accounts) != 1 {
		t.Fatalf("export manifest: %+v", em)
	}
	cur := 0
	for name, b := range files {
		if strings.Contains(name, "/cur/") {
			cur++
			if !bytes.Contains(b, []byte("Subject:")) {
				t.Fatalf("%s: not a message", name)
			}
			if strings.Contains(name, ":2,FS") && !strings.Contains(string(b), "Subject: att") {
				t.Fatalf("flags on the wrong file: %s", name)
			}
		}
	}
	if cur != 3 || !strings.Contains(string(files[prefix+"maildir/.Projects.2026/dovecot-uidlist"]), "3 V") || len(files[prefix+"audit.jsonl"]) == 0 {
		t.Fatalf("maildir files: cur=%d keys=%v", cur, keysOf(files))
	}
	if _, ok := files[prefix+"maildir/dovecot-keywords"]; !ok {
		t.Fatal("keywords file for INBOX ($label1)")
	}
}

func keysOf(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestVerifyAndExpire(t *testing.T) {
	f := newFixture(t)
	f.seed(t)
	run, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := f.svc.Verify(f.cli, f.mb.ID, VerifyOptions{Full: true})
	if err != nil || !rep.OK || rep.Checked < 4 || rep.Runs != 1 || len(rep.Stray) != 0 {
		t.Fatalf("verify: %+v %v", rep, err)
	}
	// A stray object is reported, then removed after the grace period.
	stray := "kw/mb/" + f.mb.ID.String() + "/stray.pack"
	_ = f.tg.Put(f.cli, stray, strings.NewReader("x"), 1, target.PutOptions{})
	rep, _ = f.svc.Verify(f.cli, f.mb.ID, VerifyOptions{})
	if len(rep.Stray) != 1 || len(rep.Removed) != 0 || !rep.OK {
		t.Fatalf("stray: %+v", rep)
	}
	f.now = f.now.Add(72 * time.Hour)
	rep, _ = f.svc.Verify(f.cli, f.mb.ID, VerifyOptions{})
	if len(rep.Removed) != 1 {
		t.Fatalf("stray removal: %+v", rep)
	}
	// Corruption is detected and forces a full run.
	f.tg.Corrupt(run.Packs[0])
	rep, _ = f.svc.Verify(f.cli, f.mb.ID, VerifyOptions{Full: true})
	if rep.OK || len(rep.Corrupt) == 0 {
		t.Fatalf("corrupt: %+v", rep)
	}
	if st, _ := f.cat.BackupState(f.cli, f.mb.ID); !st.ForceFull || st.VerifyError == "" {
		t.Fatalf("state after corrupt verify: %+v", st)
	}
	// The forced full run rewrites everything from RADOS.
	run2, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil || !run2.Full || run2.NewMessages != 3 {
		t.Fatalf("forced full: %+v %v", run2, err)
	}
	rep, _ = f.svc.Verify(f.cli, f.mb.ID, VerifyOptions{Full: true})
	if !rep.OK {
		t.Fatalf("after full: %+v", rep)
	}
	// A missing pack is reported.
	_ = f.tg.Delete(f.cli, run.Packs[0])
	rep, _ = f.svc.Verify(f.cli, f.mb.ID, VerifyOptions{})
	if rep.OK || len(rep.Missing) != 1 {
		t.Fatalf("missing: %+v", rep)
	}
	// Expiry: the first run leaves retention; its remaining packs go, the
	// blob stays while run2 references it, the mailbox prefix stays.
	f.now = run.Finished.Add(49 * time.Hour)
	exp, err := f.svc.Expire(f.cli, 100)
	if err != nil || exp.Runs != 1 || exp.Mailboxes != 0 {
		t.Fatalf("expire: %+v %v", exp, err)
	}
	if _, err := f.cat.GetRun(f.cli, f.mb.ID, run.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("run row should be gone")
	}
	runs, _ := f.cat.ListRuns(f.cli, f.mb.ID, 0)
	if len(runs) != 1 || runs[0].ID != run2.ID {
		t.Fatalf("runs: %+v", runs)
	}
	f.now = run2.Finished.Add(49 * time.Hour)
	exp, err = f.svc.Expire(f.cli, 100)
	// The last run went: the mailbox prefix is emptied, blob references
	// released, and the blob (past the grace period) collected.
	if err != nil || exp.Runs != 1 || exp.Mailboxes != 1 || exp.Blobs != 1 {
		t.Fatalf("expire 2: %+v %v", exp, err)
	}
	if len(f.tg.Keys()) != 0 {
		t.Fatalf("objects left: %v", f.tg.Keys())
	}
}

func TestHoldKeepsRuns(t *testing.T) {
	f := newFixture(t)
	f.seed(t)
	run, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	adminCtx := actor.With(context.Background(), actor.Actor{Username: "root", Roles: []string{"admin"}, Protocol: "admin"})
	if _, err := f.adm.Hold(adminCtx, f.mb.ID, admin.HoldRequest{Reason: "case"}); err != nil {
		t.Fatal(err)
	}
	f.now = run.Finished.Add(100 * time.Hour)
	if exp, err := f.svc.Expire(f.cli, 100); err != nil || exp.Runs != 0 {
		t.Fatalf("held runs must not expire: %+v %v", exp, err)
	}
	if err := f.svc.Forget(f.cli, f.mb.ID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("forget on hold: %v", err)
	}
	man, _ := f.svc.Manifest(f.cli, f.mb.ID, run.ID)
	if man.Hold != nil {
		t.Fatal("hold set after the run")
	}
	run2, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	man, _ = f.svc.Manifest(f.cli, f.mb.ID, run2.ID)
	if man.Hold == nil || man.Hold.Reason != "case" {
		t.Fatal("manifest records the hold")
	}
}

func TestPolicyAndSchedule(t *testing.T) {
	f := newFixture(t)
	ctx := f.cli
	every, ret := f.svc.Policy(ctx, f.mb)
	if every != 24*time.Hour || ret != 48*time.Hour {
		t.Fatalf("defaults: %s %s", every, ret)
	}
	_, _ = f.ids.UpdateDomain(ctx, "example.org", func(d *identity.Domain) error {
		d.Settings = map[string]string{identity.DomainBackupInterval: "2h", identity.DomainBackupRetention: "720h"}
		return nil
	})
	if every, ret = f.svc.Policy(ctx, f.mb); every != 2*time.Hour || ret != 720*time.Hour {
		t.Fatalf("domain: %s %s", every, ret)
	}
	f.mb.Backup.Interval = 10 * time.Second // below the floor: clamped
	if every, _ = f.svc.Policy(ctx, f.mb); every != time.Minute {
		t.Fatalf("clamp: %s", every)
	}
	f.mb.Backup.Interval = 0
	f.mb.Backup.Disabled = true
	if every, _ = f.svc.Policy(ctx, f.mb); every != 0 {
		t.Fatalf("disabled: %s", every)
	}
	if _, err := f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Backup.Disabled = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Backup(ctx, f.mb.ID, RunOptions{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled run: %v", err)
	}
	if _, err := f.svc.Backup(ctx, f.mb.ID, RunOptions{Force: true}); err != nil {
		t.Fatalf("forced run of a disabled mailbox: %v", err)
	}
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Backup.Disabled = false; return nil })
	_, _ = f.ids.UpdateDomain(ctx, "example.org", func(d *identity.Domain) error {
		d.Settings = map[string]string{identity.DomainBackupInterval: "off"}
		return nil
	})
	f.mb.Backup.Disabled = false
	if every, _ = f.svc.Policy(ctx, f.mb); every != 0 {
		t.Fatalf("domain off: %s", every)
	}
	if err := f.svc.ValidateInterval(10 * time.Second); !errors.Is(err, ErrInvalid) {
		t.Fatal("validate floor")
	}
	if err := ParseDomainSetting("nope"); !errors.Is(err, ErrInvalid) {
		t.Fatal("domain setting")
	}
	// Orphaned mailbox (no accounts): defaults apply.
	orphan, _ := f.ids.CreateMailbox(ctx, identity.Mailbox{})
	if every, _ = f.svc.Policy(ctx, orphan); every != 24*time.Hour {
		t.Fatalf("orphan: %s", every)
	}
	// Enable spreads the first run over one interval.
	if err := f.svc.Enable(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	st, _ := f.cat.BackupState(ctx, orphan.ID)
	if !st.Scheduled || st.Every != 24*time.Hour || st.Due.Before(f.now) || st.Due.After(f.now.Add(24*time.Hour)) {
		t.Fatalf("enable: %+v", st)
	}
}

func TestRunnerSchedulesAndRunsJobs(t *testing.T) {
	f := newFixture(t)
	f.seed(t)
	r := NewRunner(RunnerConfig{Concurrency: 2, Batch: 10}, f.svc, f.q, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.Now = f.clock
	ctx := context.Background()
	// Reconcile schedules the mailbox; nothing is due yet.
	r.Schedule(ctx)
	st, _ := f.cat.BackupState(ctx, f.mb.ID)
	if !st.Scheduled || st.Due.IsZero() {
		t.Fatalf("not scheduled: %+v", st)
	}
	r.Drain(ctx)
	if st, _ = f.cat.BackupState(ctx, f.mb.ID); st.LastRun != "" {
		t.Fatal("ran before due")
	}
	// Past due: the scheduler enqueues, the runner runs.
	f.now = st.Due.Add(time.Second)
	r.Schedule(ctx)
	r.Drain(ctx)
	st, _ = f.cat.BackupState(ctx, f.mb.ID)
	if st.LastRun == "" || st.Status != "ok" || !st.Due.After(f.now) {
		t.Fatalf("after due: %+v", st)
	}
	// A restore job through the queue.
	job := Job{ID: uuid.NewString(), Kind: JobRestore, Mailbox: f.mb.ID, RunID: st.LastRun, State: JobPending, Requested: "root", Created: f.now}
	_ = f.cat.PutJob(ctx, job)
	_, _ = f.q.Enqueue(ctx, TaskJob, JobTask{Job: job.ID}, time.Time{})
	r.Drain(ctx)
	got, _ := f.cat.GetJob(ctx, job.ID)
	if got.State != JobDone || got.Target == uuid.Nil || got.Messages != 3 {
		t.Fatalf("restore job: %+v", got)
	}
	ej := Job{ID: uuid.NewString(), Kind: JobExport, Mailbox: f.mb.ID, RunID: st.LastRun, State: JobPending, Created: f.now}
	_ = f.cat.PutJob(ctx, ej)
	_, _ = f.q.Enqueue(ctx, TaskJob, JobTask{Job: ej.ID}, time.Time{})
	r.Drain(ctx)
	got, _ = f.cat.GetJob(ctx, ej.ID)
	if got.State != JobDone || got.ObjectKey == "" || got.Expires.IsZero() {
		t.Fatalf("export job: %+v", got)
	}
	// A job for a run that does not exist fails permanently.
	bad := Job{ID: uuid.NewString(), Kind: JobExport, Mailbox: f.mb.ID, RunID: "nope", State: JobPending, Created: f.now}
	_ = f.cat.PutJob(ctx, bad)
	_, _ = f.q.Enqueue(ctx, TaskJob, JobTask{Job: bad.ID}, time.Time{})
	r.Drain(ctx)
	if got, _ = f.cat.GetJob(ctx, bad.ID); got.State != JobFailed {
		t.Fatalf("bad job: %+v", got)
	}
	r.Schedule(ctx) // reconcile picks up the restored mailbox
	sum, _ := f.cat.Summary(ctx)
	if sum.Scheduled != 2 || sum.Runs != 1 || sum.Jobs != 0 {
		t.Fatalf("summary: %+v", sum)
	}
	// Export expiry removes the archive (and the failed job's record).
	f.now = f.now.Add(8 * 24 * time.Hour)
	exp, err := f.svc.Expire(f.cli, 10)
	if err != nil || exp.Exports != 2 {
		t.Fatalf("export expiry: %+v %v", exp, err)
	}
	if _, err := f.tg.Head(ctx, got.ObjectKey); !errors.Is(err, target.ErrNotFound) {
		t.Fatal("archive should be gone")
	}
}

func TestEmptyMailboxAndFuzzy(t *testing.T) {
	f := newFixture(t)
	// No index yet: an empty run with no packs.
	run, err := f.svc.Backup(f.cli, f.mb.ID, RunOptions{})
	if err != nil || run.Messages != 0 || len(run.Packs) != 0 {
		t.Fatalf("empty: %+v %v", run, err)
	}
	if _, err := f.svc.Backup(f.cli, uuid.New(), RunOptions{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown mailbox: %v", err)
	}
	// Restore of an empty run creates an empty mailbox.
	res, err := f.svc.Restore(f.cli, f.mb.ID, run.ID, RestoreOptions{})
	if err != nil || res.Messages != 0 {
		t.Fatalf("empty restore: %+v %v", res, err)
	}
}
