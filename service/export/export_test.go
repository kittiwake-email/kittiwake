package export

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"kittiwake/mail"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

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
	svc  *Service
	del  *delivery.Service
	mbx  *mailbox.Service
	adm  *admin.Service
	ids  *identity.Store
	idx  *index.Store
	mb   identity.Mailbox
	cli  context.Context
	user context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{ids: identity.New(meta), idx: index.New(idxp)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auditSt := audit.New(idxp)
	aud := auditor.New(auditSt, log, nil)
	msgs, atts := message.New(msgp), attach.New(attp)
	f.del = delivery.New(f.ids, f.idx, msgs, atts, queue.New(idxp), aud, log, delivery.Config{InlineMax: 16})
	f.mbx = mailbox.New(f.ids, f.idx, msgs, atts, nil, aud, log, mailbox.Config{})
	f.adm = admin.New(f.ids, f.idx, nil, aud)
	f.svc = New(f.ids, f.idx, msgs, atts, auditSt, f.del, aud, log)
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

func readTar(t *testing.T, data []byte, gz bool) map[string][]byte {
	t.Helper()
	var r io.Reader = bytes.NewReader(data)
	if gz {
		g, err := gzip.NewReader(r)
		if err != nil {
			t.Fatal(err)
		}
		r = g
	}
	tr := tar.NewReader(r)
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
	return files
}

func TestExportAndReimport(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	out, err := f.del.Deliver(ctx, delivery.Request{Recipient: "wido@example.org", Raw: []byte(withAtt)})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.mbx.SetFlags(f.user, f.mb.ID, out[0].MessageID, mailbox.FlagOp{Add: []string{"\\Seen", "\\Flagged", "$label1"}})
	nf, _ := f.mbx.CreateFolder(f.user, f.mb.ID, "Projects/2026")
	d2, _ := f.del.Import(f.user, f.mb.ID, nf.ID, []byte("From: b@example.com\r\nSubject: second\r\n\r\nbody\r\n"), []string{"\\Draft"}, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
	_, _ = f.adm.Hold(actor.With(ctx, actor.Actor{Username: "root@example.org", Roles: []string{"admin"}, Protocol: "admin"}), f.mb.ID, admin.HoldRequest{Reason: "case 7"})

	if _, err := f.svc.ExportMailbox(f.user, f.mb.ID, io.Discard, Options{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("user export: %v", err)
	}
	if _, err := f.svc.ExportMailbox(f.cli, uuid.New(), io.Discard, Options{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown mailbox: %v", err)
	}
	var buf bytes.Buffer
	sum, err := f.svc.ExportMailbox(f.cli, f.mb.ID, &buf, Options{Gzip: true})
	if err != nil || sum.Messages != 2 || sum.Folders != 8 || len(sum.Errors) != 0 {
		t.Fatalf("export: %+v %v", sum, err)
	}
	files := readTar(t, buf.Bytes(), true)
	prefix := "kittiwake-" + f.mb.ID.String() + "/"
	var man Manifest
	if err := json.Unmarshal(files[prefix+"manifest.json"], &man); err != nil {
		t.Fatal(err)
	}
	if man.Hold == nil || man.Hold.Reason != "case 7" || len(man.Messages) != 2 || len(man.Folders) != 8 || man.AuditCount == 0 || man.Accounts[0] != "wido@example.org" {
		t.Fatalf("manifest: hold=%v msgs=%d folders=%d audit=%d", man.Hold, len(man.Messages), len(man.Folders), man.AuditCount)
	}
	// Message file is byte-exact and flagged in the filename.
	var inboxMsg ManifestMessage
	for _, m := range man.Messages {
		if m.MessageID == out[0].MessageID {
			inboxMsg = m
		}
	}
	if string(files[inboxMsg.Path]) != rendered(t, withAtt) {
		t.Fatal("exported message is not the rendered form")
	}
	if !strings.HasSuffix(inboxMsg.Path, ":2,FSa") || !strings.Contains(inboxMsg.Path, "/maildir/cur/") {
		t.Fatalf("filename flags: %s", inboxMsg.Path)
	}
	if string(files[prefix+"maildir/dovecot-keywords"]) != "0 $label1\n" {
		t.Fatalf("keywords: %q", files[prefix+"maildir/dovecot-keywords"])
	}
	if !strings.HasPrefix(string(files[prefix+"maildir/dovecot-uidlist"]), "3 V") {
		t.Fatal("uidlist")
	}
	var projMsg ManifestMessage
	for _, m := range man.Messages {
		if m.MessageID == d2.MessageID {
			projMsg = m
		}
	}
	if !strings.Contains(projMsg.Path, "/maildir/.Projects.2026/cur/") || !strings.HasSuffix(projMsg.Path, ":2,D") {
		t.Fatalf("subfolder path: %s", projMsg.Path)
	}
	if !strings.Contains(string(files[prefix+"maildir/subscriptions"]), "Projects.2026") {
		t.Fatal("subscriptions")
	}
	if len(files[prefix+"audit.jsonl"]) == 0 {
		t.Fatal("audit log empty")
	}

	// Re-import the archive into a fresh mailbox.
	mb2, _ := f.ids.CreateMailbox(ctx, identity.Mailbox{})
	isum, err := f.svc.ImportArchive(f.cli, mb2.ID, bytes.NewReader(buf.Bytes()))
	if err != nil || isum.Messages != 2 || len(isum.Errors) != 0 {
		t.Fatalf("import: %+v %v", isum, err)
	}
	root, _ := f.idx.GetRoot(ctx, mb2.ID)
	proj := root.FolderByName("Projects/2026")
	if proj == nil || proj.Total != 1 || root.FolderByRole(index.RoleInbox).Total != 1 {
		t.Fatalf("imported folders: %+v", root.Folders)
	}
	recs, _, _ := f.idx.ListFolder(ctx, mb2.ID, root.FolderByRole(index.RoleInbox).ID, 0, 10)
	if !recs[0].HasFlag("\\Seen") || !recs[0].HasFlag("\\Flagged") || !recs[0].HasFlag("$label1") {
		t.Fatalf("imported flags: %v", recs[0].Flags)
	}
	precs, _, _ := f.idx.ListFolder(ctx, mb2.ID, proj.ID, 0, 10)
	if !precs[0].HasFlag("\\Draft") || precs[0].InternalDate.Year() != 2020 {
		t.Fatalf("imported draft: %+v", precs[0])
	}
}

func TestImportPlainMaildir(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	for _, d := range []string{"cur", "new", "tmp", ".Sent/cur", ".Work.Q4/cur"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o700)
	}
	write := func(p, body string, mtime time.Time) {
		fp := filepath.Join(dir, p)
		_ = os.WriteFile(fp, []byte(body), 0o600)
		_ = os.Chtimes(fp, mtime, mtime)
	}
	write("cur/1.a.host:2,S", "From: a@x\r\nSubject: one\r\n\r\n1\r\n", time.Date(2021, 5, 5, 0, 0, 0, 0, time.UTC))
	write("new/2.a.host", "From: a@x\r\nSubject: two\r\n\r\n2\r\n", time.Now())
	write(".Sent/cur/3.a.host:2,S", "From: wido@example.org\r\nSubject: sent\r\n\r\n3\r\n", time.Now())
	write(".Work.Q4/cur/4.a.host:2,F", "From: a@x\r\nSubject: work\r\n\r\n4\r\n", time.Now())
	write("cur/garbage", "not a message but still text", time.Now())
	sum, err := f.svc.ImportMaildir(f.cli, f.mb.ID, dir)
	if err != nil || sum.Messages != 5 {
		t.Fatalf("import: %+v %v", sum, err)
	}
	root, _ := f.idx.GetRoot(context.Background(), f.mb.ID)
	if root.FolderByRole(index.RoleInbox).Total != 3 || root.FolderByRole(index.RoleSent).Total != 1 || root.FolderByName("Work/Q4") == nil || root.FolderByName("Work/Q4").Total != 1 {
		t.Fatalf("folders: %+v", root.Folders)
	}
	recs, _, _ := f.idx.ListFolder(context.Background(), f.mb.ID, root.FolderByRole(index.RoleInbox).ID, 0, 10)
	seen := 0
	for _, r := range recs {
		if r.HasFlag("\\Seen") {
			seen++
			if r.InternalDate.Year() != 2021 {
				t.Fatalf("mtime not used: %v", r.InternalDate)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("seen flags: %d", seen)
	}
	if _, err := f.svc.ImportMaildir(f.cli, f.mb.ID, filepath.Join(dir, "nope")); err == nil {
		t.Fatal("missing dir")
	}
}

// rendered returns what Kittiwake serves for a raw message: the document
// rendered back to RFC 5322 (the original bytes are not kept).
func rendered(t *testing.T, raw string) string {
	t.Helper()
	p, err := mail.Parse([]byte(raw), mail.Options{InlineMax: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	out, err := mail.Render(p.Doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
