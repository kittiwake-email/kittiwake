package mailbox

import (
	"context"
	"errors"
	"io"
	"kittiwake/mail"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
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
	ids  *identity.Store
	idx  *index.Store
	aud  *audit.Store
	mb   identity.Mailbox
	user context.Context
	ro   context.Context
	adm  context.Context
	anon context.Context
	now  time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{ids: identity.New(meta), idx: index.New(idxp), aud: audit.New(idxp), now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	now := func() time.Time { f.now = f.now.Add(time.Second); return f.now }
	f.idx.Now = now
	msgs := message.New(msgp)
	atts := attach.New(attp)
	q := queue.New(idxp)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(f.aud, log, nil)
	aud.Now = now
	f.aud.Now = now
	f.svc = New(f.ids, f.idx, msgs, atts, q, aud, log, Config{TrashRetention: 24 * time.Hour})
	f.svc.Now = now
	f.del = delivery.New(f.ids, f.idx, msgs, atts, q, aud, log, delivery.Config{})
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	acct, _ := f.ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "write", "send"}, Enabled: true})
	f.user = actor.With(ctx, actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: f.mb.ID, Roles: acct.Roles, Protocol: "jmap", ClientIP: "::1"})
	f.ro = actor.With(ctx, actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: f.mb.ID, Roles: []string{"read"}, Protocol: "jmap"})
	f.adm = actor.With(ctx, actor.Actor{AccountID: uuid.New(), Username: "admin@example.org", Roles: []string{"admin"}, Protocol: "admin"})
	other, _ := f.ids.CreateMailbox(ctx, identity.Mailbox{})
	f.anon = actor.With(ctx, actor.Actor{AccountID: uuid.New(), Username: "x@example.org", MailboxID: other.ID, Roles: []string{"read", "write"}, Protocol: "jmap"})
	return f
}

func (f *fixture) deliver(t *testing.T, subject, from string) delivery.Delivered {
	t.Helper()
	return f.deliverReply(t, subject, from, "")
}

func (f *fixture) deliverReply(t *testing.T, subject, from, inReplyTo string) delivery.Delivered {
	t.Helper()
	raw := "From: " + from + "\r\nTo: wido@example.org\r\nSubject: " + subject + "\r\nMessage-ID: <" + uuid.NewString() + "@x>\r\n"
	if inReplyTo != "" {
		raw += "In-Reply-To: <" + inReplyTo + ">\r\n"
	}
	raw += "\r\nbody " + subject + "\r\n"
	out, err := f.del.Deliver(context.Background(), delivery.Request{Recipient: "wido@example.org", Raw: []byte(raw)})
	if err != nil {
		t.Fatal(err)
	}
	return out[0]
}

func TestAuthorization(t *testing.T) {
	f := newFixture(t)
	d := f.deliver(t, "hello", "a@b.c")
	if _, err := f.svc.Root(f.anon, f.mb.ID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("other user: %v", err)
	}
	if _, err := f.svc.Root(f.user, f.mb.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Root(f.adm, f.mb.ID); err != nil {
		t.Fatalf("admin support access: %v", err)
	}
	if _, err := f.svc.SetFlags(f.ro, f.mb.ID, d.MessageID, FlagOp{Add: []string{"\\Seen"}}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("read-only role: %v", err)
	}
	if _, err := f.svc.Root(f.user, uuid.New()); !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrForbidden) {
		t.Fatalf("unknown mailbox: %v", err)
	}
	// Denied access is audited.
	recs, _, _ := f.aud.Read(context.Background(), audit.Query{Mailbox: f.mb.ID, From: f.now.Add(-time.Hour), To: f.now.Add(time.Hour), Action: "list"})
	denied := 0
	for _, r := range recs {
		if r.Result == audit.ResultDenied {
			denied++
		}
	}
	if denied != 1 {
		t.Fatalf("denied audits: %d", denied)
	}
}

func TestMessagesLifecycle(t *testing.T) {
	f := newFixture(t)
	ctx := f.user
	d1 := f.deliver(t, "first", "Alice <alice@x.org>")
	d2 := f.deliver(t, "second", "bob@x.org")
	root, _ := f.svc.Root(ctx, f.mb.ID)
	inbox := root.FolderByRole(index.RoleInbox)
	if inbox.Total != 2 {
		t.Fatal("inbox count")
	}
	recs, _, err := f.svc.List(ctx, f.mb.ID, inbox.ID, 0, 10)
	if err != nil || len(recs) != 2 {
		t.Fatal(err)
	}
	doc, err := f.svc.Document(ctx, f.mb.ID, d1.MessageID)
	if err != nil || doc.Subject != "first" || doc.From[0].Name != "Alice" {
		t.Fatalf("doc: %+v %v", doc, err)
	}
	raw, err := f.svc.Raw(ctx, f.mb.ID, d1.MessageID)
	if err != nil || len(raw) == 0 {
		t.Fatal(err)
	}
	// Flags.
	rec, err := f.svc.SetFlags(ctx, f.mb.ID, d1.MessageID, FlagOp{Add: []string{"\\Seen", "$important"}})
	if err != nil || !rec.HasFlag("\\Seen") || !rec.HasFlag("$important") {
		t.Fatalf("flags: %+v %v", rec, err)
	}
	rec, _ = f.svc.SetFlags(ctx, f.mb.ID, d1.MessageID, FlagOp{Remove: []string{"\\seen"}})
	if rec.HasFlag("\\Seen") || !rec.HasFlag("$important") {
		t.Fatalf("remove: %+v", rec)
	}
	rec, _ = f.svc.SetFlags(ctx, f.mb.ID, d1.MessageID, FlagOp{Replace: []string{"\\Flagged"}})
	if len(rec.Flags) != 1 {
		t.Fatalf("replace: %+v", rec)
	}
	if _, err := f.svc.SetFlags(ctx, f.mb.ID, d1.MessageID, FlagOp{Add: []string{"\\Recent"}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("\\Recent should be refused")
	}
	if _, err := f.svc.SetFlags(ctx, f.mb.ID, d1.MessageID, FlagOp{Add: []string{"bad flag"}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("space in keyword")
	}
	// Folder ops and move.
	nf, err := f.svc.CreateFolder(ctx, f.mb.ID, "Archive/2026")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := f.svc.Move(ctx, f.mb.ID, d2.MessageID, nf.ID)
	if err != nil || moved.UID != 1 {
		t.Fatal(err)
	}
	loc, _, _ := f.svc.Get(ctx, f.mb.ID, d2.MessageID)
	if loc.FolderID != nf.ID {
		t.Fatal("move")
	}
	if err := f.svc.DeleteFolder(ctx, f.mb.ID, nf.ID, false); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("delete non-empty: %v", err)
	}
	if err := f.svc.RenameFolder(ctx, f.mb.ID, nf.ID, "Archive/2025"); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.DeleteFolder(ctx, f.mb.ID, nf.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.svc.Get(ctx, f.mb.ID, d2.MessageID); !errors.Is(err, ErrNotFound) {
		t.Fatal("message should be gone with folder")
	}
	// Destroy and expunge.
	d3 := f.deliver(t, "third", "c@x.org")
	if err := f.svc.Destroy(ctx, f.mb.ID, d3.MessageID); err != nil {
		t.Fatal(err)
	}
	_, _ = f.svc.SetFlags(ctx, f.mb.ID, d1.MessageID, FlagOp{Add: []string{"\\Deleted"}})
	uids, err := f.svc.Expunge(ctx, f.mb.ID, inbox.ID, nil)
	if err != nil || len(uids) != 1 || uids[0] != d1.UID {
		t.Fatalf("expunge: %v %v", uids, err)
	}
	ents, _, _, _ := f.idx.ListRetention(context.Background(), f.mb.ID, time.Time{}, 10)
	if len(ents) != 3 {
		t.Fatalf("retention: %d", len(ents))
	}
	root, _ = f.svc.Root(ctx, f.mb.ID)
	if root.Usage.Messages != 0 {
		t.Fatalf("usage: %+v", root.Usage)
	}
	// Changes.
	ch, _, cur, err := f.svc.Changes(ctx, f.mb.ID, 0, 1000)
	if err != nil || len(ch) == 0 || cur != root.ModSeq {
		t.Fatalf("changes: %d %d %v", len(ch), cur, err)
	}
	// Subscription allowed; quota.
	if err := f.svc.SetSubscribed(ctx, f.mb.ID, inbox.ID, false); err != nil {
		t.Fatal(err)
	}
	qi, err := f.svc.Quota(ctx, f.mb.ID)
	if err != nil || qi.Usage.Messages != 0 {
		t.Fatal(err)
	}
}

func TestQuery(t *testing.T) {
	f := newFixture(t)
	ctx := f.user
	a := f.deliver(t, "Invoice March", "billing@acme.example")
	adoc, _ := f.svc.Document(ctx, f.mb.ID, a.MessageID)
	f.deliverReply(t, "Re: Invoice March", "wido@example.org", adoc.MessageID)
	f.deliver(t, "Lunch?", "friend@x.org")
	root, _ := f.svc.Root(ctx, f.mb.ID)
	inbox := root.FolderByRole(index.RoleInbox).ID
	res, err := f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{InFolder: inbox}, Sort: SortReceivedAt})
	if err != nil || res.Total != 3 || res.Records[0].Subject != "Lunch?" {
		t.Fatalf("default sort desc: %+v %v", res, err)
	}
	res, _ = f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{Text: "invoice"}, Asc: true})
	if res.Total != 2 || res.Records[0].MessageID != a.MessageID {
		t.Fatalf("text: %+v", res)
	}
	res, _ = f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{From: "acme"}})
	if res.Total != 1 {
		t.Fatal("from")
	}
	res, _ = f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{InFolder: inbox}, CollapseThreads: true})
	if res.Total != 2 {
		t.Fatalf("collapse threads: %d", res.Total)
	}
	res, _ = f.svc.Query(ctx, f.mb.ID, Query{Position: 1, Limit: 1})
	if res.Total != 3 || len(res.Records) != 1 {
		t.Fatalf("paging: %+v", res)
	}
	_, _ = f.svc.SetFlags(ctx, f.mb.ID, a.MessageID, FlagOp{Add: []string{"\\Seen"}})
	res, _ = f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{NotKeyword: "\\Seen"}})
	if res.Total != 2 {
		t.Fatal("notKeyword")
	}
	res, _ = f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{HasKeyword: "\\Seen"}})
	if res.Total != 1 {
		t.Fatal("hasKeyword")
	}
	if _, err := f.svc.Query(ctx, f.mb.ID, Query{Filter: Filter{InFolder: "nope"}}); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown folder")
	}
}

func TestLegalHold(t *testing.T) {
	f := newFixture(t)
	ctx := f.user
	d := f.deliver(t, "evidence", "a@x.org")
	_, _ = f.ids.UpdateMailbox(context.Background(), f.mb.ID, func(m *identity.Mailbox) error {
		m.Hold = &identity.Hold{Reason: "case 42", AcceptDelivery: true}
		return nil
	})
	// Reads work.
	if _, err := f.svc.Document(ctx, f.mb.ID, d.MessageID); err != nil {
		t.Fatal(err)
	}
	// Every mutation is refused.
	if _, err := f.svc.SetFlags(ctx, f.mb.ID, d.MessageID, FlagOp{Add: []string{"\\Seen"}}); !errors.Is(err, ErrHold) {
		t.Fatalf("flags on hold: %v", err)
	}
	if err := f.svc.Destroy(ctx, f.mb.ID, d.MessageID); !errors.Is(err, ErrHold) {
		t.Fatalf("destroy on hold: %v", err)
	}
	if _, err := f.svc.CreateFolder(ctx, f.mb.ID, "X"); !errors.Is(err, ErrHold) {
		t.Fatalf("folder on hold: %v", err)
	}
	// Admin is also refused writes (hold is absolute).
	if _, err := f.svc.Move(f.adm, f.mb.ID, d.MessageID, "f2"); !errors.Is(err, ErrHold) {
		t.Fatalf("admin write on hold: %v", err)
	}
	// Sealed: no user access at all, admin still reads.
	_, _ = f.ids.UpdateMailbox(context.Background(), f.mb.ID, func(m *identity.Mailbox) error { m.Hold.Sealed = true; return nil })
	if _, err := f.svc.Root(ctx, f.mb.ID); !errors.Is(err, ErrSealed) {
		t.Fatalf("sealed: %v", err)
	}
	if _, err := f.svc.Root(f.adm, f.mb.ID); err != nil {
		t.Fatalf("admin on sealed: %v", err)
	}
	// Delivery still accepted.
	f.deliver(t, "more evidence", "b@x.org")
}

func TestWatch(t *testing.T) {
	f := newFixture(t)
	ev, stop, err := f.svc.Watch(f.user, f.mb.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	f.deliver(t, "ping", "a@x.org")
	select {
	case <-ev:
	case <-time.After(time.Second):
		t.Fatal("no notification")
	}
}

func TestAttachmentMembershipAndImportAuthorization(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	victim, err := f.ids.CreateMailbox(ctx, identity.Mailbox{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := f.idx.EnsureRoot(ctx, victim.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("From: a@example.org\r\nContent-Type: application/octet-stream\r\n\r\n" + strings.Repeat("private", 2000))
	parsed, err := mail.Parse(raw, mail.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Attachments) != 1 {
		t.Fatal("attachment fixture")
	}
	_, err = f.del.Import(f.adm, victim.ID, root.Folders[0].ID, raw, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = f.svc.Attachment(f.user, f.mb.ID, parsed.Attachments[0].Hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-mailbox attachment: %v", err)
	}
	own, _ := f.idx.EnsureRoot(ctx, f.mb.ID)
	if _, err = f.del.Import(f.ro, f.mb.ID, own.Folders[0].ID, raw, nil, time.Time{}); !errors.Is(err, delivery.ErrForbidden) {
		t.Fatalf("read-only import: %v", err)
	}
	if _, err = f.del.Import(ctx, f.mb.ID, own.Folders[0].ID, raw, nil, time.Time{}); !errors.Is(err, delivery.ErrForbidden) {
		t.Fatalf("anonymous import: %v", err)
	}
	if _, err = f.del.Import(f.user, f.mb.ID, own.Folders[0].ID, raw, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if data, _, err := f.svc.Attachment(f.user, f.mb.ID, parsed.Attachments[0].Hash); err != nil || len(data) == 0 {
		t.Fatalf("own attachment: %v", err)
	}
}
func TestExistingActorLosesRevokedAccountAccess(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	a := actor.From(f.user)
	if _, err := f.svc.Root(f.user, f.mb.ID); err != nil {
		t.Fatal(err)
	}
	_, err := f.ids.UpdateAccount(ctx, a.AccountID, func(a *identity.Account) error { a.Enabled = false; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.CheckAccess(f.user, f.mb.ID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("disabled account still authorized: %v", err)
	}
}
