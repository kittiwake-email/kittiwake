package submit

import (
	"context"
	"errors"
	"io"
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

type fakeRelay struct {
	from string
	to   []string
	data []byte
	err  error
}

func (f *fakeRelay) Send(_ context.Context, from string, to []string, data []byte) error {
	f.from, f.to, f.data = from, to, data
	return f.err
}

type fixture struct {
	svc   *Service
	relay *fakeRelay
	ids   *identity.Store
	idx   *index.Store
	aud   *audit.Store
	mb    identity.Mailbox
	user  context.Context
	noRol context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{ids: identity.New(meta), idx: index.New(idxp), aud: audit.New(idxp), relay: &fakeRelay{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(f.aud, log, nil)
	del := delivery.New(f.ids, f.idx, message.New(msgp), attach.New(attp), queue.New(idxp), aud, log, delivery.Config{})
	f.svc = New(Config{Hostname: "mx.test", MaxMessageSize: 4096}, f.ids, f.idx, del, aud, f.relay, log)
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	acct, _ := f.ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "write", "send"}, Enabled: true})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: f.mb.ID}}})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: f.mb.ID}}})
	f.user = actor.With(ctx, actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: f.mb.ID, Roles: acct.Roles, Protocol: "jmap", ClientIP: "2001:db8::7", SessionID: "sess1"})
	f.noRol = actor.With(ctx, actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: f.mb.ID, Roles: []string{"read"}, Protocol: "jmap"})
	return f
}

const outgoing = "From: Wido <wido@example.org>\r\nTo: Bob <bob@other.example>\r\nCc: carol@other.example\r\nBcc: secret@other.example\r\nSubject: hi\r\nMessage-ID: <o1@example.org>\r\n\r\nhello\r\n"

func TestIdentities(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// A second account on the same mailbox is not an identity of the first.
	_, _ = f.ids.CreateAccount(ctx, identity.Account{Username: "shared@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "send"}, Enabled: true})
	if _, err := f.svc.SetName(f.user, " Wido den Hollander "); err != nil {
		t.Fatal(err)
	}
	ids, err := f.svc.Identities(f.user, f.mb.ID)
	if err != nil || len(ids) != 2 || ids[0].Email != "wido@example.org" || ids[1].Email != "info@example.org" {
		t.Fatalf("identities: %+v %v", ids, err)
	}
	if ids[0].Name != "Wido den Hollander" || ids[1].Name != "Wido den Hollander" {
		t.Fatalf("display name not carried: %+v", ids)
	}
	// Sending as the other account's address is refused; as an alias it is allowed.
	msg := "From: shared@example.org\r\nTo: bob@other.example\r\nSubject: x\r\n\r\nhi\r\n"
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(msg)}); !errors.Is(err, ErrBadSender) {
		t.Fatalf("other account's address accepted as From: %v", err)
	}
	msg = "From: Info <info@example.org>\r\nTo: bob@other.example\r\nSubject: x\r\n\r\nhi\r\n"
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(msg)}); err != nil {
		t.Fatalf("alias refused as From: %v", err)
	}
	if _, err := f.svc.SetName(f.user, "bad\nname"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("newline in name accepted: %v", err)
	}
	other := actor.With(context.Background(), actor.Actor{MailboxID: uuid.New(), Roles: []string{"read"}})
	if _, err := f.svc.Identities(other, f.mb.ID); !errors.Is(err, ErrForbidden) {
		t.Fatal("other mailbox")
	}
}

func TestSendHappyPath(t *testing.T) {
	f := newFixture(t)
	res, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)})
	if err != nil {
		t.Fatal(err)
	}
	if res.EnvelopeFrom != "wido@example.org" || len(res.Recipients) != 3 || res.SentCopy == nil {
		t.Fatalf("result: %+v", res)
	}
	if f.relay.from != "wido@example.org" || len(f.relay.to) != 3 {
		t.Fatalf("relay envelope: %s %v", f.relay.from, f.relay.to)
	}
	data := string(f.relay.data)
	if !strings.HasPrefix(data, "Received: from [2001:db8::7] by mx.test (Kittiwake) with JMAP id sess1 for <wido@example.org>;") {
		t.Fatalf("received header: %q", data[:80])
	}
	if strings.Contains(data, "Bcc:") {
		t.Fatal("Bcc leaked to the relay")
	}
	if !strings.Contains(data, "Cc: carol@other.example") {
		t.Fatal("Cc stripped")
	}
	// Sent copy keeps Bcc and is flagged seen in the Sent folder.
	root, _ := f.idx.GetRoot(context.Background(), f.mb.ID)
	if root.FolderByRole(index.RoleSent).Total != 1 {
		t.Fatal("no sent copy")
	}
	rec, _ := f.idx.GetRecord(context.Background(), f.mb.ID, res.SentCopy.FolderID, res.SentCopy.UID)
	if !rec.HasFlag(index.FlagSeen) {
		t.Fatal("sent copy not seen")
	}
	recs, _, _ := f.aud.Read(context.Background(), audit.Query{Mailbox: f.mb.ID, From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour), Action: "send"})
	if len(recs) != 1 || recs[0].Result != audit.ResultOK {
		t.Fatalf("audit: %+v", recs)
	}
	// Explicit envelope and no sent copy.
	no := false
	res, err = f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing), From: "info@example.org", To: []string{"x@y.example"}, SaveToSent: &no})
	if err != nil || res.SentCopy != nil || f.relay.from != "info@example.org" || len(f.relay.to) != 1 {
		t.Fatalf("explicit envelope: %+v %v", res, err)
	}
}

func TestSendRefusals(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.svc.Send(f.noRol, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("no send role: %v", err)
	}
	bad := strings.Replace(outgoing, "From: Wido <wido@example.org>", "From: ceo@example.org", 1)
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(bad)}); !errors.Is(err, ErrBadSender) {
		t.Fatalf("catch-all is not an identity: %v", err)
	}
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing), From: "spoof@example.org"}); !errors.Is(err, ErrBadSender) {
		t.Fatalf("envelope from spoof: %v", err)
	}
	spoofSender := strings.Replace(outgoing, "Subject: hi", "Sender: other@example.org\r\nSubject: hi", 1)
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(spoofSender)}); !errors.Is(err, ErrBadSender) {
		t.Fatalf("sender header spoof: %v", err)
	}
	noRcpt := "From: wido@example.org\r\nSubject: x\r\n\r\nbody\r\n"
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(noRcpt)}); !errors.Is(err, ErrNoRecipients) {
		t.Fatalf("no recipients: %v", err)
	}
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing), To: []string{"not an address"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad recipient: %v", err)
	}
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(strings.Repeat("x", 5000))}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("too large: %v", err)
	}
	// Relay failure: error, nothing stored.
	f.relay.err = errors.New("452 try later")
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)}); !errors.Is(err, ErrRelay) {
		t.Fatalf("relay error: %v", err)
	}
	root, _ := f.idx.EnsureRoot(ctx, f.mb.ID)
	if root.FolderByRole(index.RoleSent).Total != 0 {
		t.Fatal("sent copy stored despite relay failure")
	}
	f.relay.err = nil
	// Hold: refused unless AllowSend.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Hold = &identity.Hold{Reason: "r"}; return nil })
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)}); !errors.Is(err, ErrHold) {
		t.Fatalf("hold: %v", err)
	}
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Hold.AllowSend = true; return nil })
	no := false
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing), SaveToSent: &no}); err != nil {
		t.Fatalf("hold with allow-send: %v", err)
	}
}

func TestStripHeader(t *testing.T) {
	raw := "To: a\r\nBcc: x,\r\n y\r\nSubject: s\r\n\r\nbody\r\nBcc: not a header\r\n"
	got := string(stripHeader([]byte(raw), "bcc"))
	if got != "To: a\r\nSubject: s\r\n\r\nbody\r\nBcc: not a header\r\n" {
		t.Fatalf("%q", got)
	}
	if string(stripHeader([]byte("no headers"), "Bcc")) != "no headers" {
		t.Fatal("no header block")
	}
}
