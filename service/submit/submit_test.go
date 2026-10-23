package submit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
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

func TestSendOnlySavesSentButCannotImport(t *testing.T) {
	f := newFixture(t)
	a := actor.From(f.user)
	a.Roles = []string{identity.RoleSend}
	ctx := actor.With(context.Background(), a)
	res, err := f.svc.Send(ctx, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)})
	if err != nil || res.SentCopy == nil {
		t.Fatalf("send-only Sent copy: %+v %v", res, err)
	}
	if _, err := f.svc.del.Import(ctx, f.mb.ID, res.SentCopy.FolderID, []byte(outgoing), nil, time.Time{}); !errors.Is(err, delivery.ErrForbidden) {
		t.Fatalf("send-only import: %v", err)
	}
	if _, err := f.svc.del.SaveSent(ctx, uuid.New(), []byte(outgoing), time.Time{}); !errors.Is(err, delivery.ErrForbidden) {
		t.Fatalf("cross-mailbox Sent copy: %v", err)
	}
}

func TestSendAccountHeaderAndMissingHeaders(t *testing.T) {
	f := newFixture(t)
	acct := actor.From(f.user).AccountID.String()
	// A client-supplied account header is replaced by ours.
	raw := "From: wido@example.org\r\nTo: bob@other.example\r\nX-Kittiwake-Account: 00000000-0000-0000-0000-000000000001\r\nSubject: x\r\n\r\nhi\r\n"
	res, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(raw)})
	if err != nil {
		t.Fatal(err)
	}
	data := string(f.relay.data)
	if strings.Count(data, AccountHeader+":") != 1 || !strings.Contains(data, "\r\n"+AccountHeader+": "+acct+"\r\n") {
		t.Fatalf("account header: %q", data)
	}
	// Message-ID and Date were missing: added to what is relayed and to the Sent copy.
	if !strings.Contains(data, "Message-ID: <") || !strings.Contains(data, "@mx.test>\r\n") || !strings.Contains(data, "\r\nDate: ") {
		t.Fatalf("Message-ID/Date not added: %q", data)
	}
	if h := res.SentCopy.MessageIDHeader; h == "" || !strings.Contains(data, h) {
		t.Fatalf("Sent copy Message-ID %q differs from the relayed one", h)
	}
	// Present headers are kept as they are.
	dated := strings.Replace(outgoing, "Subject: hi", "Date: Mon, 19 Oct 2026 10:00:00 +0200\r\nSubject: hi", 1)
	if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(dated)}); err != nil {
		t.Fatal(err)
	}
	if data := string(f.relay.data); strings.Count(data, "Message-ID:") != 1 || strings.Count(data, "Date:") != 1 || !strings.Contains(data, "<o1@example.org>") {
		t.Fatalf("headers added to a complete message: %q", data)
	}
}

func TestSubmissionReceivedAndRelayError(t *testing.T) {
	f := newFixture(t)
	a := actor.From(f.user)
	a.Protocol = "submission"
	ctx := actor.With(context.Background(), a)
	if err := f.svc.CheckSender(ctx, f.mb.ID, "Info@Example.org"); err != nil {
		t.Fatalf("alias as envelope sender: %v", err)
	}
	if err := f.svc.CheckSender(ctx, f.mb.ID, "ceo@example.org"); !errors.Is(err, ErrBadSender) {
		t.Fatalf("catch-all as envelope sender: %v", err)
	}
	if _, err := f.svc.Send(ctx, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(f.relay.data), "(Kittiwake) with ESMTPSA id") {
		t.Fatalf("received: %q", f.relay.data)
	}
	// The relay's SMTP reply survives the wrapping.
	f.relay.err = &smtp.SMTPError{Code: 452, EnhancedCode: smtp.EnhancedCode{4, 5, 3}, Message: "Too many recipients"}
	_, err := f.svc.Send(ctx, Request{MailboxID: f.mb.ID, Raw: []byte(outgoing)})
	var se *smtp.SMTPError
	if !errors.Is(err, ErrRelay) || !errors.As(err, &se) || se.Code != 452 {
		t.Fatalf("relay error: %v", err)
	}
}

func TestSendAsAlias(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	alias := func(addr string, enabled bool, to ...identity.Destination) {
		t.Helper()
		if _, err := f.ids.CreateAlias(ctx, identity.Alias{Address: addr, Enabled: enabled, Destinations: to}); err != nil {
			t.Fatal(err)
		}
	}
	// sales@ → wido@ (address destination), support@ → sales@ (chain),
	// old@ → mailbox but disabled, team@ → the colleague's address only.
	alias("sales@example.org", true, identity.Destination{Address: "wido@example.org"}, identity.Destination{Address: "boss@other.example"})
	alias("support@example.org", true, identity.Destination{Address: "sales@example.org"})
	alias("old@example.org", false, identity.Destination{MailboxID: f.mb.ID})
	_, _ = f.ids.CreateAccount(ctx, identity.Account{Username: "colleague@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "send"}, Enabled: true})
	alias("team@example.org", true, identity.Destination{Address: "colleague@example.org"})

	ids, err := f.svc.Identities(f.user, f.mb.ID)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, id := range ids {
		got = append(got, id.Email)
	}
	if strings.Join(got, ",") != "wido@example.org,info@example.org,sales@example.org,support@example.org" {
		t.Fatalf("identities: %v", got)
	}
	for _, from := range []string{"info@example.org", "sales@example.org", "Support <SUPPORT@example.org>"} {
		raw := "From: " + from + "\r\nTo: bob@other.example\r\nSubject: x\r\n\r\nhi\r\n"
		if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(raw)}); err != nil {
			t.Fatalf("send as %s: %v", from, err)
		}
		if err := f.svc.CheckSender(f.user, f.mb.ID, firstAddress(from)); err != nil {
			t.Fatalf("envelope %s: %v", from, err)
		}
	}
	if f.relay.from != "support@example.org" {
		t.Fatalf("envelope sender from header: %q", f.relay.from)
	}
	for _, from := range []string{"old@example.org", "team@example.org", "colleague@example.org", "anything@example.org"} {
		raw := "From: " + from + "\r\nTo: bob@other.example\r\nSubject: x\r\n\r\nhi\r\n"
		if _, err := f.svc.Send(f.user, Request{MailboxID: f.mb.ID, Raw: []byte(raw)}); !errors.Is(err, ErrBadSender) {
			t.Fatalf("send as %s: %v", from, err)
		}
	}
}
