package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"dovetail/mail"
	"dovetail/rados"
	"dovetail/service/auditor"
	"dovetail/store/attach"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
	"dovetail/store/message"
	"dovetail/store/queue"
)

type fixture struct {
	svc  *Service
	ids  *identity.Store
	idx  *index.Store
	msgs *message.Store
	atts *attach.Store
	q    *queue.Store
	aud  *audit.Store
	mb   identity.Mailbox
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{ids: identity.New(meta), idx: index.New(idxp), msgs: message.New(msgp), atts: attach.New(attp), q: queue.New(idxp), aud: audit.New(idxp)}
	aud := auditor.New(f.aud, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 1 << 20
	}
	f.svc = New(f.ids, f.idx, f.msgs, f.atts, f.q, aud, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "off.example", Enabled: false})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = f.ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read"}, Enabled: true})
	return f
}

const simpleMsg = "From: Alice <alice@example.com>\r\nTo: wido@example.org\r\nSubject: Hello\r\nMessage-ID: <m1@example.com>\r\nDate: Tue, 21 Oct 2026 10:00:00 +0000\r\n\r\nHi there\r\n"

func TestResolve(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	mb2, _ := f.ids.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb2.ID}, {Address: "wido@example.org"}}})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "loop@example.org", Enabled: true, Destinations: []identity.Destination{{Address: "loop2@example.org"}}})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "loop2@example.org", Enabled: true, Destinations: []identity.Destination{{Address: "loop@example.org"}}})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "off@example.org", Enabled: false, Destinations: []identity.Destination{{MailboxID: mb2.ID}}})

	tg, err := f.svc.Resolve(ctx, "WIDO@example.org")
	if err != nil || len(tg) != 1 || tg[0].MailboxID != f.mb.ID || tg[0].Via != "wido@example.org" {
		t.Fatalf("exact: %+v %v", tg, err)
	}
	tg, err = f.svc.Resolve(ctx, "info@example.org")
	if err != nil || len(tg) != 2 || tg[0].MailboxID != mb2.ID || tg[1].MailboxID != f.mb.ID {
		t.Fatalf("alias fanout: %+v %v", tg, err)
	}
	if _, err := f.svc.Resolve(ctx, "nobody@example.org"); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := f.svc.Resolve(ctx, "x@nope.example"); !errors.Is(err, ErrUnknownDomain) {
		t.Fatalf("unknown domain: %v", err)
	}
	if _, err := f.svc.Resolve(ctx, "x@off.example"); !errors.Is(err, ErrUnknownDomain) {
		t.Fatalf("disabled domain: %v", err)
	}
	if _, err := f.svc.Resolve(ctx, "loop@example.org"); !errors.Is(err, ErrAliasLoop) {
		t.Fatalf("loop: %v", err)
	}
	if _, err := f.svc.Resolve(ctx, "off@example.org"); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("disabled alias: %v", err)
	}
	if _, err := f.svc.Resolve(ctx, "not an address"); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("invalid: %v", err)
	}
	// Catch-all.
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb2.ID}}})
	tg, err = f.svc.Resolve(ctx, "anything@example.org")
	if err != nil || len(tg) != 1 || tg[0].Via != "@example.org" {
		t.Fatalf("catch-all: %+v %v", tg, err)
	}
	// Exact still wins over catch-all.
	tg, _ = f.svc.Resolve(ctx, "wido@example.org")
	if tg[0].MailboxID != f.mb.ID {
		t.Fatal("catch-all shadowed account")
	}
}

func TestDeliverStoresEverything(t *testing.T) {
	f := newFixture(t, Config{SpamHeader: "X-Spam-Flag", SpamValue: "YES", InlineMax: 16, SearchEvents: true})
	ctx := context.Background()
	pdf := strings.Repeat("PDFDATA", 100)
	raw := "From: a@example.com\r\nTo: wido@example.org\r\nSubject: =?utf-8?q?Caf=C3=A9?=\r\nMessage-ID: <x1@example.com>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nSee attached\r\n--B\r\nContent-Type: application/pdf; name=a.pdf\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=a.pdf\r\n\r\n" + b64lines(pdf) + "\r\n--B--\r\n"
	out, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Sender: "a@example.com", Raw: []byte(raw), ClientIP: "::1"})
	if err != nil || len(out) != 1 || out[0].UID != 1 {
		t.Fatalf("deliver: %+v %v", out, err)
	}
	d := out[0]
	root, _ := f.idx.GetRoot(ctx, f.mb.ID)
	if root.FolderByID(d.FolderID).Role != index.RoleInbox || root.Usage.Messages != 1 || root.Usage.Bytes == 0 {
		t.Fatalf("root: %+v", root)
	}
	if d.MessageIDHeader != "x1@example.com" {
		t.Fatalf("message-id header not reported: %q", d.MessageIDHeader)
	}
	// The message object carries its location; the record is compact.
	loc, err := f.msgs.Location(ctx, f.mb.ID, d.MessageID)
	if err != nil || loc != d.FolderID+"/1" {
		t.Fatalf("location: %q %v", loc, err)
	}
	rec, err := f.idx.GetRecord(ctx, f.mb.ID, d.FolderID, 1)
	if err != nil || rec.MessageID != d.MessageID || rec.Subject != "Café" || !rec.HasAttachments || rec.FromEmail != "a@example.com" {
		t.Fatalf("record: %+v %v", rec, err)
	}
	docJSON, err := f.msgs.GetDoc(ctx, f.mb.ID, d.MessageID)
	if err != nil || !strings.Contains(string(docJSON), `"subject":"Café"`) {
		t.Fatalf("doc: %s %v", docJSON, err)
	}
	var doc mail.Document
	_ = json.Unmarshal(docJSON, &doc)
	if !strings.Contains(doc.RawHeaders, "Message-ID: <x1@example.com>") {
		t.Fatal("verbatim headers lost")
	}
	refs := mail.AttachmentHashes(&doc)
	if len(refs) != 1 {
		t.Fatalf("refs: %v", refs)
	}
	info, err := f.atts.Stat(ctx, refs[0])
	if err != nil || info.Refs != 1 || info.ContentType != "application/pdf" {
		t.Fatalf("attachment: %+v %v", info, err)
	}
	// No raw copy is kept for ordinary mail; the rendered size matches the record.
	if _, err := f.msgs.GetRaw(ctx, f.mb.ID, d.MessageID); err == nil {
		t.Fatal("raw copy kept for an unsigned message")
	}
	rendered, err := mail.Render(&doc, func(h string) ([]byte, error) { return f.atts.Get(ctx, h) })
	if err != nil || int64(len(rendered)) != rec.Size || rec.Size != doc.Size {
		t.Fatalf("render: %v len=%d rec=%d doc=%d", err, len(rendered), rec.Size, doc.Size)
	}
	if !strings.Contains(string(rendered), "Message-ID: <x1@example.com>") || !strings.Contains(string(rendered), "See attached") {
		t.Fatal("rendered message lost content")
	}
	// Search event enqueued.
	st, _ := f.q.Stats(ctx, TaskSearchIndex)
	if st.Pending != 1 {
		t.Fatalf("queue: %+v", st)
	}
	// Second delivery of the same attachment dedups: refs 2, one object.
	out2, _ := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(raw)})
	info, _ = f.atts.Stat(ctx, refs[0])
	if info.Refs != 2 || out2[0].UID != 2 {
		t.Fatalf("dedup: refs=%d uid=%d", info.Refs, out2[0].UID)
	}
	// Audit.
	recs, _, _ := f.aud.Read(ctx, audit.Query{Mailbox: f.mb.ID, From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour)})
	if len(recs) != 2 || recs[0].Action != "deliver" || recs[0].Protocol != "lmtp" || recs[0].ClientIP != "::1" {
		t.Fatalf("audit: %+v", recs)
	}
}

func b64lines(s string) string {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var out []byte
	b := []byte(s)
	for i := 0; i < len(b); i += 3 {
		var n uint32
		pad := 0
		for j := 0; j < 3; j++ {
			n <<= 8
			if i+j < len(b) {
				n |= uint32(b[i+j])
			} else {
				pad++
			}
		}
		for j := 3; j >= 0; j-- {
			if j < pad {
				out = append(out, '=')
			} else {
				out = append(out, tbl[(n>>(6*uint(j)))&63])
			}
		}
	}
	var lines []string
	for len(out) > 76 {
		lines = append(lines, string(out[:76]))
		out = out[76:]
	}
	lines = append(lines, string(out))
	return strings.Join(lines, "\r\n")
}

func TestSpamRoutingAndAdmission(t *testing.T) {
	f := newFixture(t, Config{SpamHeader: "X-Spam-Flag", SpamValue: "YES", DefaultQuota: identity.QuotaLimits{HardMessages: 2}})
	ctx := context.Background()
	spam := "X-Spam-Flag: yes\r\n" + simpleMsg
	out, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(spam)})
	if err != nil {
		t.Fatal(err)
	}
	root, _ := f.idx.GetRoot(ctx, f.mb.ID)
	if root.FolderByID(out[0].FolderID).Role != index.RoleJunk {
		t.Fatal("spam not routed to Junk")
	}
	// Quota: 2 messages max.
	if _, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)})
	if !errors.Is(err, ErrOverQuota) {
		t.Fatalf("quota: %v", err)
	}
	code, enh, _ := f.svc.Status(err)
	if code != 552 || enh != "5.2.2" {
		t.Fatalf("status: %d %s", code, enh)
	}
	f.svc.cfg.OverQuotaTempFail = true
	if code, _, _ := f.svc.Status(err); code != 452 {
		t.Fatal("tempfail config ignored")
	}
	// CheckRecipient reports the same.
	if _, err := f.svc.CheckRecipient(ctx, "wido@example.org", 10); !errors.Is(err, ErrOverQuota) {
		t.Fatalf("check: %v", err)
	}
	// Per-mailbox limit overrides default.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Quota.HardMessages = 10; return nil })
	if _, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); err != nil {
		t.Fatalf("after raising quota: %v", err)
	}
	// Suspended (soft): temporary failure so the MTA queues.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.State = identity.StateSuspended; return nil })
	err = nil
	if _, err = f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); !errors.Is(err, ErrMailboxSuspended) {
		t.Fatalf("suspended: %v", err)
	}
	if code, enh, _ := f.svc.Status(err); code != 450 || enh != "4.2.1" {
		t.Fatalf("suspended status: %d %s", code, enh)
	}
	if _, err := f.svc.CheckRecipient(ctx, "wido@example.org", 10); !errors.Is(err, ErrMailboxSuspended) {
		t.Fatal("check suspended")
	}
	// Disabled (hard): permanent failure.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.State = identity.StateDisabled; return nil })
	if _, err = f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); !errors.Is(err, ErrMailboxDisabled) {
		t.Fatalf("disabled: %v", err)
	}
	if code, enh, _ := f.svc.Status(err); code != 550 || enh != "5.2.1" {
		t.Fatalf("disabled status: %d %s", code, enh)
	}
	// Hold without delivery.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error {
		m.State = identity.StateActive
		m.Hold = &identity.Hold{Reason: "case", AcceptDelivery: false}
		return nil
	})
	if _, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); !errors.Is(err, ErrHold) {
		t.Fatalf("hold: %v", err)
	}
	if code, _, _ := f.svc.Status(ErrHold); code != 450 {
		t.Fatal("hold must be a temp failure")
	}
	// Hold with delivery accepted still delivers, but Import is refused.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Hold.AcceptDelivery = true; return nil })
	if _, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); err != nil {
		t.Fatalf("hold with delivery: %v", err)
	}
	root, _ = f.idx.GetRoot(ctx, f.mb.ID)
	if _, err := f.svc.Import(ctx, f.mb.ID, root.FolderByRole(index.RoleDrafts).ID, []byte(simpleMsg), []string{index.FlagDraft}, time.Time{}); !errors.Is(err, ErrHold) {
		t.Fatalf("import on hold: %v", err)
	}
	// Too large.
	f.svc.cfg.MaxMessageSize = 10
	if _, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(simpleMsg)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("too large: %v", err)
	}
}

func TestSignedMessagesKeepRaw(t *testing.T) {
	f := newFixture(t, Config{KeepRaw: "signed"})
	ctx := context.Background()
	signed := "From: a@example.com\r\nTo: wido@example.org\r\nSubject: signed\r\nContent-Type: multipart/signed; boundary=s; protocol=\"application/pgp-signature\"\r\n\r\n--s\r\nContent-Type: text/plain\r\n\r\nexact bytes matter\r\n--s\r\nContent-Type: application/pgp-signature\r\n\r\n-----BEGIN PGP SIGNATURE-----\r\nabc\r\n-----END PGP SIGNATURE-----\r\n--s--\r\n"
	out, err := f.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(signed)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.msgs.GetRaw(ctx, f.mb.ID, out[0].MessageID)
	if err != nil || string(raw) != signed {
		t.Fatalf("signed message raw not kept: %v", err)
	}
	f2 := newFixture(t, Config{KeepRaw: "never"})
	out, _ = f2.svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(signed)})
	if _, err := f2.msgs.GetRaw(ctx, f2.mb.ID, out[0].MessageID); err == nil {
		t.Fatal("raw kept with keep_raw=never")
	}
}

func TestImportAndFanout(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	root, _ := f.idx.EnsureRoot(ctx, f.mb.ID)
	drafts := root.FolderByRole(index.RoleDrafts).ID
	d, err := f.svc.Import(ctx, f.mb.ID, drafts, []byte(simpleMsg), []string{"\\Draft", "\\Seen"}, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || d.FolderID != drafts {
		t.Fatal(err)
	}
	rec, _ := f.idx.GetRecord(ctx, f.mb.ID, d.FolderID, d.UID)
	if !rec.HasFlag(index.FlagDraft) || !rec.HasFlag(index.FlagSeen) || rec.InternalDate.Year() != 2020 {
		t.Fatalf("import record: %+v", rec)
	}
	if _, err := f.svc.Import(ctx, f.mb.ID, "f999", []byte(simpleMsg), nil, time.Time{}); !errors.Is(err, ErrNoFolder) {
		t.Fatalf("bad folder: %v", err)
	}
	// Alias fanout stores one copy per mailbox, sharing the attachment object.
	mb2, _ := f.ids.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "all@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: f.mb.ID}, {MailboxID: mb2.ID}}})
	out, err := f.svc.Deliver(ctx, Request{Recipient: "all@example.org", Raw: []byte(simpleMsg)})
	if err != nil || len(out) != 2 || out[0].MailboxID == out[1].MailboxID || out[0].MessageID == out[1].MessageID {
		t.Fatalf("fanout: %+v %v", out, err)
	}
	// Unknown mailbox in a destination is skipped at resolve but admission fails.
	_, _ = f.ids.CreateAlias(ctx, identity.Alias{Address: "ghost@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb2.ID}}})
	_ = f.ids.DeleteMailbox(ctx, mb2.ID)
	if _, err := f.svc.Deliver(ctx, Request{Recipient: "ghost@example.org", Raw: []byte(simpleMsg)}); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("ghost: %v", err)
	}
	_ = uuid.Nil
}
