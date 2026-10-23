package imap

import (
	"context"
	"io"
	"kittiwake/mail"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/emersion/go-sasl"
	"github.com/google/uuid"

	cryptoauth "kittiwake/auth"
	"kittiwake/rados"
	"kittiwake/service/auditor"
	authsvc "kittiwake/service/auth"
	"kittiwake/service/delivery"
	"kittiwake/service/mailbox"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
	sessstore "kittiwake/store/session"
)

type fixture struct {
	attachmentReads func() int
	t               *testing.T
	addr            string
	ids             *identity.Store
	idx             *index.Store
	auth            *authsvc.Service
	del             *delivery.Service
	mb              identity.Mailbox
	acct            identity.Account
}

const (
	user    = "wido@example.org"
	pass    = "correct horse battery"
	appPass = "app-password-1234"
)

func newFixture(t *testing.T) *fixture { return newFixtureCfg(t, Config{Hostname: "test"}) }

func newFixtureCfg(t *testing.T, cfg Config) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{t: t, ids: identity.New(meta), idx: index.New(idxp), attachmentReads: attp.(interface{ Reads() int }).Reads}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(audit.New(idxp), log, nil)
	msgs, atts, q := message.New(msgp), attach.New(attp), queue.New(idxp)
	f.auth = authsvc.New(f.ids, sessstore.New(idxp), aud, authsvc.Config{Issuer: "test"})
	f.del = delivery.New(f.ids, f.idx, msgs, atts, q, aud, log, delivery.Config{})
	mbsvc := mailbox.New(f.ids, f.idx, msgs, atts, q, aud, log, mailbox.Config{})
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	h, _ := cryptoauth.HashPassword(pass)
	ah, _ := cryptoauth.HashPassword(appPass)
	f.acct, _ = f.ids.CreateAccount(ctx, identity.Account{
		Username: user, MailboxID: f.mb.ID, Enabled: true, Roles: []string{"read", "write", "send"},
		Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordPrimary}, {Hash: ah, Kind: identity.PasswordApplication}},
	})
	srv := New(cfg, f.auth, mbsvc, f.del, log)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	f.addr = l.Addr().String()
	return f
}

func (f *fixture) deliver(raw string) delivery.Delivered {
	f.t.Helper()
	out, err := f.del.Deliver(context.Background(), delivery.Request{Recipient: user, Raw: []byte(raw)})
	if err != nil {
		f.t.Fatal(err)
	}
	return out[0]
}

func (f *fixture) client(opts *imapclient.Options) *imapclient.Client {
	f.t.Helper()
	c, err := imapclient.DialInsecure(f.addr, opts)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = c.Close() })
	return c
}

func (f *fixture) login() *imapclient.Client {
	f.t.Helper()
	c := f.client(nil)
	if err := c.Login(user, pass).Wait(); err != nil {
		f.t.Fatal(err)
	}
	return c
}

func simple(subject string) string {
	return "From: Alice <alice@example.com>\r\nTo: " + user + "\r\nSubject: " + subject + "\r\nDate: Tue, 21 Oct 2026 10:00:00 +0000\r\nMessage-ID: <" + uuid.NewString() + "@x>\r\n\r\nbody of " + subject + "\r\n"
}

const multipart = "From: Bob <bob@example.com>\r\nTo: wido@example.org\r\nSubject: =?utf-8?q?R=C3=A9sum=C3=A9?=\r\nMessage-ID: <mp1@x>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"XYZ\"\r\n\r\n--XYZ\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nSee attachment.\r\n--XYZ\r\nContent-Type: application/pdf; name=\"doc.pdf\"\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=\"doc.pdf\"\r\n\r\nJVBERi0xLjQKJcOkw7zDtsOfCjIgMCBvYmoKPDwvTGVuZ3RoIDMgMCBSL0ZpbHRlci9GbGF0ZURl\r\nY29kZT4+CnN0cmVhbQp4nDPQM1Qo5ypUMFAwALJMLU31jBQsTAz0jE0UQi1NLCyNjU0UwpJyFOIV\r\n--XYZ--\r\n"

func TestLogin(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	if !c.Caps().Has(imap.AuthCap("OAUTHBEARER")) || !c.Caps().Has(imap.AuthCap("PLAIN")) || c.Caps().Has(imap.CapLoginDisabled) {
		t.Fatalf("pre-auth caps: %v", c.Caps())
	}
	if err := c.Login(user, "wrong").Wait(); err == nil {
		t.Fatal("bad password accepted")
	}
	if err := c.Login(user, pass).Wait(); err != nil {
		t.Fatal(err)
	}
	if !c.Caps().Has(imap.CapIdle) || !c.Caps().Has(imap.CapUIDPlus) || !c.Caps().Has(imap.CapMove) || !c.Caps().Has(imap.CapSpecialUse) || !c.Caps().Has(imap.CapNamespace) {
		t.Fatalf("caps: %v", c.Caps())
	}
	// TOTP-enabled account: primary refused, application password accepted.
	_, _ = f.ids.UpdateAccount(context.Background(), f.acct.ID, func(a *identity.Account) error {
		a.TOTP = &identity.TOTP{Secret: "JBSWY3DPEHPK3PXP", Digits: 6, Period: 30, Algorithm: "SHA1", Confirmed: true}
		return nil
	})
	c2 := f.client(nil)
	err := c2.Login(user, pass).Wait()
	if err == nil || !strings.Contains(err.Error(), "application password") {
		t.Fatalf("totp primary: %v", err)
	}
	c3 := f.client(nil)
	if err := c3.Login(user, appPass).Wait(); err != nil {
		t.Fatalf("app password: %v", err)
	}
	// SASL PLAIN.
	c4 := f.client(nil)
	if err := c4.Authenticate(sasl.NewPlainClient("", user, appPass)); err != nil {
		t.Fatalf("plain: %v", err)
	}
}

func TestOAuthBearer(t *testing.T) {
	f := newFixture(t)
	toks, err := f.auth.Login(context.Background(), authsvc.LoginRequest{Username: user, Password: pass, Client: "test"})
	if err != nil {
		t.Fatal(err)
	}
	c := f.client(nil)
	if err := c.Authenticate(sasl.NewOAuthBearerClient(&sasl.OAuthBearerOptions{Username: user, Token: toks.AccessToken})); err != nil {
		t.Fatalf("oauthbearer: %v", err)
	}
	if _, err := c.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	c2 := f.client(nil)
	if err := c2.Authenticate(sasl.NewOAuthBearerClient(&sasl.OAuthBearerOptions{Username: user, Token: "garbage"})); err == nil {
		t.Fatal("bad token accepted")
	}
}

func TestListStatusAndFolderOps(t *testing.T) {
	f := newFixture(t)
	c := f.login()
	list, err := c.List("", "*", &imap.ListOptions{ReturnSpecialUse: true}).Collect()
	if err != nil || len(list) != 6 {
		t.Fatalf("list: %d %v", len(list), err)
	}
	attrs := map[string]imap.MailboxAttr{}
	for _, l := range list {
		for _, a := range l.Attrs {
			if a != imap.MailboxAttrHasChildren && a != imap.MailboxAttrHasNoChildren {
				attrs[l.Mailbox] = a
			}
		}
		if l.Delim != '/' {
			t.Fatal("delim")
		}
	}
	if attrs["Trash"] != imap.MailboxAttrTrash || attrs["Sent"] != imap.MailboxAttrSent || attrs["Junk"] != imap.MailboxAttrJunk {
		t.Fatalf("special-use: %v", attrs)
	}
	if err := c.Create("Projects/2026", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := c.Create("Projects", nil).Wait(); err == nil {
		t.Fatal("duplicate create accepted")
	}
	list, _ = c.List("", "Projects/%", nil).Collect()
	if len(list) != 1 || list[0].Mailbox != "Projects/2026" {
		t.Fatalf("wildcard list: %+v", list)
	}
	if err := c.Rename("Projects/2026", "Projects/2025", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := c.Unsubscribe("Projects/2025").Wait(); err != nil {
		t.Fatal(err)
	}
	list, _ = c.List("", "*", &imap.ListOptions{SelectSubscribed: true}).Collect()
	for _, l := range list {
		if l.Mailbox == "Projects/2025" {
			t.Fatal("unsubscribed folder listed by LSUB")
		}
	}
	if err := c.Subscribe("Projects/2025").Wait(); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("Projects").Wait(); err == nil {
		t.Fatal("delete with children accepted")
	}
	if err := c.Delete("Projects/2025").Wait(); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("Projects").Wait(); err != nil {
		t.Fatal(err)
	}
	f.deliver(simple("one"))
	f.deliver(simple("two"))
	st, err := c.Status("INBOX", &imap.StatusOptions{NumMessages: true, NumUnseen: true, UIDNext: true, UIDValidity: true}).Wait()
	if err != nil || *st.NumMessages != 2 || *st.NumUnseen != 2 || st.UIDNext != 3 || st.UIDValidity == 0 {
		t.Fatalf("status: %+v %v", st, err)
	}
	ns, err := c.Namespace().Wait()
	if err != nil || len(ns.Personal) != 1 || ns.Personal[0].Delim != '/' {
		t.Fatalf("namespace: %+v %v", ns, err)
	}
}

func TestSelectFetchStoreSearch(t *testing.T) {
	f := newFixture(t)
	f.deliver(simple("first"))
	f.deliver(multipart)
	c := f.login()
	sel, err := c.Select("INBOX", nil).Wait()
	if err != nil || sel.NumMessages != 2 || sel.UIDNext != 3 {
		t.Fatalf("select: %+v %v", sel, err)
	}
	if len(sel.PermanentFlags) == 0 || sel.PermanentFlags[len(sel.PermanentFlags)-1] != imap.FlagWildcard {
		t.Fatalf("permanent flags: %v", sel.PermanentFlags)
	}
	// FETCH everything on the multipart message (seq 2).
	msgs, err := c.Fetch(imap.SeqSetNum(2), &imap.FetchOptions{
		UID: true, Flags: true, Envelope: true, InternalDate: true, RFC822Size: true, BodyStructure: &imap.FetchItemBodyStructure{Extended: true},
		BodySection: []*imap.FetchItemBodySection{
			{Peek: true},
			{Specifier: imap.PartSpecifierHeader, HeaderFields: []string{"Subject"}, Peek: true},
			{Part: []int{1}, Peek: true},
		},
	}).Collect()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch: %v", err)
	}
	m := msgs[0]
	if m.UID != 2 || m.RFC822Size != int64(len(rendered(t, multipart))) || m.Envelope.Subject != "Résumé" || m.Envelope.From[0].Mailbox != "bob" || m.Envelope.MessageID != "mp1@x" {
		t.Fatalf("items: uid=%d size=%d env=%+v", m.UID, m.RFC822Size, m.Envelope)
	}
	mp, ok := m.BodyStructure.(*imap.BodyStructureMultiPart)
	if !ok || len(mp.Children) != 2 || mp.Children[1].MediaType() != "application/pdf" {
		t.Fatalf("bodystructure: %+v", m.BodyStructure)
	}
	if string(m.FindBodySection(&imap.FetchItemBodySection{Peek: true})) != rendered(t, multipart) {
		t.Fatal("BODY[] is not the rendered form")
	}
	hdr := string(m.FindBodySection(&imap.FetchItemBodySection{Specifier: imap.PartSpecifierHeader, HeaderFields: []string{"Subject"}, Peek: true}))
	if !strings.HasPrefix(hdr, "Subject: =?utf-8?q?R=C3=A9sum=C3=A9?=") {
		t.Fatalf("header fields: %q", hdr)
	}
	if string(m.FindBodySection(&imap.FetchItemBodySection{Part: []int{1}, Peek: true})) != "See attachment." {
		t.Fatalf("BODY[1]: %q", m.FindBodySection(&imap.FetchItemBodySection{Part: []int{1}, Peek: true}))
	}
	if len(m.Flags) != 0 {
		t.Fatalf("peek set flags: %v", m.Flags)
	}
	// Non-peek BODY[] marks \Seen.
	msgs, _ = c.Fetch(imap.UIDSetNum(1), &imap.FetchOptions{BodySection: []*imap.FetchItemBodySection{{Specifier: imap.PartSpecifierText}}}).Collect()
	if len(msgs) != 1 || !hasFlag(msgs[0].Flags, imap.FlagSeen) {
		t.Fatalf("non-peek should set seen: %+v", msgs[0].Flags)
	}
	// STORE.
	msgs, err = c.Store(imap.SeqSetNum(2), &imap.StoreFlags{Op: imap.StoreFlagsAdd, Flags: []imap.Flag{imap.FlagFlagged, "$Label1"}}, nil).Collect()
	if err != nil || len(msgs) != 1 || !hasFlag(msgs[0].Flags, imap.FlagFlagged) || !hasFlag(msgs[0].Flags, "$Label1") {
		t.Fatalf("store: %+v %v", msgs, err)
	}
	msgs, _ = c.Store(imap.SeqSetNum(2), &imap.StoreFlags{Op: imap.StoreFlagsDel, Flags: []imap.Flag{"$Label1"}}, nil).Collect()
	if hasFlag(msgs[0].Flags, "$Label1") {
		t.Fatal("flag not removed")
	}
	// SEARCH.
	sd, err := c.UIDSearch(&imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "Subject", Value: "résumé"}}}, nil).Wait()
	if err != nil || len(sd.AllUIDs()) != 1 || sd.AllUIDs()[0] != 2 {
		t.Fatalf("search subject: %v %v", sd.AllUIDs(), err)
	}
	sd, _ = c.Search(&imap.SearchCriteria{Flag: []imap.Flag{imap.FlagFlagged}}, nil).Wait()
	if nums := sd.AllSeqNums(); len(nums) != 1 || nums[0] != 2 {
		t.Fatalf("search flagged: %v", nums)
	}
	sd, _ = c.Search(&imap.SearchCriteria{NotFlag: []imap.Flag{imap.FlagSeen}}, nil).Wait()
	if nums := sd.AllSeqNums(); len(nums) != 1 || nums[0] != 2 {
		t.Fatalf("search unseen: %v", nums)
	}
	sd, _ = c.UIDSearch(&imap.SearchCriteria{UID: []imap.UIDSet{imap.UIDSetNum(1)}, Body: []string{"body of first"}}, nil).Wait()
	if uids := sd.AllUIDs(); len(uids) != 1 || uids[0] != 1 {
		t.Fatalf("search body+uid: %v", uids)
	}
	sd, _ = c.UIDSearch(&imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "To", Value: "wido"}}, Larger: 10}, nil).Wait()
	if len(sd.AllUIDs()) != 2 {
		t.Fatalf("search To header: %v", sd.AllUIDs())
	}
	sd, _ = c.UIDSearch(&imap.SearchCriteria{Not: []imap.SearchCriteria{{Flag: []imap.Flag{imap.FlagSeen}}}}, nil).Wait()
	if len(sd.AllUIDs()) != 1 {
		t.Fatalf("search NOT: %v", sd.AllUIDs())
	}
}

func hasFlag(flags []imap.Flag, f imap.Flag) bool {
	for _, x := range flags {
		if strings.EqualFold(string(x), string(f)) {
			return true
		}
	}
	return false
}

func TestCopyMoveExpungeAppend(t *testing.T) {
	f := newFixture(t)
	f.deliver(simple("a"))
	f.deliver(simple("b"))
	f.deliver(simple("c"))
	c := f.login()
	if _, err := c.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	// COPY to Archive.
	cd, err := c.Copy(imap.UIDSetNum(1), "Archive").Wait()
	if err != nil || cd.DestUIDs.String() != "1" || cd.SourceUIDs.String() != "1" || cd.UIDValidity == 0 {
		t.Fatalf("copy: %+v %v", cd, err)
	}
	st, _ := c.Status("Archive", &imap.StatusOptions{NumMessages: true}).Wait()
	if *st.NumMessages != 1 {
		t.Fatal("copy did not land")
	}
	if _, err := c.Copy(imap.UIDSetNum(1), "Nope").Wait(); err == nil {
		t.Fatal("copy to missing mailbox")
	}
	// MOVE seq 2 (uid 2) to Trash: expect COPYUID and EXPUNGE 2.
	var expunged []uint32
	c2 := f.client(&imapclient.Options{UnilateralDataHandler: &imapclient.UnilateralDataHandler{Expunge: func(seq uint32) { expunged = append(expunged, seq) }}})
	if err := c2.Login(user, pass).Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	md, err := c2.Move(imap.SeqSetNum(2), "Trash").Wait()
	if err != nil || md.DestUIDs.String() != "1" || md.SourceUIDs.String() != "2" {
		t.Fatalf("move: %+v %v", md, err)
	}
	if len(expunged) != 1 || expunged[0] != 2 {
		t.Fatalf("move expunge: %v", expunged)
	}
	// After the move, seq 2 is uid 3.
	msgs, _ := c2.Fetch(imap.SeqSetNum(2), &imap.FetchOptions{UID: true}).Collect()
	if len(msgs) != 1 || msgs[0].UID != 3 {
		t.Fatalf("seq after move: %+v", msgs)
	}
	// EXPUNGE after \Deleted on uid 1.
	if _, err := c2.Store(imap.UIDSetNum(1), &imap.StoreFlags{Op: imap.StoreFlagsAdd, Flags: []imap.Flag{imap.FlagDeleted}, Silent: true}, nil).Collect(); err != nil {
		t.Fatal(err)
	}
	seqs, err := c2.Expunge().Collect()
	if err != nil || len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("expunge: %v %v", seqs, err)
	}
	sel, _ := c2.Select("INBOX", nil).Wait()
	if sel.NumMessages != 1 {
		t.Fatalf("after expunge: %d", sel.NumMessages)
	}
	// APPEND to Drafts with flags.
	draft := simple("draft")
	ac := c2.Append("Drafts", int64(len(draft)), &imap.AppendOptions{Flags: []imap.Flag{imap.FlagDraft, imap.FlagSeen}, Time: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)})
	_, _ = ac.Write([]byte(draft))
	_ = ac.Close()
	ad, err := ac.Wait()
	if err != nil || ad.UID != 1 || ad.UIDValidity == 0 {
		t.Fatalf("append: %+v %v", ad, err)
	}
	if _, err := c2.Select("Drafts", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	msgs, _ = c2.Fetch(imap.UIDSetNum(1), &imap.FetchOptions{Flags: true, InternalDate: true, BodySection: []*imap.FetchItemBodySection{{Peek: true}}}).Collect()
	if len(msgs) != 1 || !hasFlag(msgs[0].Flags, imap.FlagDraft) || msgs[0].InternalDate.Year() != 2020 || string(msgs[0].FindBodySection(&imap.FetchItemBodySection{Peek: true})) != rendered(t, draft) {
		t.Fatalf("appended message: %+v", msgs[0])
	}
	// UID EXPUNGE only the given UIDs.
	f.deliver(simple("x"))
	f.deliver(simple("y"))
	if _, err := c2.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	_, _ = c2.Store(imap.SeqSetNum(1, 2, 3), &imap.StoreFlags{Op: imap.StoreFlagsAdd, Flags: []imap.Flag{imap.FlagDeleted}, Silent: true}, nil).Collect()
	seqs, _ = c2.UIDExpunge(imap.UIDSetNum(4)).Collect()
	if len(seqs) != 1 {
		t.Fatalf("uid expunge: %v", seqs)
	}
}

func TestIdleAndPoll(t *testing.T) {
	f := newFixture(t)
	f.deliver(simple("a"))
	var mu sync.Mutex
	var exists []uint32
	got := make(chan struct{}, 4)
	c := f.client(&imapclient.Options{UnilateralDataHandler: &imapclient.UnilateralDataHandler{Mailbox: func(d *imapclient.UnilateralDataMailbox) {
		if d.NumMessages != nil {
			mu.Lock()
			exists = append(exists, *d.NumMessages)
			mu.Unlock()
			got <- struct{}{}
		}
	}}})
	if err := c.Login(user, pass).Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	idle, err := c.Idle()
	if err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(100 * time.Millisecond); f.deliver(simple("b")) }()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no EXISTS during IDLE")
	}
	if err := idle.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	last := exists[len(exists)-1]
	mu.Unlock()
	if last != 2 {
		t.Fatalf("EXISTS %d", last)
	}
	// NOOP polling also picks up a delivery.
	f.deliver(simple("c"))
	if err := c.Noop().Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("no EXISTS after NOOP")
	}
}

func TestLegalHold(t *testing.T) {
	f := newFixture(t)
	d := f.deliver(simple("evidence"))
	_, _ = f.ids.UpdateMailbox(context.Background(), f.mb.ID, func(m *identity.Mailbox) error {
		m.Hold = &identity.Hold{Reason: "case", AcceptDelivery: true}
		return nil
	})
	c := f.login()
	if _, err := c.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	msgs, err := c.Fetch(imap.UIDSetNum(imap.UID(d.UID)), &imap.FetchOptions{Envelope: true}).Collect()
	if err != nil || len(msgs) != 1 || msgs[0].Envelope.Subject != "evidence" {
		t.Fatalf("read on hold: %v", err)
	}
	_, err = c.Store(imap.UIDSetNum(1), &imap.StoreFlags{Op: imap.StoreFlagsAdd, Flags: []imap.Flag{imap.FlagSeen}}, nil).Collect()
	if err == nil || !strings.Contains(err.Error(), "hold") {
		t.Fatalf("store on hold: %v", err)
	}
	if err := c.Create("New", nil).Wait(); err == nil {
		t.Fatal("create on hold accepted")
	}
	// Sealed: login refused.
	_, _ = f.ids.UpdateMailbox(context.Background(), f.mb.ID, func(m *identity.Mailbox) error { m.Hold.Sealed = true; return nil })
	c2 := f.client(nil)
	if err := c2.Login(user, pass).Wait(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "sealed") {
		t.Fatalf("sealed login: %v", err)
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

// Old clients configured for a Dovecot/Courier layout ("INBOX." prefix,
// "." delimiter) keep working through the compatibility namespace, which
// maps onto the same folders.
func TestCompatNamespace(t *testing.T) {
	f := newFixtureCfg(t, Config{Hostname: "test", CompatPrefix: "INBOX.", CompatDelim: "."})
	c := f.login()
	ns, err := c.Namespace().Wait()
	if err != nil || len(ns.Personal) != 2 || ns.Personal[1].Prefix != "INBOX." || ns.Personal[1].Delim != '.' {
		t.Fatalf("namespace: %+v %v", ns, err)
	}
	// Create through the old layout, see it in the canonical one.
	if err := c.Create("INBOX.Projects.2026", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	list, _ := c.List("", "Projects/*", nil).Collect()
	if len(list) != 1 || list[0].Mailbox != "Projects/2026" || list[0].Delim != '/' {
		t.Fatalf("canonical view: %+v", list)
	}
	// The default view does not show the compat names ...
	list, _ = c.List("", "*", nil).Collect()
	for _, l := range list {
		if strings.HasPrefix(l.Mailbox, "INBOX.") {
			t.Fatalf("compat name leaked into the default view: %s", l.Mailbox)
		}
	}
	// ... but a pattern under the prefix does, with the compat delimiter.
	list, _ = c.List("", "INBOX.*", nil).Collect()
	names := map[string]rune{}
	for _, l := range list {
		names[l.Mailbox] = l.Delim
	}
	if names["INBOX.Projects.2026"] != '.' || names["INBOX.Sent"] != '.' || names["INBOX.Projects"] != '.' {
		t.Fatalf("compat view: %v", names)
	}
	list, _ = c.List("INBOX.", "%", nil).Collect()
	if len(list) == 0 {
		t.Fatal("reference form of the compat listing")
	}
	// SELECT, STATUS, APPEND and subscriptions by compat name.
	if _, err := c.Select("INBOX.Projects.2026", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	st, err := c.Status("INBOX.Sent", &imap.StatusOptions{NumMessages: true}).Wait()
	if err != nil || st.Mailbox != "INBOX.Sent" {
		t.Fatalf("status: %+v %v", st, err)
	}
	if err := c.Unsubscribe("INBOX.Projects.2026").Wait(); err != nil {
		t.Fatal(err)
	}
	list, _ = c.List("", "INBOX.*", &imap.ListOptions{SelectSubscribed: true}).Collect()
	for _, l := range list {
		if l.Mailbox == "INBOX.Projects.2026" {
			t.Fatal("unsubscribe through the compat name did not apply")
		}
	}
	// INBOX itself is INBOX in both views; "INBOX." alone is INBOX too.
	if _, err := c.Select("INBOX.", nil).Wait(); err != nil {
		t.Fatalf("select INBOX.: %v", err)
	}
	// A folder whose name contains the compat delimiter round-trips escaped.
	if err := c.Create("Release 1.0", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	list, _ = c.List("", "INBOX.Release*", nil).Collect()
	if len(list) != 1 || list[0].Mailbox != "INBOX.Release 1\\2e0" {
		t.Fatalf("escaped compat name: %q", list[0].Mailbox)
	}
	if _, err := c.Select("INBOX.Release 1\\2e0", nil).Wait(); err != nil {
		t.Fatalf("select escaped compat name: %v", err)
	}
	// Rename within the compat view.
	if err := c.Rename("INBOX.Projects.2026", "INBOX.Archive.2026", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	list, _ = c.List("", "Archive/*", nil).Collect()
	if len(list) != 1 || list[0].Mailbox != "Archive/2026" {
		t.Fatalf("after rename: %+v", list)
	}
}

// A "/" inside a folder name is a character, stored escaped the way
// Dovecot's listescape does it, so a migrated "A\2fB Ltd" is one folder.
func TestEscapedDelimiterInName(t *testing.T) {
	f := newFixture(t)
	c := f.login()
	if err := c.Create("Clients/A\\2fB Ltd", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	list, _ := c.List("", "Clients/*", nil).Collect()
	if len(list) != 1 || list[0].Mailbox != "Clients/A\\2fB Ltd" {
		t.Fatalf("list: %+v", list)
	}
	if _, err := c.Select("Clients/A\\2fB Ltd", nil).Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataFetchDoesNotReadAttachmentPayload(t *testing.T) {
	f := newFixture(t)
	raw := "From: a@example.org\r\nSubject: attachment\r\nContent-Type: application/octet-stream\r\n\r\n" + strings.Repeat("payload", 20000)
	f.deliver(raw)
	c := f.client(nil)
	defer c.Close()
	if err := c.Login(user, pass).Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Select("INBOX", nil).Wait(); err != nil {
		t.Fatal(err)
	}
	before := f.attachmentReads()
	_, err := c.Fetch(imap.SeqSetNum(1), &imap.FetchOptions{Envelope: true, BodyStructure: &imap.FetchItemBodyStructure{Extended: true}, BodySection: []*imap.FetchItemBodySection{{Specifier: imap.PartSpecifierHeader, Peek: true}}}).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if after := f.attachmentReads(); after != before {
		t.Fatalf("metadata read attachment payload: %d extra reads", after-before)
	}
}
