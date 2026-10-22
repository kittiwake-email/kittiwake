package pop3

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"kittiwake/mail"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

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
	sessionstore "kittiwake/store/session"
)

type fixture struct {
	addr string
	ids  *identity.Store
	idx  *index.Store
	auth *authsvc.Service
	mb   identity.Mailbox
	acct identity.Account
	raws []string
}

const msg1 = "From: a@example.com\r\nTo: wido@example.org\r\nSubject: one\r\n\r\nline1\r\n.leading dot\r\nline3\r\n"
const msg2 = "From: b@example.com\r\nTo: wido@example.org\r\nSubject: two\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nbody\r\n--B\r\nContent-Type: application/pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5\r\n--B--\r\n"

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{ids: identity.New(meta), idx: index.New(idxp)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(audit.New(idxp), log, nil)
	msgs, atts, q := message.New(msgp), attach.New(attp), queue.New(idxp)
	del := delivery.New(f.ids, f.idx, msgs, atts, q, aud, log, delivery.Config{InlineMax: 16})
	mbx := mailbox.New(f.ids, f.idx, msgs, atts, q, aud, log, mailbox.Config{})
	f.auth = authsvc.New(f.ids, sessionstore.New(idxp), aud, authsvc.Config{})
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	h, _ := cryptoauth.HashPassword("correct horse")
	apph, _ := cryptoauth.HashPassword("app-pass-1234")
	f.acct, _ = f.ids.CreateAccount(ctx, identity.Account{
		Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "write"}, Enabled: true,
		Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordPrimary}, {Hash: apph, Kind: identity.PasswordApplication}},
	})
	for _, raw := range []string{msg1, msg2} {
		if _, err := del.Deliver(ctx, delivery.Request{Recipient: "wido@example.org", Raw: []byte(raw)}); err != nil {
			t.Fatal(err)
		}
		f.raws = append(f.raws, raw)
	}
	srv := New(Config{Hostname: "test"}, f.auth, mbx, log)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	f.addr = l.Addr().String()
	return f
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func (f *fixture) dial(t *testing.T) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	c := &client{t: t, conn: conn, r: bufio.NewReader(conn)}
	if l := c.line(); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("greeting: %q", l)
	}
	return c
}

func (c *client) line() string {
	l, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return strings.TrimRight(l, "\r\n")
}

// cmd sends a command and returns the status line.
func (c *client) cmd(s string) string {
	if _, err := c.conn.Write([]byte(s + "\r\n")); err != nil {
		c.t.Fatal(err)
	}
	return c.line()
}

// multi reads a dot-terminated multi-line body (un-stuffed), raw CRLF joined.
func (c *client) multi() []string {
	var out []string
	for {
		l := c.line()
		if l == "." {
			return out
		}
		out = append(out, strings.TrimPrefix(l, "."))
	}
}

func (c *client) login(user, pass string) string {
	if l := c.cmd("USER " + user); !strings.HasPrefix(l, "+OK") {
		return l
	}
	return c.cmd("PASS " + pass)
}

func (c *client) close() { _ = c.conn.Close() }

func TestCapaAndAuth(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	defer c.close()
	if l := c.cmd("CAPA"); !strings.HasPrefix(l, "+OK") {
		t.Fatal(l)
	}
	caps := strings.Join(c.multi(), "\n")
	for _, want := range []string{"UIDL", "TOP", "SASL PLAIN OAUTHBEARER XOAUTH2", "USER"} {
		if !strings.Contains(caps, want) {
			t.Fatalf("missing capability %s in %q", want, caps)
		}
	}
	if l := c.cmd("STAT"); !strings.HasPrefix(l, "-ERR") {
		t.Fatalf("STAT before auth: %s", l)
	}
	if l := c.login("wido@example.org", "wrong"); !strings.HasPrefix(l, "-ERR [AUTH]") {
		t.Fatalf("wrong password: %s", l)
	}
	if l := c.login("wido@example.org", "correct horse"); !strings.HasPrefix(l, "+OK 2 messages") {
		t.Fatalf("login: %s", l)
	}
	if l := c.cmd("USER x"); !strings.HasPrefix(l, "-ERR") {
		t.Fatal("USER after auth")
	}
	if l := c.cmd("QUIT"); l != "+OK bye" {
		t.Fatalf("quit: %s", l)
	}
	// AUTH PLAIN with initial response.
	c2 := f.dial(t)
	defer c2.close()
	ir := base64.StdEncoding.EncodeToString([]byte("\x00wido@example.org\x00correct horse"))
	if l := c2.cmd("AUTH PLAIN " + ir); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("AUTH PLAIN: %s", l)
	}
	// AUTH PLAIN with challenge round trip.
	c3 := f.dial(t)
	defer c3.close()
	if l := c3.cmd("AUTH PLAIN"); l != "+ " {
		t.Fatalf("challenge: %q", l)
	}
	if l := c3.cmd(ir); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("AUTH PLAIN response: %s", l)
	}
	// OAUTHBEARER with a session token.
	toks, err := f.auth.Login(context.Background(), authsvc.LoginRequest{Username: "wido@example.org", Password: "correct horse"})
	if err != nil {
		t.Fatal(err)
	}
	c4 := f.dial(t)
	defer c4.close()
	ob := base64.StdEncoding.EncodeToString([]byte("n,a=wido@example.org,\x01auth=Bearer " + toks.AccessToken + "\x01\x01"))
	if l := c4.cmd("AUTH OAUTHBEARER " + ob); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("OAUTHBEARER: %s", l)
	}
	c5 := f.dial(t)
	defer c5.close()
	xo := base64.StdEncoding.EncodeToString([]byte("user=wido@example.org\x01auth=Bearer " + toks.AccessToken + "\x01\x01"))
	if l := c5.cmd("AUTH XOAUTH2 " + xo); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("XOAUTH2: %s", l)
	}
}

func TestListingsRetrTop(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	defer c.close()
	c.login("wido@example.org", "correct horse")
	want := len(rendered(t, msg1)) + len(rendered(t, msg2))
	if l := c.cmd("STAT"); l != "+OK 2 "+itoa(want) {
		t.Fatalf("STAT: %s", l)
	}
	if l := c.cmd("LIST"); !strings.HasPrefix(l, "+OK") {
		t.Fatal(l)
	}
	lines := c.multi()
	if len(lines) != 2 || lines[0] != "1 "+itoa(len(rendered(t, msg1))) || lines[1] != "2 "+itoa(len(rendered(t, msg2))) {
		t.Fatalf("LIST: %v", lines)
	}
	if l := c.cmd("LIST 2"); l != "+OK 2 "+itoa(len(rendered(t, msg2))) {
		t.Fatalf("LIST 2: %s", l)
	}
	if l := c.cmd("LIST 3"); !strings.HasPrefix(l, "-ERR") {
		t.Fatal("LIST 3")
	}
	c.cmd("UIDL")
	uidls := c.multi()
	if len(uidls) != 2 || !strings.HasPrefix(uidls[0], "1 ") || uidls[0] == uidls[1] {
		t.Fatalf("UIDL: %v", uidls)
	}
	// RETR byte-exact after un-stuffing.
	if l := c.cmd("RETR 1"); l != "+OK "+itoa(len(rendered(t, msg1)))+" octets" {
		t.Fatalf("RETR: %s", l)
	}
	got := strings.Join(c.multi(), "\r\n") + "\r\n"
	if got != rendered(t, msg1) {
		t.Fatalf("RETR mismatch:\n%q\n%q", got, msg1)
	}
	c.cmd("RETR 2")
	if got := strings.Join(c.multi(), "\r\n") + "\r\n"; got != rendered(t, msg2) {
		t.Fatal("RETR 2 mismatch")
	}
	// TOP 1 0 is headers only; TOP 1 1 adds one body line.
	c.cmd("TOP 1 0")
	top := c.multi()
	// Rendered messages carry MIME-Version, Content-Type and
	// Content-Transfer-Encoding after the original headers.
	if len(top) != 7 || top[6] != "" || top[0] != "From: a@example.com" {
		t.Fatalf("TOP 1 0: %v", top)
	}
	c.cmd("TOP 1 1")
	if top = c.multi(); len(top) != 8 || top[7] != "line1" {
		t.Fatalf("TOP 1 1: %v", top)
	}
	if l := c.cmd("TOP 1"); !strings.HasPrefix(l, "-ERR") {
		t.Fatal("TOP syntax")
	}
	if l := c.cmd("BOGUS"); !strings.HasPrefix(l, "-ERR") {
		t.Fatal("unknown command")
	}
	// UIDLs are stable across sessions.
	c2 := f.dial(t)
	defer c2.close()
	c2.login("wido@example.org", "correct horse")
	c2.cmd("UIDL")
	if u2 := c2.multi(); u2[0] != uidls[0] || u2[1] != uidls[1] {
		t.Fatalf("UIDL unstable: %v vs %v", uidls, u2)
	}
}

func TestDeleteRsetQuit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	c := f.dial(t)
	c.login("wido@example.org", "correct horse")
	if l := c.cmd("DELE 1"); !strings.HasPrefix(l, "+OK") {
		t.Fatal(l)
	}
	if l := c.cmd("RETR 1"); !strings.HasPrefix(l, "-ERR") {
		t.Fatal("RETR deleted")
	}
	if l := c.cmd("STAT"); l != "+OK 1 "+itoa(len(rendered(t, msg2))) {
		t.Fatalf("STAT after DELE: %s", l)
	}
	c.cmd("RSET")
	if l := c.cmd("STAT"); l != "+OK 2 "+itoa(len(rendered(t, msg1))+len(rendered(t, msg2))) {
		t.Fatalf("STAT after RSET: %s", l)
	}
	c.cmd("DELE 1")
	if l := c.cmd("QUIT"); l != "+OK bye, 1 messages deleted" {
		t.Fatalf("QUIT: %s", l)
	}
	c.close()
	c2 := f.dial(t)
	defer c2.close()
	if l := c2.login("wido@example.org", "correct horse"); l != "+OK 1 messages" {
		t.Fatalf("after delete: %s", l)
	}
	c2.cmd("RETR 1")
	if got := strings.Join(c2.multi(), "\r\n") + "\r\n"; got != rendered(t, msg2) {
		t.Fatal("remaining message is not msg2")
	}
	ents, _, _, _ := f.idx.ListRetention(ctx, f.mb.ID, time.Time{}, 10)
	if len(ents) != 1 || ents[0].Reason != "expunge" {
		t.Fatalf("retention: %+v", ents)
	}
}

func TestTOTPAndHold(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_, _ = f.ids.UpdateAccount(ctx, f.acct.ID, func(a *identity.Account) error {
		a.TOTP = &identity.TOTP{Secret: "JBSWY3DPEHPK3PXP", Confirmed: true}
		return nil
	})
	c := f.dial(t)
	defer c.close()
	if l := c.login("wido@example.org", "correct horse"); !strings.Contains(l, "application password") {
		t.Fatalf("totp primary: %s", l)
	}
	if l := c.login("wido@example.org", "app-pass-1234"); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("app password: %s", l)
	}
	// Hold: DELE accepted in session, QUIT does not remove.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Hold = &identity.Hold{Reason: "r"}; return nil })
	c.cmd("DELE 1")
	if l := c.cmd("QUIT"); !strings.HasPrefix(l, "+OK") {
		t.Fatalf("QUIT on hold: %s", l)
	}
	c2 := f.dial(t)
	defer c2.close()
	if l := c2.login("wido@example.org", "app-pass-1234"); l != "+OK 2 messages" {
		t.Fatalf("hold deleted messages: %s", l)
	}
	// Sealed: login refused.
	_, _ = f.ids.UpdateMailbox(ctx, f.mb.ID, func(m *identity.Mailbox) error { m.Hold.Sealed = true; return nil })
	c3 := f.dial(t)
	defer c3.close()
	if l := c3.login("wido@example.org", "app-pass-1234"); !strings.HasPrefix(l, "-ERR [SYS/PERM]") {
		t.Fatalf("sealed: %s", l)
	}
}

func TestTooManyFailures(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	defer c.close()
	c.login("wido@example.org", "x")
	c.login("wido@example.org", "y")
	if l := c.login("wido@example.org", "z"); !strings.Contains(l, "too many") {
		t.Fatalf("third failure: %s", l)
	}
	if _, err := c.r.ReadString('\n'); err == nil {
		t.Fatal("connection should be closed")
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

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
