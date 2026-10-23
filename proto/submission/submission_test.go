package submission

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"

	cryptoauth "kittiwake/auth"
	"kittiwake/proto/proxyproto"
	"kittiwake/rados"
	"kittiwake/service/auditor"
	authsvc "kittiwake/service/auth"
	"kittiwake/service/delivery"
	"kittiwake/service/submit"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
	sessionstore "kittiwake/store/session"
)

type fakeRelay struct {
	mu   sync.Mutex
	from string
	to   []string
	data string
	err  error
}

func (f *fakeRelay) Send(_ context.Context, from string, to []string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.from, f.to, f.data = from, to, string(data)
	return f.err
}

type fixture struct {
	addr  string
	relay *fakeRelay
	idx   *index.Store
	mb    identity.Mailbox
	acct  identity.Account
}

// newFixture serves submission behind a PROXY-required listener, like
// production behind HAProxy.
func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	ids := identity.New(meta)
	f := &fixture{relay: &fakeRelay{}, idx: index.New(idxp)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(audit.New(idxp), log, nil)
	del := delivery.New(ids, f.idx, message.New(msgp), attach.New(attp), queue.New(idxp), aud, log, delivery.Config{})
	auth := authsvc.New(ids, sessionstore.New(idxp), aud, authsvc.Config{})
	sub := submit.New(submit.Config{Hostname: "mx.test"}, ids, f.idx, del, aud, f.relay, log)
	ctx := context.Background()
	_, _ = ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = ids.CreateMailbox(ctx, identity.Mailbox{})
	h, _ := cryptoauth.HashPassword("app-pass-1234")
	f.acct, _ = ids.CreateAccount(ctx, identity.Account{
		Username: "wido@example.org", MailboxID: f.mb.ID, Roles: []string{"read", "write", "send"}, Enabled: true,
		Services:  []string{identity.ServiceIMAP}, // submission is governed by the send role, not a service
		Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordApplication}},
	})
	_, _ = ids.CreateAccount(ctx, identity.Account{
		Username: "reader@example.org", MailboxID: f.mb.ID, Roles: []string{"read"}, Enabled: true,
		Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordApplication}},
	})
	_, _ = ids.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: f.mb.ID}}})
	cfg.Hostname = "mx.test"
	srv := New(cfg, auth, sub, log)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(proxyproto.Wrap(l, proxyproto.Required)) }()
	t.Cleanup(func() { _ = srv.Close() })
	f.addr = l.Addr().String()
	return f
}

// dial connects with a PROXY v2 header for 2001:db8::7, with the SSL TLV
// when tlsClient.
func (f *fixture) dial(t *testing.T, tlsClient bool) *smtp.Client {
	t.Helper()
	conn, err := net.Dial("tcp", f.addr)
	if err != nil {
		t.Fatal(err)
	}
	hdr := []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x21, 0x21}
	body := make([]byte, 36)
	copy(body[0:16], net.ParseIP("2001:db8::7").To16())
	copy(body[16:32], net.ParseIP("2001:db8::1").To16())
	binary.BigEndian.PutUint16(body[32:34], 40000)
	binary.BigEndian.PutUint16(body[34:36], 465)
	if tlsClient {
		body = append(body, 0x20, 0x00, 0x05, 0x01, 0, 0, 0, 0)
	}
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(body)))
	if _, err := conn.Write(append(hdr, body...)); err != nil {
		t.Fatal(err)
	}
	c := smtp.NewClient(conn)
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Hello("client.test"); err != nil {
		t.Fatal(err)
	}
	return c
}

func code(err error) int {
	var se *smtp.SMTPError
	if errors.As(err, &se) {
		return se.Code
	}
	return 0
}

const msg = "From: Wido <wido@example.org>\r\nTo: bob@other.example\r\nBcc: secret@other.example\r\nSubject: hi\r\n\r\nhello\r\n"

func send(c *smtp.Client, from string, to []string, raw string) error {
	if err := c.Mail(from, nil); err != nil {
		return err
	}
	for _, r := range to {
		if err := c.Rcpt(r, nil); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, raw); err != nil {
		return err
	}
	return w.Close()
}

func TestSubmit(t *testing.T) {
	f := newFixture(t, Config{RequireTLSProxy: true})
	c := f.dial(t, true)
	if ok, mechs := c.Extension("AUTH"); !ok || mechs != "PLAIN LOGIN OAUTHBEARER XOAUTH2" {
		t.Fatalf("AUTH: %v %q", ok, mechs)
	}
	if err := c.Mail("wido@example.org", nil); code(err) != 530 {
		t.Fatalf("MAIL before AUTH: %v", err)
	}
	if err := c.Auth(sasl.NewPlainClient("", "wido@example.org", "wrong")); code(err) != 535 {
		t.Fatalf("wrong password: %v", err)
	}
	if err := c.Auth(sasl.NewLoginClient("wido@example.org", "app-pass-1234")); err != nil {
		t.Fatalf("LOGIN: %v", err)
	}
	if err := c.Mail("", nil); code(err) != 550 {
		t.Fatalf("null sender: %v", err)
	}
	if err := c.Mail("ceo@example.org", nil); code(err) != 550 {
		t.Fatalf("foreign sender: %v", err)
	}
	if err := c.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := send(c, "wido@example.org", []string{"bob@other.example", "secret@other.example"}, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	f.relay.mu.Lock()
	data, from, to := f.relay.data, f.relay.from, f.relay.to
	f.relay.mu.Unlock()
	if from != "wido@example.org" || len(to) != 2 {
		t.Fatalf("envelope: %s %v", from, to)
	}
	if !strings.HasPrefix(data, "Received: from [2001:db8::7] by mx.test (Kittiwake) with ESMTPSA for <wido@example.org>;") ||
		!strings.Contains(data, "\r\nX-Kittiwake-Account: "+f.acct.ID.String()+"\r\n") || strings.Contains(data, "Bcc:") {
		t.Fatalf("relayed: %q", data)
	}
	// No Sent copy by default.
	root, _ := f.idx.EnsureRoot(context.Background(), f.mb.ID)
	if root.FolderByRole(index.RoleSent).Total != 0 {
		t.Fatal("Sent copy stored with save_to_sent off")
	}
	// Header From that is not an identity is refused at DATA.
	if err := send(c, "info@example.org", []string{"bob@other.example"}, strings.Replace(msg, "wido@example.org", "ceo@example.org", 1)); code(err) != 550 {
		t.Fatalf("header From spoof: %v", err)
	}
	// The relay's reply reaches the client unchanged; a dead relay is temporary.
	f.relay.mu.Lock()
	f.relay.err = &smtp.SMTPError{Code: 554, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "Rate limit exceeded"}
	f.relay.mu.Unlock()
	if err := send(c, "wido@example.org", []string{"bob@other.example"}, msg); code(err) != 554 || !strings.Contains(err.Error(), "Rate limit") {
		t.Fatalf("relay rejection: %v", err)
	}
	f.relay.mu.Lock()
	f.relay.err = errors.New("dial tcp: connection refused")
	f.relay.mu.Unlock()
	if err := send(c, "wido@example.org", []string{"bob@other.example"}, msg); code(err) != 451 {
		t.Fatalf("relay down: %v", err)
	}
}

func TestSubmitAuthPolicy(t *testing.T) {
	f := newFixture(t, Config{RequireTLSProxy: true, SaveToSent: true})
	// Without the proxy's TLS flag no AUTH is offered or accepted.
	c := f.dial(t, false)
	if ok, _ := c.Extension("AUTH"); ok {
		t.Fatal("AUTH offered on a plain client connection")
	}
	// An account without the send role cannot log in.
	c = f.dial(t, true)
	if err := c.Auth(sasl.NewPlainClient("", "reader@example.org", "app-pass-1234")); code(err) != 535 {
		t.Fatalf("no send role: %v", err)
	}
	if err := c.Auth(sasl.NewPlainClient("", "wido@example.org", "app-pass-1234")); err != nil {
		t.Fatal(err)
	}
	// Sending as an alias: envelope and header From both the alias.
	asAlias := strings.Replace(msg, "From: Wido <wido@example.org>", "From: Info <info@example.org>", 1)
	if err := send(c, "info@example.org", []string{"bob@other.example"}, asAlias); err != nil {
		t.Fatalf("send as alias: %v", err)
	}
	f.relay.mu.Lock()
	from, data := f.relay.from, f.relay.data
	f.relay.mu.Unlock()
	if from != "info@example.org" || !strings.Contains(data, "From: Info <info@example.org>") {
		t.Fatalf("alias relayed as %s: %q", from, data)
	}
	root, _ := f.idx.EnsureRoot(context.Background(), f.mb.ID)
	if root.FolderByRole(index.RoleSent).Total != 1 {
		t.Fatal("no Sent copy with save_to_sent on")
	}
}

func TestLoginServer(t *testing.T) {
	var got [2]string
	l := &loginServer{auth: func(u, p string) error { got = [2]string{u, p}; return nil }}
	if ch, done, _ := l.Next([]byte("wido")); done || string(ch) != "Password:" {
		t.Fatalf("initial response: %q %v", ch, done)
	}
	if _, done, err := l.Next([]byte("pw")); !done || err != nil || got != [2]string{"wido", "pw"} {
		t.Fatalf("password: %v %v %v", done, err, got)
	}
}
