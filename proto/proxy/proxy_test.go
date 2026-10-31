package proxy_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"kittiwake/app"
	cryptoauth "kittiwake/auth"
	"kittiwake/config"
	"kittiwake/proto/httpx"
	"kittiwake/proto/imap"
	"kittiwake/proto/jmap"
	"kittiwake/proto/pop3"
	"kittiwake/proto/proxy"
	"kittiwake/proto/proxyproto"
	"kittiwake/proto/rest"
	"kittiwake/proto/submission"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	authsvc "kittiwake/service/auth"
	"kittiwake/store/identity"
)

const password = "correct horse battery"

// fixture: one platform on the memory backend with real IMAP, POP3,
// submission and JMAP servers behind PROXY-required listeners (as behind
// HAProxy), registered as nodes serving every namespace, and a proxy in
// front of them. Namespaces: default (wido@example.org), acme
// (bob@acme.example), lonely (nobody serves it), off (disabled), and
// ingress (reached through TLS listeners on localhost only).
type fixture struct {
	t     *testing.T
	a     *app.App
	px    *proxy.Server
	imap  string // proxy addresses
	pop3  string
	smtp  string
	http  *httptest.Server
	ctx   context.Context
	ports map[string]int // ingress listener ports
	plain int            // ingress IMAP without TLS
	local bool
	key   identity.ProxyKey // the proxy site's registered key (local)
	auth  *authsvc.Service  // the proxy's own verifier (local)
	// imapConns counts connections the IMAP backend accepted.
	imapConns atomic.Int32
}

// newFixture: pass-through proxy. newFixtureMode(t, true): local
// verification with a registered site key.
func newFixture(t *testing.T) *fixture { return newFixtureMode(t, false) }

// both runs a test against a pass-through and a local-mode proxy.
func both(t *testing.T, fn func(t *testing.T, f *fixture)) {
	t.Run("passthrough", func(t *testing.T) { fn(t, newFixtureMode(t, false)) })
	t.Run("local", func(t *testing.T) { fn(t, newFixtureMode(t, true)) })
}

// countingListener counts accepted connections.
type countingListener struct {
	net.Listener
	n *atomic.Int32
}

func (c countingListener) Accept() (net.Conn, error) {
	conn, err := c.Listener.Accept()
	if err == nil {
		c.n.Add(1)
	}
	return conn, err
}

func newFixtureMode(t *testing.T, local bool) *fixture {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("KW_TEST_LOG") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	cfg := config.Default()
	cfg.Ceph.Backend = "memory"
	cfg.Submission.RequireTLSProxy = false
	cfg.Proxy.TrustedAddresses = []string{"127.0.0.1", "::1"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	client, _ := app.Connect(cfg.Ceph)
	a, err := app.New(cfg, client, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, a: a, local: local, ctx: actor.With(context.Background(), actor.Actor{AccountID: uuid.New(), Username: "root@example.org", Roles: []string{"admin"}, Protocol: "admin"})}
	adm := a.Services.Admin
	for _, ns := range []string{"acme", "lonely", "off", "ingress"} {
		if _, err := adm.CreateNamespace(f.ctx, identity.Namespace{Name: ns, Enabled: ns != "off"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []struct{ domain, ns, user string }{
		{"example.org", "", "wido"}, {"acme.example", "acme", "bob"}, {"lonely.example", "lonely", "lone"}, {"off.example", "off", "nobody"}, {"ingress.example", "ingress", "ing"},
	} {
		if _, err := adm.CreateDomain(f.ctx, identity.Domain{Name: d.domain, Namespace: d.ns, Enabled: true}); err != nil {
			t.Fatal(err)
		}
		mb, err := adm.CreateMailbox(f.ctx, identity.Mailbox{Namespace: d.ns})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adm.CreateAccount(f.ctx, admin.CreateAccountRequest{Username: d.user + "@" + d.domain, MailboxID: mb.ID, Roles: []string{"read", "write", "send"}, Password: password}); err != nil {
			t.Fatal(err)
		}
	}
	// Backends behind PROXY-required listeners.
	trusted, _ := httpx.ParseTrusted(cfg.Proxy.TrustedAddresses)
	imapSrv := imap.New(imap.Config{Hostname: "backend"}, a.Services.Auth, a.Services.Mailbox, a.Services.Delivery, log)
	pop3Srv := pop3.New(pop3.Config{Hostname: "backend"}, a.Services.Auth, a.Services.Mailbox, log)
	smtpSrv := submission.New(submission.Config{Hostname: "backend", RequireTLSProxy: false}, a.Services.Auth, a.Services.Submit, log)
	rs := rest.New(rest.Config{Realm: "kittiwake", TrustedProxies: trusted, MaxBody: 1 << 20, Identity: a.Stores.Identity}, a.Services.Auth, a.Services.Admin, a.Stores.Audit, a.Stores.Queue, log)
	js := jmap.New(jmap.Config{BaseURL: "https://mail.example.org", Uploads: a.Stores.Messages}, a.Services.Mailbox, a.Services.Delivery, log)
	mux := http.NewServeMux()
	mux.Handle("/v1/", rs.AccountHandler())
	auth := httpx.Auth(a.Services.Auth, "jmap", trusted, "kittiwake")
	mux.Handle("/.well-known/jmap", auth(js.SessionHandler()))
	mux.Handle("/jmap/", auth(js))
	serve := func(mode string, serve func(net.Listener) error) string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		var inner net.Listener = l
		if mode == "imap" {
			inner = countingListener{l, &f.imapConns}
		}
		go func() { _ = serve(proxyproto.Wrap(inner, proxyproto.Required)) }()
		t.Cleanup(func() { _ = l.Close() })
		return l.Addr().String()
	}
	imapAddr := serve("imap", imapSrv.Serve)
	pop3Addr := serve("pop3", pop3Srv.Serve)
	smtpAddr := serve("submission", smtpSrv.Serve)
	jl, _ := net.Listen("tcp", "127.0.0.1:0")
	hs := &http.Server{Handler: mux, ConnContext: httpx.ConnContext}
	go func() { _ = hs.Serve(jl) }()
	t.Cleanup(func() { _ = hs.Close() })
	// A dead node that must be skipped, and one serving only "other".
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	_ = dead.Close()
	far := time.Now().Add(time.Hour)
	for mode, addr := range map[string]string{"imap": imapAddr, "pop3": pop3Addr, "submission": smtpAddr, "jmap": jl.Addr().String()} {
		for _, n := range []identity.Node{
			{Mode: mode, Address: addr, Host: "backend", Namespaces: []string{"default", "acme"}, Expires: far},
			{Mode: mode, Address: deadAddr, Host: "dead", Expires: far},
			{Mode: mode, Address: "127.0.0.1:1", Host: "other", Namespaces: []string{"other"}, Expires: far},
			{Mode: mode, Address: "127.0.0.1:2", Host: "expired", Expires: time.Now().Add(-time.Minute)},
		} {
			if _, err := a.Stores.Identity.RegisterNode(f.ctx, n); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Ingress listeners for the "ingress" namespace: TLS (self-signed)
	// with PROXY before the handshake, and a plain one.
	cert := selfSigned(t)
	f.ports = map[string]int{}
	for mode, srv := range map[string]func(net.Listener) error{"imap": imapSrv.Serve, "pop3": pop3Srv.Serve, "submission": smtpSrv.Serve} {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		tl := tls.NewListener(proxyproto.Wrap(l, proxyproto.Required), &tls.Config{Certificates: []tls.Certificate{cert}})
		go func() { _ = srv(tl) }()
		t.Cleanup(func() { _ = l.Close() })
		f.ports[mode] = l.Addr().(*net.TCPAddr).Port
	}
	jtl, _ := net.Listen("tcp", "127.0.0.1:0")
	jhs := &http.Server{Handler: mux, ConnContext: httpx.ConnContext}
	go func() { _ = jhs.Serve(tls.NewListener(jtl, &tls.Config{Certificates: []tls.Certificate{cert}})) }()
	t.Cleanup(func() { _ = jhs.Close() })
	f.ports["jmap"] = jtl.Addr().(*net.TCPAddr).Port
	pl, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = imapSrv.Serve(proxyproto.Wrap(pl, proxyproto.Required)) }()
	t.Cleanup(func() { _ = pl.Close() })
	f.plain = pl.Addr().(*net.TCPAddr).Port

	pcfg := proxy.Config{Hostname: "proxy", ConnectTimeout: 2 * time.Second, NodeTTL: time.Millisecond, IngressTLS: "insecure", IngressPorts: f.ports, TrustedProxies: trusted, RequireTLSProxy: false}
	if local {
		// The proxy site's key, registered on the platform; the proxy's
		// verifier is read-only and audits to the log only.
		priv, err := cryptoauth.NewHandoffKey()
		if err != nil {
			t.Fatal(err)
		}
		f.key, err = adm.CreateProxyKey(f.ctx, "test-site", priv.Public().(ed25519.PublicKey))
		if err != nil {
			t.Fatal(err)
		}
		f.auth = authsvc.New(a.Stores.Identity, nil, auditor.New(nil, log, nil), authsvc.Config{CacheTTL: 5 * time.Second, ReadOnly: true, HandoffKey: priv})
		pcfg.Auth = f.auth
	}
	f.px = proxy.New(pcfg, a.Stores.Identity, log)
	t.Cleanup(func() { _ = f.px.Shutdown(context.Background()) })
	f.imap = serveProxy(t, f.px.ServeIMAP)
	f.pop3 = serveProxy(t, f.px.ServePOP3)
	f.smtp = serveProxy(t, f.px.ServeSubmission)
	f.http = httptest.NewUnstartedServer(f.px.HTTPHandler())
	f.http.Config.ConnContext = proxy.ConnContext
	f.http.Start()
	t.Cleanup(f.http.Close)
	return f
}

func serveProxy(t *testing.T, serve func(net.Listener) error) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = serve(l) }()
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().String()
}

func (f *fixture) setIngress(host, policy string) {
	f.t.Helper()
	_, err := f.a.Services.Admin.UpdateNamespace(f.ctx, "ingress", func(ns *identity.Namespace) error {
		ns.Ingress = &identity.Ingress{Host: host, TLS: policy}
		return nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
	f.a.Stores.Identity.InvalidateCache()
}

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// lineClient is a tiny line-based client for the TCP protocols.
type lineClient struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func dial(t *testing.T, addr string) *lineClient {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { _ = c.Close() })
	return &lineClient{t: t, c: c, r: bufio.NewReader(c)}
}

func (l *lineClient) line() string {
	l.t.Helper()
	s, err := l.r.ReadString('\n')
	if err != nil {
		l.t.Fatalf("read: %v (got %q)", err, s)
	}
	return strings.TrimRight(s, "\r\n")
}

func (l *lineClient) send(s string) {
	l.t.Helper()
	if _, err := l.c.Write([]byte(s + "\r\n")); err != nil {
		l.t.Fatal(err)
	}
}

// imapCmd sends a tagged command and returns the tagged reply, skipping
// untagged lines.
func (l *lineClient) imapCmd(tag, cmd string) string {
	l.t.Helper()
	l.send(tag + " " + cmd)
	for {
		s := l.line()
		if strings.HasPrefix(s, tag+" ") {
			return s
		}
	}
}

func plainIR(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte("\x00" + user + "\x00" + pass))
}

func TestIMAP(t *testing.T) { both(t, testIMAP) }

func testIMAP(t *testing.T, f *fixture) {
	c := dial(t, f.imap)
	if g := c.line(); !strings.HasPrefix(g, "* OK [CAPABILITY IMAP4rev1") || !strings.Contains(g, "AUTH=PLAIN") {
		t.Fatalf("greeting: %s", g)
	}
	c.send("a CAPABILITY")
	if s := c.line(); !strings.HasPrefix(s, "* CAPABILITY ") || !strings.Contains(s, "LITERAL+") {
		t.Fatalf("capability: %s", s)
	}
	c.line()
	// Wrong password: the backend's refusal is relayed, the session goes on.
	if r := c.imapCmd("b", `LOGIN wido@example.org wrong`); !strings.HasPrefix(r, "b NO [AUTHENTICATIONFAILED]") {
		t.Fatalf("wrong password: %s", r)
	}
	if r := c.imapCmd("c", `LOGIN nobody@nowhere.example x`); !strings.HasPrefix(r, "c NO [AUTHENTICATIONFAILED] Authentication failed") {
		t.Fatalf("unknown domain: %s", r)
	}
	if r := c.imapCmd("d", `LOGIN nobody@off.example x`); !strings.HasPrefix(r, "d NO [AUTHENTICATIONFAILED] Account disabled") {
		t.Fatalf("disabled namespace: %s", r)
	}
	if r := c.imapCmd("e", `LOGIN lone@lonely.example "`+password+`"`); !strings.HasPrefix(r, "e NO [UNAVAILABLE]") {
		t.Fatalf("no backend: %s", r)
	}
	if r := c.imapCmd("f", `SELECT INBOX`); !strings.HasPrefix(r, "f BAD") {
		t.Fatalf("pre-login select: %s", r)
	}
	// Quoted password, then the spliced session talks to the backend.
	if r := c.imapCmd("g", `LOGIN wido@example.org "`+password+`"`); !strings.HasPrefix(r, "g OK") {
		t.Fatalf("login: %s", r)
	}
	if r := c.imapCmd("h", "SELECT INBOX"); !strings.HasPrefix(r, "h OK") {
		t.Fatalf("select through the proxy: %s", r)
	}
	if r := c.imapCmd("i", "NAMESPACE"); !strings.HasPrefix(r, "i OK") {
		t.Fatalf("namespace through the proxy: %s", r)
	}
	c.send("j LOGOUT")
	if s := c.line(); !strings.HasPrefix(s, "* BYE") {
		t.Fatalf("logout: %s", s)
	}

	// Synchronizing literal, another namespace.
	c = dial(t, f.imap)
	c.line()
	c.send("a LOGIN bob@acme.example {" + itoa(len(password)) + "}")
	if s := c.line(); !strings.HasPrefix(s, "+") {
		t.Fatalf("continuation: %s", s)
	}
	c.send(password)
	for {
		s := c.line()
		if strings.HasPrefix(s, "a ") {
			if !strings.HasPrefix(s, "a OK") {
				t.Fatalf("literal login: %s", s)
			}
			break
		}
	}
	if r := c.imapCmd("b", "LIST \"\" *"); !strings.HasPrefix(r, "b OK") {
		t.Fatalf("list: %s", r)
	}

	// AUTHENTICATE PLAIN with and without an initial response.
	c = dial(t, f.imap)
	c.line()
	if r := c.imapCmd("a", "AUTHENTICATE PLAIN "+plainIR("bob@acme.example", password)); !strings.HasPrefix(r, "a OK") {
		t.Fatalf("authenticate ir: %s", r)
	}
	c = dial(t, f.imap)
	c.line()
	c.send("a AUTHENTICATE PLAIN")
	if s := c.line(); s != "+ " && s != "+" {
		t.Fatalf("sasl continuation: %q", s)
	}
	c.send(plainIR("wido@example.org", password))
	for {
		s := c.line()
		if strings.HasPrefix(s, "a ") {
			if !strings.HasPrefix(s, "a OK") {
				t.Fatalf("authenticate: %s", s)
			}
			break
		}
	}
	if r := c.imapCmd("b", "STATUS INBOX (MESSAGES)"); !strings.HasPrefix(r, "b OK") {
		t.Fatalf("status: %s", r)
	}
}

func itoa(n int) string { return strings.TrimSpace(strings.Repeat(" ", 0) + strconvItoa(n)) }

func strconvItoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestPOP3(t *testing.T) { both(t, testPOP3) }

func testPOP3(t *testing.T, f *fixture) {
	c := dial(t, f.pop3)
	if g := c.line(); !strings.HasPrefix(g, "+OK") {
		t.Fatalf("greeting: %s", g)
	}
	c.send("CAPA")
	var caps []string
	for s := c.line(); s != "."; s = c.line() {
		caps = append(caps, s)
	}
	if !strings.Contains(strings.Join(caps, "\n"), "SASL PLAIN") {
		t.Fatalf("capa: %v", caps)
	}
	c.send("STAT")
	if s := c.line(); !strings.HasPrefix(s, "-ERR") {
		t.Fatalf("pre-login stat: %s", s)
	}
	c.send("USER wido@example.org")
	c.line()
	c.send("PASS wrong")
	if s := c.line(); !strings.HasPrefix(s, "-ERR") {
		t.Fatalf("wrong password: %s", s)
	}
	c.send("USER nobody@nowhere.example")
	c.line()
	c.send("PASS x")
	if s := c.line(); s != "-ERR [AUTH] authentication failed" {
		t.Fatalf("unknown domain: %s", s)
	}
	c.send("USER lone@lonely.example")
	c.line()
	c.send("PASS " + password)
	if s := c.line(); !strings.HasPrefix(s, "-ERR [SYS/TEMP]") {
		t.Fatalf("no backend: %s", s)
	}
	c.send("USER wido@example.org")
	c.line()
	c.send("PASS " + password)
	if s := c.line(); !strings.HasPrefix(s, "+OK") {
		t.Fatalf("login: %s", s)
	}
	c.send("STAT")
	if s := c.line(); !strings.HasPrefix(s, "+OK 0 ") {
		t.Fatalf("stat through the proxy: %s", s)
	}
	c.send("QUIT")
	c.line()

	c = dial(t, f.pop3)
	c.line()
	c.send("AUTH PLAIN")
	if s := c.line(); !strings.HasPrefix(s, "+") {
		t.Fatalf("sasl continuation: %s", s)
	}
	c.send(plainIR("bob@acme.example", password))
	if s := c.line(); !strings.HasPrefix(s, "+OK") {
		t.Fatalf("auth plain: %s", s)
	}
	c.send("UIDL")
	if s := c.line(); !strings.HasPrefix(s, "+OK") {
		t.Fatalf("uidl through the proxy: %s", s)
	}
}

func TestSubmission(t *testing.T) { both(t, testSubmission) }

func testSubmission(t *testing.T, f *fixture) {
	c := dial(t, f.smtp)
	if g := c.line(); !strings.HasPrefix(g, "220 ") {
		t.Fatalf("greeting: %s", g)
	}
	c.send("EHLO client.example")
	var ehlo []string
	for {
		s := c.line()
		ehlo = append(ehlo, s)
		if strings.HasPrefix(s, "250 ") {
			break
		}
	}
	if !strings.Contains(strings.Join(ehlo, "\n"), "250-AUTH PLAIN LOGIN") {
		t.Fatalf("ehlo: %v", ehlo)
	}
	c.send("MAIL FROM:<wido@example.org>")
	if s := c.line(); !strings.HasPrefix(s, "530") {
		t.Fatalf("pre-auth mail: %s", s)
	}
	c.send("AUTH PLAIN " + plainIR("nobody@nowhere.example", "x"))
	if s := c.line(); !strings.HasPrefix(s, "535") {
		t.Fatalf("unknown domain: %s", s)
	}
	c.send("AUTH PLAIN " + plainIR("wido@example.org", "wrong"))
	if s := c.line(); !strings.HasPrefix(s, "535") {
		t.Fatalf("wrong password: %s", s)
	}
	c.send("AUTH PLAIN " + plainIR("wido@example.org", password))
	if s := c.line(); !strings.HasPrefix(s, "235") {
		t.Fatalf("auth: %s", s)
	}
	c.send("MAIL FROM:<wido@example.org>")
	if s := c.line(); !strings.HasPrefix(s, "250") {
		t.Fatalf("mail through the proxy: %s", s)
	}
	c.send("QUIT")

	// LOGIN, interactive, into the other namespace.
	c = dial(t, f.smtp)
	c.line()
	c.send("EHLO x")
	for s := c.line(); !strings.HasPrefix(s, "250 "); s = c.line() {
	}
	c.send("AUTH LOGIN")
	if s := c.line(); s != "334 VXNlcm5hbWU6" {
		t.Fatalf("username prompt: %s", s)
	}
	c.send(base64.StdEncoding.EncodeToString([]byte("bob@acme.example")))
	if s := c.line(); s != "334 UGFzc3dvcmQ6" {
		t.Fatalf("password prompt from the backend: %s", s)
	}
	c.send(base64.StdEncoding.EncodeToString([]byte(password)))
	if s := c.line(); !strings.HasPrefix(s, "235") {
		t.Fatalf("auth login: %s", s)
	}
}

func (f *fixture) get(path string, headers map[string]string) (*http.Response, map[string]any) {
	f.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, f.http.URL+path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp, body
}

func (f *fixture) post(path string, form url.Values) (*http.Response, map[string]any) {
	f.t.Helper()
	resp, err := http.Post(f.http.URL+path, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp, body
}

func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func TestHTTP(t *testing.T) { both(t, testHTTP) }

func testHTTP(t *testing.T, f *fixture) {
	if resp, _ := f.get("/.well-known/jmap", nil); resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("no credential: %d", resp.StatusCode)
	}
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("nobody@nowhere.example", "x")}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown domain: %d", resp.StatusCode)
	}
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("lone@lonely.example", password)}); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("no backend: %d", resp.StatusCode)
	}
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("nobody@off.example", "x")}); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("disabled: %d", resp.StatusCode)
	}
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("wido@example.org", "wrong")}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password relayed: %d", resp.StatusCode)
	}
	resp, sess := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("wido@example.org", password)})
	if resp.StatusCode != http.StatusOK || sess["username"] != "wido@example.org" {
		t.Fatalf("basic session: %d %v", resp.StatusCode, sess)
	}
	// Password grant through the proxy, then the JWT and the refresh token route by themselves.
	resp, tok := f.post("/v1/oauth/token", url.Values{"grant_type": {"password"}, "username": {"bob@acme.example"}, "password": {password}})
	if resp.StatusCode != http.StatusOK || tok["access_token"] == nil {
		t.Fatalf("token: %d %v", resp.StatusCode, tok)
	}
	resp, sess = f.get("/.well-known/jmap", map[string]string{"Authorization": "Bearer " + tok["access_token"].(string)})
	if resp.StatusCode != http.StatusOK || sess["username"] != "bob@acme.example" {
		t.Fatalf("jwt session: %d %v", resp.StatusCode, sess)
	}
	resp, tok2 := f.post("/v1/oauth/token", url.Values{"grant_type": {"refresh_token"}, "refresh_token": {tok["refresh_token"].(string)}})
	if resp.StatusCode != http.StatusOK || tok2["access_token"] == nil {
		t.Fatalf("refresh: %d %v", resp.StatusCode, tok2)
	}
	resp, _ = f.post("/v1/oauth/revoke", url.Values{"token": {tok2["refresh_token"].(string)}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("revoke: %d", resp.StatusCode)
	}
	if resp, _ := f.post("/v1/oauth/token", url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"kwr1.x.not-a-uuid.y"}}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("malformed refresh: %d", resp.StatusCode)
	}
	// An API token: one lookup, then cached.
	acct, _ := f.a.Stores.Identity.GetAccountByUsername(f.ctx, "wido@example.org")
	_, secret, err := f.a.Services.Admin.CreateToken(f.ctx, acct.Value.ID, []string{"read"}, time.Now().Add(time.Hour), "test")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		resp, sess = f.get("/.well-known/jmap", map[string]string{"Authorization": "Bearer " + secret})
		if resp.StatusCode != http.StatusOK || sess["username"] != "wido@example.org" {
			t.Fatalf("api token session: %d %v", resp.StatusCode, sess)
		}
	}
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": "Bearer kwt_unknown"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown api token: %d", resp.StatusCode)
	}
	// A JMAP call goes through too.
	req, _ := http.NewRequest(http.MethodPost, f.http.URL+"/jmap/api", strings.NewReader(`{"using":["urn:ietf:params:jmap:core","urn:ietf:params:jmap:mail"],"methodCalls":[["Mailbox/get",{"accountId":"`+sess["primaryAccounts"].(map[string]any)["urn:ietf:params:jmap:mail"].(string)+`"},"0"]]}`))
	req.Header.Set("Authorization", basic("wido@example.org", password))
	req.Header.Set("Content-Type", "application/json")
	r2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r2.Body)
	r2.Body.Close()
	if r2.StatusCode != http.StatusOK || !strings.Contains(string(b), "Mailbox/get") {
		t.Fatalf("jmap api through the proxy: %d %s", r2.StatusCode, b)
	}
}

func TestIngress(t *testing.T) { both(t, testIngress) }

func testIngress(t *testing.T, f *fixture) {
	login := func(expectOK bool, what string) {
		t.Helper()
		c := dial(t, f.imap)
		c.line()
		r := c.imapCmd("a", `LOGIN ing@ingress.example "`+password+`"`)
		if ok := strings.HasPrefix(r, "a OK"); ok != expectOK {
			t.Fatalf("%s: %s", what, r)
		}
		if expectOK {
			if r := c.imapCmd("b", "SELECT INBOX"); !strings.HasPrefix(r, "b OK") {
				t.Fatalf("%s select: %s", what, r)
			}
		}
	}
	// No ingress, no nodes: unavailable.
	login(false, "no ingress")
	// Insecure (the proxy default here): the self-signed ingress is fine.
	f.setIngress("localhost", "")
	login(true, "insecure via default")
	// Strict: a self-signed certificate is refused.
	f.setIngress("localhost", identity.IngressTLSStrict)
	login(false, "strict against self-signed")
	// Off: plain TCP to the plain port.
	f.setIngress("localhost", identity.IngressTLSOff)
	login(false, "off against a TLS port")
	// HTTP through the TLS ingress.
	f.setIngress("localhost", identity.IngressTLSInsecure)
	resp, sess := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("ing@ingress.example", password)})
	if resp.StatusCode != http.StatusOK || sess["username"] != "ing@ingress.example" {
		t.Fatalf("http via ingress: %d %v", resp.StatusCode, sess)
	}
	// POP3 and submission through the TLS ingress.
	c := dial(t, f.pop3)
	c.line()
	c.send("USER ing@ingress.example")
	c.line()
	c.send("PASS " + password)
	if s := c.line(); !strings.HasPrefix(s, "+OK") {
		t.Fatalf("pop3 via ingress: %s", s)
	}
	c = dial(t, f.smtp)
	c.line()
	c.send("EHLO x")
	for s := c.line(); !strings.HasPrefix(s, "250 "); s = c.line() {
	}
	c.send("AUTH PLAIN " + plainIR("ing@ingress.example", password))
	if s := c.line(); !strings.HasPrefix(s, "235") {
		t.Fatalf("submission via ingress: %s", s)
	}
}

func TestIngressPlain(t *testing.T) {
	f := newFixture(t)
	// A proxy whose IMAP ingress port is the plain listener: policy off works.
	ports := map[string]int{"imap": f.plain}
	px := proxy.New(proxy.Config{Hostname: "proxy", ConnectTimeout: 2 * time.Second, NodeTTL: time.Millisecond, IngressPorts: ports}, f.a.Stores.Identity, slog.New(slog.NewTextHandler(io.Discard, nil)))
	addr := serveProxy(t, px.ServeIMAP)
	f.setIngress("localhost", identity.IngressTLSOff)
	c := dial(t, addr)
	c.line()
	if r := c.imapCmd("a", `LOGIN ing@ingress.example "`+password+`"`); !strings.HasPrefix(r, "a OK") {
		t.Fatalf("plain ingress: %s", r)
	}
	// Strict by default against the plain port: refused.
	f.setIngress("localhost", "")
	c = dial(t, addr)
	c.line()
	if r := c.imapCmd("a", `LOGIN ing@ingress.example "`+password+`"`); !strings.HasPrefix(r, "a NO [UNAVAILABLE]") {
		t.Fatalf("strict against plain: %s", r)
	}
}

// TestLocal covers what only local mode does: refusals without a backend
// connection, the failure limit, the handoff refused by a backend that
// does not know the key, and the pass-through of a legacy hash.
func TestLocal(t *testing.T) {
	f := newFixtureMode(t, true)
	before := f.imapConns.Load()
	c := dial(t, f.imap)
	c.line()
	if r := c.imapCmd("a", `LOGIN wido@example.org wrong`); !strings.HasPrefix(r, "a NO [AUTHENTICATIONFAILED] Authentication failed") {
		t.Fatalf("wrong password: %s", r)
	}
	if r := c.imapCmd("b", `LOGIN nobody@example.org x`); !strings.HasPrefix(r, "b NO [AUTHENTICATIONFAILED] Authentication failed") {
		t.Fatalf("unknown user: %s", r)
	}
	if r := c.imapCmd("c", "AUTHENTICATE PLAIN "+plainIR("wido@example.org", "wrong")); !strings.HasPrefix(r, "c NO [AUTHENTICATIONFAILED]") {
		t.Fatalf("wrong password via PLAIN: %s", r)
	}
	if n := f.imapConns.Load(); n != before {
		t.Fatalf("refused logins reached the backend: %d connections", n-before)
	}
	if r := c.imapCmd("d", `LOGIN wido@example.org "`+password+`"`); !strings.HasPrefix(r, "d OK") {
		t.Fatalf("login: %s", r)
	}
	if r := c.imapCmd("e", "SELECT INBOX"); !strings.HasPrefix(r, "e OK") {
		t.Fatalf("select after handoff: %s", r)
	}
	if n := f.imapConns.Load(); n != before+1 {
		t.Fatalf("handoff connections: %d", n-before)
	}
	c.send("f LOGOUT")

	// A disabled account and a wrong-password submission are refused in
	// the backends' words; submission also needs the send role.
	acct, _ := f.a.Stores.Identity.GetAccountByUsername(f.ctx, "bob@acme.example")
	if _, err := f.a.Services.Admin.UpdateAccount(f.ctx, acct.Value.ID, func(a *identity.Account) error { a.Roles = []string{"read"}; return nil }); err != nil {
		t.Fatal(err)
	}
	f.auth.InvalidateCache()
	f.a.Stores.Identity.InvalidateCache()
	sc := dial(t, f.smtp)
	sc.line()
	sc.send("EHLO x")
	for l := sc.line(); !strings.HasPrefix(l, "250 "); l = sc.line() {
	}
	sc.send("AUTH PLAIN " + plainIR("bob@acme.example", password))
	if l := sc.line(); l != "535 5.7.8 Account may not send mail" {
		t.Fatalf("no send role: %s", l)
	}
	sc.send("AUTH LOGIN " + base64.StdEncoding.EncodeToString([]byte("wido@example.org")))
	if l := sc.line(); l != "334 UGFzc3dvcmQ6" {
		t.Fatalf("password prompt: %s", l)
	}
	sc.send(base64.StdEncoding.EncodeToString([]byte("wrong")))
	if l := sc.line(); l != "535 5.7.8 Authentication failed" {
		t.Fatalf("wrong password via LOGIN: %s", l)
	}
	sc.send("AUTH LOGIN " + base64.StdEncoding.EncodeToString([]byte("wido@example.org")))
	sc.line()
	sc.send(base64.StdEncoding.EncodeToString([]byte(password)))
	if l := sc.line(); !strings.HasPrefix(l, "235") {
		t.Fatalf("auth login: %s", l)
	}

	// POP3 closes after three failures, as the backend does.
	pc := dial(t, f.pop3)
	pc.line()
	for i := 0; i < 2; i++ {
		pc.send("USER wido@example.org")
		pc.line()
		pc.send("PASS wrong")
		if l := pc.line(); l != "-ERR [AUTH] authentication failed" {
			t.Fatalf("pop3 wrong password: %s", l)
		}
	}
	pc.send("USER wido@example.org")
	pc.line()
	pc.send("PASS wrong")
	if l := pc.line(); !strings.Contains(l, "too many failures") {
		t.Fatalf("pop3 limit: %s", l)
	}
	if _, err := pc.r.ReadString('\n'); err == nil {
		t.Fatal("pop3 connection stayed open after the failure limit")
	}

	// HTTP: a wrong password is a 401 from the proxy, a right one is
	// handed over as Bearer and the backend answers.
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("wido@example.org", "wrong")}); resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("http wrong password: %d", resp.StatusCode)
	}
	if resp, sess := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("wido@example.org", password)}); resp.StatusCode != http.StatusOK || sess["username"] != "wido@example.org" {
		t.Fatalf("http handoff: %d %v", resp.StatusCode, sess)
	}

	// A legacy hash: verified here, then passed through so the backend
	// upgrades it; the next login hands off.
	legacy, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	ing, _ := f.a.Stores.Identity.GetAccountByUsername(f.ctx, "ing@ingress.example")
	if _, err := f.a.Stores.Identity.UpdateAccount(f.ctx, ing.Value.ID, func(a *identity.Account) error {
		a.Passwords[0].Hash = "{BLF-CRYPT}" + string(legacy)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.a.Services.Auth.InvalidateCache()
	f.auth.InvalidateCache()
	f.setIngress("localhost", identity.IngressTLSInsecure)
	c = dial(t, f.imap)
	c.line()
	if r := c.imapCmd("a", `LOGIN ing@ingress.example "`+password+`"`); !strings.HasPrefix(r, "a OK") {
		t.Fatalf("legacy login: %s", r)
	}
	ing, _ = f.a.Stores.Identity.GetAccountByUsername(f.ctx, "ing@ingress.example")
	if !strings.HasPrefix(ing.Value.Passwords[0].Hash, "{ARGON2ID}") {
		t.Fatalf("hash not upgraded by the backend: %s", ing.Value.Passwords[0].Hash[:12])
	}

	// The key is removed: the backend refuses the handoff and the client
	// is told the server is unavailable, not that it was wrong.
	if err := f.a.Services.Admin.DeleteProxyKey(f.ctx, f.key.ID); err != nil {
		t.Fatal(err)
	}
	f.a.Services.Auth.InvalidateProxyKeys()
	f.auth.InvalidateCache()
	c = dial(t, f.imap)
	c.line()
	if r := c.imapCmd("a", `LOGIN wido@example.org "`+password+`"`); !strings.HasPrefix(r, "a NO [UNAVAILABLE] The server refused the proxied login") {
		t.Fatalf("handoff without key: %s", r)
	}
	pc = dial(t, f.pop3)
	pc.line()
	pc.send("AUTH PLAIN " + plainIR("wido@example.org", password))
	if l := pc.line(); !strings.HasPrefix(l, "-ERR [SYS/TEMP] the server refused") {
		t.Fatalf("pop3 handoff without key: %s", l)
	}
	sc = dial(t, f.smtp)
	sc.line()
	sc.send("EHLO x")
	for l := sc.line(); !strings.HasPrefix(l, "250 "); l = sc.line() {
	}
	sc.send("AUTH PLAIN " + plainIR("wido@example.org", password))
	if l := sc.line(); !strings.HasPrefix(l, "454 4.7.0 The server refused") {
		t.Fatalf("smtp handoff without key: %s", l)
	}
	if resp, _ := f.get("/.well-known/jmap", map[string]string{"Authorization": basic("wido@example.org", password)}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("http handoff without key: %d", resp.StatusCode)
	}
}
