package submit

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
	"github.com/google/uuid"

	"kittiwake/service/actor"
)

// A relay that records whether each message arrived over TLS.
type recordingBackend struct {
	mu     sync.Mutex
	tls    []bool
	hellos []string // EHLO names of every session, including pre-STARTTLS ones
}

type recordingSession struct {
	b *recordingBackend
	c *smtp.Conn
}

func (b *recordingBackend) NewSession(c *smtp.Conn) (smtp.Session, error) {
	b.mu.Lock()
	b.hellos = append(b.hellos, c.Hostname())
	b.mu.Unlock()
	return &recordingSession{b: b, c: c}, nil
}
func (s *recordingSession) Reset()                               {}
func (s *recordingSession) Logout() error                        { return nil }
func (s *recordingSession) Mail(string, *smtp.MailOptions) error { return nil }
func (s *recordingSession) Rcpt(string, *smtp.RcptOptions) error { return nil }
func (s *recordingSession) Data(r io.Reader) error {
	_, _ = io.ReadAll(r)
	_, ok := s.c.TLSConnectionState()
	s.b.mu.Lock()
	s.b.tls = append(s.b.tls, ok)
	s.b.mu.Unlock()
	return nil
}

func selfSigned(t *testing.T, name string) tls.Certificate {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// startRelay runs a go-smtp server on a loopback port; with withTLS it
// offers STARTTLS with a self-signed certificate for "relay.test".
func startRelay(t *testing.T, withTLS bool) (string, *recordingBackend) {
	t.Helper()
	b := &recordingBackend{}
	srv := smtp.NewServer(b)
	srv.Domain = "relay.test"
	srv.AllowInsecureAuth = true
	if withTLS {
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{selfSigned(t, "relay.test")}}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return l.Addr().String(), b
}

func send(t *testing.T, cfg Config) error {
	t.Helper()
	cfg.Hostname = "client.test"
	r := &smtpRelay{cfg: cfg}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return r.Send(ctx, "a@example.org", []string{"b@example.org"}, []byte("Subject: t\r\n\r\nhi\r\n"))
}

func TestRelayTLSModes(t *testing.T) {
	tlsAddr, tlsB := startRelay(t, true)
	plainAddr, plainB := startRelay(t, false)

	// Default (opportunistic): encrypted when offered, plain otherwise.
	if err := send(t, Config{RelayHost: tlsAddr}); err != nil {
		t.Fatalf("opportunistic to STARTTLS relay: %v", err)
	}
	if err := send(t, Config{RelayHost: plainAddr, TLS: "opportunistic"}); err != nil {
		t.Fatalf("opportunistic to plain relay: %v", err)
	}
	// Off: plain even when offered.
	if err := send(t, Config{RelayHost: tlsAddr, TLS: "off"}); err != nil {
		t.Fatalf("off: %v", err)
	}
	// Required: a self-signed certificate is refused, accepted with skip-verify,
	// and a relay without STARTTLS is refused.
	err := send(t, Config{RelayHost: tlsAddr, TLS: "required", TLSServerName: "relay.test"})
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("required with self-signed certificate: %v, want a certificate error", err)
	}
	if err := send(t, Config{RelayHost: tlsAddr, TLS: "required", TLSInsecureSkipVerify: true}); err != nil {
		t.Fatalf("required with skip-verify: %v", err)
	}
	if err := send(t, Config{RelayHost: plainAddr, TLS: "required"}); !errors.Is(err, ErrNoStartTLS) {
		t.Fatalf("required to plain relay: %v, want ErrNoStartTLS", err)
	}

	tlsB.mu.Lock()
	defer tlsB.mu.Unlock()
	if got := tlsB.tls; len(got) != 3 || !got[0] || got[1] || !got[2] {
		t.Fatalf("TLS relay saw %v, want [true false true] (opportunistic, off, required+skip-verify)", got)
	}
	plainB.mu.Lock()
	defer plainB.mu.Unlock()
	if got := plainB.tls; len(got) != 1 || got[0] {
		t.Fatalf("plain relay saw %v, want [false]", got)
	}
	// Every EHLO, before and after STARTTLS, carries the configured name.
	for _, h := range append(append([]string{}, tlsB.hellos...), plainB.hellos...) {
		if h != "client.test" {
			t.Fatalf("EHLO names %v %v, want client.test throughout", tlsB.hellos, plainB.hellos)
		}
	}
}

// scriptedRelay is a minimal Postfix-like SMTP server (go-smtp's server has
// no XCLIENT). It records the commands it saw.
func scriptedRelay(t *testing.T, offerXClient bool) (string, func() string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var mu sync.Mutex
	var seen []string
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				tp := textproto.NewConn(c)
				_ = tp.PrintfLine("220 relay.test ESMTP")
				data := false
				for {
					line, err := tp.ReadLine()
					if err != nil {
						return
					}
					if data {
						if line == "." {
							data = false
							_ = tp.PrintfLine("250 2.0.0 queued")
						}
						continue
					}
					mu.Lock()
					seen = append(seen, line)
					mu.Unlock()
					verb, _, _ := strings.Cut(strings.ToUpper(line), " ")
					switch verb {
					case "EHLO":
						if offerXClient {
							_ = tp.PrintfLine("250-relay.test\r\n250-XCLIENT NAME ADDR LOGIN\r\n250 8BITMIME")
						} else {
							_ = tp.PrintfLine("250-relay.test\r\n250 8BITMIME")
						}
					case "XCLIENT":
						_ = tp.PrintfLine("220 relay.test ESMTP")
					case "DATA":
						data = true
						_ = tp.PrintfLine("354 go ahead")
					case "QUIT":
						_ = tp.PrintfLine("221 bye")
						return
					default:
						_ = tp.PrintfLine("250 ok")
					}
				}
			}(c)
		}
	}()
	return l.Addr().String(), func() string {
		mu.Lock()
		defer mu.Unlock()
		return strings.Join(seen, "|")
	}
}

func TestRelayXClient(t *testing.T) {
	addr, seen := scriptedRelay(t, true)
	acct := uuid.MustParse("6f1c2a5e-0d7b-4c8e-9a51-3e2f4b6d8c10")
	r := &smtpRelay{cfg: Config{RelayHost: addr, TLS: "opportunistic", XClient: true, Hostname: "client.test"}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actx := actor.With(ctx, actor.Actor{AccountID: acct, ClientIP: "2001:db8::7"})
	if err := r.Send(actx, "a@example.org", []string{"b@example.org"}, []byte("Subject: t\r\n\r\nhi\r\n")); err != nil {
		t.Fatal(err)
	}
	got := seen()
	want := "EHLO client.test|XCLIENT LOGIN=" + acct.String() + " ADDR=IPV6:2001:db8::7 NAME=[UNAVAILABLE]|EHLO client.test|MAIL FROM:<a@example.org>"
	if !strings.HasPrefix(got, want) || !strings.HasSuffix(got, "RCPT TO:<b@example.org>|DATA|QUIT") {
		t.Fatalf("session:\n%s\nwant prefix:\n%s", got, want)
	}
	// Without an account, and against a relay that does not offer XCLIENT, nothing is sent.
	if err := r.Send(ctx, "a@example.org", []string{"b@example.org"}, []byte("x\r\n")); err == nil {
		t.Fatal("XCLIENT without an account")
	}
	plain, _ := scriptedRelay(t, false)
	r.cfg.RelayHost = plain
	if err := r.Send(actx, "a@example.org", []string{"b@example.org"}, []byte("x\r\n")); !errors.Is(err, ErrNoXClient) {
		t.Fatalf("relay without XCLIENT: %v", err)
	}
	if x := xtext("a+b=c d"); x != "a+2Bb+3Dc+20d" {
		t.Fatalf("xtext: %q", x)
	}
}
