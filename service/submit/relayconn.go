package submit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"time"

	"github.com/emersion/go-smtp"
	"github.com/google/uuid"

	"kittiwake/service/actor"
)

// ErrNoXClient: smtp.xclient is on and the relay does not offer XCLIENT
// to this host (Postfix smtpd_authorized_xclient_hosts).
var ErrNoXClient = errors.New("submit: relay does not offer XCLIENT")

// connect opens a relay session: greeting, EHLO, STARTTLS per the TLS mode
// and, with XClient, XCLIENT carrying the sending account as LOGIN and the
// client address as ADDR (https://www.postfix.org/XCLIENT_README.html).
// go-smtp has neither XCLIENT nor a way to set the EHLO name before its own
// STARTTLS (it would greet as "localhost"), so the handshake is done here
// and the connection handed to go-smtp with a 220 reply replayed as the
// greeting it reads first; go-smtp then sends EHLO with cfg.Hostname.
func (r *smtpRelay) connect(ctx context.Context, a actor.Actor) (*smtp.Client, error) {
	if r.cfg.XClient && a.AccountID == uuid.Nil {
		return nil, errors.New("submit: XCLIENT needs an account")
	}
	conn, err := r.dial(ctx)
	if err != nil {
		return nil, err
	}
	mode := r.mode()
	if mode == "off" && !r.cfg.XClient {
		return smtp.NewClient(conn), nil
	}
	deadline := time.Now().Add(time.Minute)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	fail := func(err error) (*smtp.Client, error) {
		_ = conn.Close()
		return nil, err
	}
	tp := textproto.NewConn(conn)
	_, greeting, err := tp.ReadResponse(220)
	if err != nil {
		return fail(err)
	}
	ext, err := ehlo(tp, r.cfg.Hostname)
	if err != nil {
		return fail(err)
	}
	if mode != "off" {
		if _, ok := ext["STARTTLS"]; ok {
			if _, err := command(tp, 220, "STARTTLS"); err != nil {
				return fail(err)
			}
			tc := tls.Client(conn, r.tlsConfig())
			if err := tc.HandshakeContext(ctx); err != nil {
				return fail(err)
			}
			conn = tc
			tp = textproto.NewConn(conn)
			if r.cfg.XClient { // the extensions after STARTTLS decide
				if ext, err = ehlo(tp, r.cfg.Hostname); err != nil {
					return fail(err)
				}
			}
		} else if mode == "required" {
			return fail(ErrNoStartTLS)
		}
	}
	if r.cfg.XClient {
		if _, ok := ext["XCLIENT"]; !ok {
			return fail(ErrNoXClient)
		}
		if greeting, err = command(tp, 220, "XCLIENT "+xclientArgs(a)); err != nil {
			return fail(fmt.Errorf("XCLIENT: %w", err))
		}
	}
	_ = conn.SetDeadline(time.Time{})
	first, _, _ := strings.Cut(greeting, "\n")
	return smtp.NewClient(&replayConn{Conn: conn, r: io.MultiReader(strings.NewReader("220 "+first+"\r\n"), tp.R)}), nil
}

// xclientArgs: LOGIN is the account UUID (Postfix sasl_username), ADDR the
// real client address, NAME unavailable so Postfix logs "unknown[addr]"
// instead of Kittiwake's own name.
func xclientArgs(a actor.Actor) string {
	addr := "[UNAVAILABLE]"
	if ip := net.ParseIP(a.ClientIP); ip != nil {
		addr = ip.String()
		if ip.To4() == nil {
			addr = "IPV6:" + addr
		}
	}
	return "LOGIN=" + xtext(a.AccountID.String()) + " ADDR=" + xtext(addr) + " NAME=[UNAVAILABLE]"
}

// xtext encodes an XCLIENT attribute value (RFC 3461 xtext).
func xtext(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '!' || c > '~' || c == '+' || c == '=' {
			fmt.Fprintf(&b, "+%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func command(tp *textproto.Conn, expect int, cmd string) (string, error) {
	if err := tp.PrintfLine("%s", cmd); err != nil {
		return "", err
	}
	_, msg, err := tp.ReadResponse(expect)
	return msg, err
}

// ehlo sends EHLO and returns the advertised extensions.
func ehlo(tp *textproto.Conn, name string) (map[string]string, error) {
	msg, err := command(tp, 250, "EHLO "+name)
	if err != nil {
		return nil, err
	}
	ext := map[string]string{}
	lines := strings.Split(msg, "\n")
	for _, l := range lines[1:] {
		k, v, _ := strings.Cut(l, " ")
		ext[strings.ToUpper(k)] = v
	}
	return ext, nil
}

// replayConn serves reads from r (bytes already buffered, then the
// connection) so go-smtp can take over a session mid-way.
type replayConn struct {
	net.Conn
	r io.Reader
}

func (c *replayConn) Read(b []byte) (int, error) { return c.r.Read(b) }
