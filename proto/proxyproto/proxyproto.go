// Package proxyproto wraps a net.Listener so accepted connections parse the
// HAProxy PROXY protocol (v1 and v2) and report the real client address.
// See README.md "Client IP passthrough".
package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Mode controls how the header is handled.
type Mode int

const (
	// Off: header not expected; connections are passed through untouched.
	Off Mode = iota
	// Optional: parse the header if present, otherwise use the socket
	// address. Costs OptionalHeaderTimeout before the banner on
	// server-speaks-first protocols; prefer Required behind a proxy and
	// Off without one.
	Optional
	// Required: reject connections without a valid header.
	Required
)

// ParseMode converts the config string.
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(s) {
	case "", "off":
		return Off, nil
	case "optional":
		return Optional, nil
	case "required":
		return Required, nil
	}
	return Off, fmt.Errorf("proxyproto: unknown mode %q", s)
}

// ErrNoHeader is returned (and the connection closed) when Required and
// no header was sent.
var ErrNoHeader = errors.New("proxyproto: PROXY header required but absent")

// HeaderTimeout bounds how long we wait for the header in Required mode.
var HeaderTimeout = 5 * time.Second

// OptionalHeaderTimeout bounds the wait in Optional mode. It must be short:
// server-speaks-first protocols (IMAP, POP3, LMTP) delay their banner by
// this much for every client that sends no header. A proxy sends the
// header immediately on connect, so a few hundred milliseconds is plenty.
var OptionalHeaderTimeout = 300 * time.Millisecond

var (
	v1Prefix = []byte("PROXY ")
	v2Sig    = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
)

// Listener wraps an inner listener.
type Listener struct {
	net.Listener
	Mode Mode
}

// Wrap returns a listener applying mode.
func Wrap(l net.Listener, mode Mode) net.Listener {
	if mode == Off {
		return l
	}
	return &Listener{Listener: l, Mode: mode}
}

// Accept accepts and parses the header. Parsing failures in Required mode
// close the connection and are reported as a temporary error so servers
// keep accepting.
func (l *Listener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		pc, err := NewConn(c, l.Mode)
		if err != nil {
			_ = c.Close()
			continue
		}
		return pc, nil
	}
}

// Conn is a connection with the real remote address.
type Conn struct {
	net.Conn
	r      *bufio.Reader
	remote net.Addr
	local  net.Addr
	tls    bool
}

// NewConn reads the PROXY header from c according to mode.
func NewConn(c net.Conn, mode Mode) (*Conn, error) {
	pc := &Conn{Conn: c, r: bufio.NewReaderSize(c, 4096), remote: c.RemoteAddr(), local: c.LocalAddr()}
	if mode == Off {
		return pc, nil
	}
	wait := HeaderTimeout
	if mode == Optional {
		wait = OptionalHeaderTimeout
	}
	_ = c.SetReadDeadline(time.Now().Add(wait))
	defer c.SetReadDeadline(time.Time{})
	peek, err := pc.r.Peek(12)
	if err != nil && (mode == Required || len(peek) == 0) {
		if mode == Required {
			return nil, ErrNoHeader
		}
		return pc, nil
	}
	switch {
	case bytes.Equal(peek[:min(len(peek), 12)], v2Sig[:min(len(peek), 12)]) && len(peek) == 12:
		if err := pc.parseV2(); err != nil {
			return nil, err
		}
	case bytes.HasPrefix(peek, v1Prefix):
		if err := pc.parseV1(); err != nil {
			return nil, err
		}
	default:
		if mode == Required {
			return nil, ErrNoHeader
		}
	}
	return pc, nil
}

func (pc *Conn) parseV1() error {
	line, err := pc.r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("proxyproto: v1 header: %w", err)
	}
	if !strings.HasSuffix(line, "\r\n") || len(line) > 108 {
		return errors.New("proxyproto: malformed v1 header")
	}
	f := strings.Fields(strings.TrimSuffix(line, "\r\n"))
	if len(f) < 2 {
		return errors.New("proxyproto: malformed v1 header")
	}
	switch f[1] {
	case "UNKNOWN":
		return nil
	case "TCP4", "TCP6":
		if len(f) != 6 {
			return errors.New("proxyproto: malformed v1 header")
		}
		sp, err1 := strconv.Atoi(f[4])
		dp, err2 := strconv.Atoi(f[5])
		sip, dip := net.ParseIP(f[2]), net.ParseIP(f[3])
		if err1 != nil || err2 != nil || sip == nil || dip == nil {
			return errors.New("proxyproto: malformed v1 addresses")
		}
		pc.remote = &net.TCPAddr{IP: sip, Port: sp}
		pc.local = &net.TCPAddr{IP: dip, Port: dp}
		return nil
	}
	return errors.New("proxyproto: unknown v1 family")
}

func (pc *Conn) parseV2() error {
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(pc.r, hdr); err != nil {
		return fmt.Errorf("proxyproto: v2 header: %w", err)
	}
	if hdr[12]>>4 != 2 {
		return errors.New("proxyproto: bad v2 version")
	}
	cmd := hdr[12] & 0x0f
	fam := hdr[13]
	n := int(binary.BigEndian.Uint16(hdr[14:16]))
	body := make([]byte, n)
	if _, err := io.ReadFull(pc.r, body); err != nil {
		return fmt.Errorf("proxyproto: v2 body: %w", err)
	}
	if cmd == 0 { // LOCAL: health checks from the proxy itself
		return nil
	}
	addrLen := 0
	switch fam >> 4 {
	case 1: // AF_INET
		if n < 12 {
			return errors.New("proxyproto: short v2 inet body")
		}
		addrLen = 12
		pc.remote = &net.TCPAddr{IP: net.IP(body[0:4]), Port: int(binary.BigEndian.Uint16(body[8:10]))}
		pc.local = &net.TCPAddr{IP: net.IP(body[4:8]), Port: int(binary.BigEndian.Uint16(body[10:12]))}
	case 2: // AF_INET6
		if n < 36 {
			return errors.New("proxyproto: short v2 inet6 body")
		}
		pc.remote = &net.TCPAddr{IP: net.IP(body[0:16]), Port: int(binary.BigEndian.Uint16(body[32:34]))}
		pc.local = &net.TCPAddr{IP: net.IP(body[16:32]), Port: int(binary.BigEndian.Uint16(body[34:36]))}
		addrLen = 36
	case 3: // UNIX: keep socket addresses
		addrLen = min(n, 216)
	case 0: // UNSPEC: keep socket addresses
	default:
		return errors.New("proxyproto: unknown v2 family")
	}
	pc.tls = sslTLV(body[addrLen:])
	return nil
}

// sslTLV reports whether the TLVs after the address block carry
// PP2_TYPE_SSL with PP2_CLIENT_SSL set: the client spoke TLS to the proxy
// (HAProxy send-proxy-v2-ssl).
func sslTLV(b []byte) bool {
	for len(b) >= 3 {
		typ, n := b[0], int(binary.BigEndian.Uint16(b[1:3]))
		if len(b) < 3+n {
			return false
		}
		if typ == 0x20 && n >= 1 && b[3]&0x01 != 0 {
			return true
		}
		b = b[3+n:]
	}
	return false
}

func (pc *Conn) Read(b []byte) (int, error) { return pc.r.Read(b) }
func (pc *Conn) RemoteAddr() net.Addr       { return pc.remote }
func (pc *Conn) LocalAddr() net.Addr        { return pc.local }

// TLS reports whether the proxy said the client connection was TLS (an
// SSL TLV in a v2 header).
func (pc *Conn) TLS() bool { return pc.tls }

// TLSOf reports whether c is a PROXY connection whose client spoke TLS to
// the proxy.
func TLSOf(c net.Conn) bool {
	pc, ok := c.(*Conn)
	return ok && pc.tls
}

// ProxyAddr returns the address of the proxy itself (the socket peer).
func (pc *Conn) ProxyAddr() net.Addr { return pc.Conn.RemoteAddr() }

// IPOf extracts the IP string from a net.Addr (without port/zone).
func IPOf(a net.Addr) string {
	switch v := a.(type) {
	case *net.TCPAddr:
		return v.IP.String()
	case *net.UDPAddr:
		return v.IP.String()
	case *net.UnixAddr:
		return "unix"
	}
	if a == nil {
		return ""
	}
	h, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return a.String()
	}
	return h
}
