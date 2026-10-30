package proxyproto

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
)

func pipeConn(t *testing.T, payload []byte, mode Mode) (*Conn, error) {
	t.Helper()
	server, client := net.Pipe()
	go func() {
		_, _ = client.Write(payload)
		_, _ = client.Write([]byte("DATA"))
	}()
	return NewConn(server, mode)
}

func readData(t *testing.T, c net.Conn) string {
	t.Helper()
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatal(err)
	}
	return string(buf)
}

func TestV1(t *testing.T) {
	c, err := pipeConn(t, []byte("PROXY TCP6 2001:db8::1 2001:db8::2 51234 143\r\n"), Required)
	if err != nil {
		t.Fatal(err)
	}
	if IPOf(c.RemoteAddr()) != "2001:db8::1" || c.RemoteAddr().(*net.TCPAddr).Port != 51234 || IPOf(c.LocalAddr()) != "2001:db8::2" {
		t.Fatalf("addr: %v %v", c.RemoteAddr(), c.LocalAddr())
	}
	if readData(t, c) != "DATA" {
		t.Fatal("payload not preserved")
	}
	c, err = pipeConn(t, []byte("PROXY TCP4 10.0.0.1 10.0.0.2 1 2\r\n"), Optional)
	if err != nil || IPOf(c.RemoteAddr()) != "10.0.0.1" {
		t.Fatal(err)
	}
	if _, err := pipeConn(t, []byte("PROXY TCP4 garbage\r\n"), Optional); err == nil {
		t.Fatal("malformed accepted")
	}
}

func TestV2(t *testing.T) {
	hdr := append([]byte{}, v2Sig...)
	hdr = append(hdr, 0x21, 0x21) // PROXY, TCP over IPv6
	body := make([]byte, 36)
	copy(body[0:16], net.ParseIP("2001:db8::10").To16())
	copy(body[16:32], net.ParseIP("2001:db8::20").To16())
	binary.BigEndian.PutUint16(body[32:34], 40000)
	binary.BigEndian.PutUint16(body[34:36], 993)
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(body)))
	hdr = append(hdr, body...)
	c, err := pipeConn(t, hdr, Required)
	if err != nil {
		t.Fatal(err)
	}
	if IPOf(c.RemoteAddr()) != "2001:db8::10" || c.RemoteAddr().(*net.TCPAddr).Port != 40000 {
		t.Fatalf("addr: %v", c.RemoteAddr())
	}
	if readData(t, c) != "DATA" {
		t.Fatal("payload")
	}
	// LOCAL command keeps socket address.
	local := append([]byte{}, v2Sig...)
	local = append(local, 0x20, 0x00, 0x00, 0x00)
	c, err = pipeConn(t, local, Required)
	if err != nil {
		t.Fatal(err)
	}
	if c.RemoteAddr().String() != "pipe" {
		t.Fatalf("local cmd: %v", c.RemoteAddr())
	}
}

func TestRequiredWithoutHeader(t *testing.T) {
	if _, err := pipeConn(t, []byte("EHLO x\r\n"), Required); !errors.Is(err, ErrNoHeader) {
		t.Fatalf("expected ErrNoHeader, got %v", err)
	}
	c, err := pipeConn(t, []byte("EHLO x\r\n"), Optional)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	_, _ = io.ReadFull(c, buf)
	if string(buf) != "EHLO x\r\n" {
		t.Fatalf("optional passthrough: %q", buf)
	}
	if _, err := ParseMode("bogus"); err == nil {
		t.Fatal("bogus mode")
	}
}

func TestV2SSLTLV(t *testing.T) {
	build := func(tlvs []byte) []byte {
		hdr := append([]byte{}, v2Sig...)
		hdr = append(hdr, 0x21, 0x21)
		body := make([]byte, 36)
		copy(body[0:16], net.ParseIP("2001:db8::10").To16())
		copy(body[16:32], net.ParseIP("2001:db8::20").To16())
		body = append(body, tlvs...)
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(body)))
		return append(hdr, body...)
	}
	// An ALPN TLV, then PP2_TYPE_SSL with PP2_CLIENT_SSL and a version sub-TLV.
	ssl := []byte{0x01, 0x00, 0x04, 's', 'm', 't', 'p', 0x20, 0x00, 0x0c, 0x01, 0, 0, 0, 0, 0x21, 0x00, 0x04, 'T', 'L', 'S', '1'}
	c, err := pipeConn(t, build(ssl), Required)
	if err != nil {
		t.Fatal(err)
	}
	if !c.TLS() || !TLSOf(c) || readData(t, c) != "DATA" {
		t.Fatal("SSL TLV not seen")
	}
	for name, tlvs := range map[string][]byte{"none": nil, "not ssl": {0x20, 0x00, 0x05, 0x00, 0, 0, 0, 0}, "truncated": {0x20, 0x00, 0x09, 0x01}} {
		c, err := pipeConn(t, build(tlvs), Required)
		if err != nil {
			t.Fatal(name, err)
		}
		if c.TLS() {
			t.Fatalf("%s: TLS reported", name)
		}
	}
	if TLSOf(&net.TCPConn{}) {
		t.Fatal("plain conn reported TLS")
	}
}

func TestHeaderV2RoundTrip(t *testing.T) {
	src := &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 40001}
	dst := &net.TCPAddr{IP: net.ParseIP("2001:db8::2"), Port: 993}
	c, err := pipeConn(t, HeaderV2(src, dst, true), Required)
	if err != nil {
		t.Fatal(err)
	}
	if IPOf(c.RemoteAddr()) != "2001:db8::1" || c.RemoteAddr().(*net.TCPAddr).Port != 40001 || IPOf(c.LocalAddr()) != "2001:db8::2" || !c.TLS() {
		t.Fatalf("v6 round trip: %v %v tls=%v", c.RemoteAddr(), c.LocalAddr(), c.TLS())
	}
	if readData(t, c) != "DATA" {
		t.Fatal("payload not preserved")
	}
	c, err = pipeConn(t, HeaderV2(&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 5}, &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 143}, false), Required)
	if err != nil || IPOf(c.RemoteAddr()) != "10.1.2.3" || c.TLS() {
		t.Fatalf("v4 round trip: %v %v", c.RemoteAddr(), err)
	}
	// Mixed or non-TCP addresses: a LOCAL header, socket addresses kept.
	c, err = pipeConn(t, HeaderV2(&net.UnixAddr{Name: "/x", Net: "unix"}, dst, false), Required)
	if err != nil || c.RemoteAddr() == nil {
		t.Fatalf("local header: %v", err)
	}
	if readData(t, c) != "DATA" {
		t.Fatal("payload after local header not preserved")
	}
}
