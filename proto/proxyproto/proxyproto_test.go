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
