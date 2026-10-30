package proxy

import (
	"bufio"
	"io"
	"net"
	"strings"
	"time"

	"kittiwake/proto/proxyproto"
)

// lineConn is a client or backend connection read line by line during
// the login phase and spliced afterwards.
type lineConn struct {
	net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

const maxLine = 8192

func newLineConn(c net.Conn) *lineConn {
	return &lineConn{Conn: c, r: bufio.NewReaderSize(c, maxLine), w: bufio.NewWriterSize(c, 4096)}
}

// readLine returns one CRLF (or LF) terminated line without the ending.
func (c *lineConn) readLine() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		if line == "" {
			return "", err
		}
		if err != io.EOF {
			return "", err
		}
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// read reads exactly n bytes (an IMAP literal).
func (c *lineConn) read(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(c.r, b)
	return b, err
}

// send writes a line with CRLF and flushes.
func (c *lineConn) send(line string) error {
	if _, err := c.w.WriteString(line + "\r\n"); err != nil {
		return err
	}
	return c.w.Flush()
}

// write writes raw bytes and flushes.
func (c *lineConn) write(b []byte) error {
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *lineConn) deadline(d time.Duration) {
	if d <= 0 {
		_ = c.SetDeadline(time.Time{})
		return
	}
	_ = c.SetDeadline(time.Now().Add(d))
}

// splice copies both directions until one side ends, then closes both.
// Bytes buffered during the login phase are flushed first.
func splice(a, b *lineConn) {
	a.deadline(0)
	b.deadline(0)
	done := make(chan struct{}, 2)
	copy := func(dst, src *lineConn) {
		_, _ = io.Copy(dst.Conn, src.r)
		closeWrite(dst.Conn)
		done <- struct{}{}
	}
	go copy(b, a)
	go copy(a, b)
	<-done
	// Give the other direction a moment to drain (a LOGOUT reply), then cut.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	_ = a.Close()
	_ = b.Close()
}

func closeWrite(c net.Conn) {
	type cw interface{ CloseWrite() error }
	switch v := c.(type) {
	case cw:
		_ = v.CloseWrite()
	case *proxyproto.Conn:
		if t, ok := v.Conn.(cw); ok {
			_ = t.CloseWrite()
		}
	}
}

func tlsOf(c net.Conn) bool { return proxyproto.TLSOf(c) }

func ipOf(a net.Addr) string { return proxyproto.IPOf(a) }
