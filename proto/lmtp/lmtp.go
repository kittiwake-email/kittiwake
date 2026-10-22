// Package lmtp is the inbound delivery adapter: an LMTP server (RFC 2033)
// on top of github.com/emersion/go-smtp that hands each recipient to the
// delivery service and answers per recipient. See README.md "LMTP delivery".
package lmtp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/emersion/go-smtp"

	"dovetail/proto/proxyproto"
	"dovetail/service/delivery"
)

// Config for the server.
type Config struct {
	Hostname       string
	MaxMessageSize int64
	MaxRecipients  int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

// Server is the LMTP server.
type Server struct {
	srv *smtp.Server
	del *delivery.Service
	log *slog.Logger
	cfg Config
}

// New returns a server.
func New(cfg Config, del *delivery.Service, log *slog.Logger) *Server {
	if cfg.Hostname == "" {
		cfg.Hostname = "dovetail"
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 50 << 20
	}
	if cfg.MaxRecipients <= 0 {
		cfg.MaxRecipients = 100
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Minute
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Server{del: del, log: log, cfg: cfg}
	s.srv = smtp.NewServer(s)
	s.srv.LMTP = true
	s.srv.Domain = cfg.Hostname
	s.srv.MaxMessageBytes = cfg.MaxMessageSize
	s.srv.MaxRecipients = cfg.MaxRecipients
	s.srv.ReadTimeout = cfg.ReadTimeout
	s.srv.WriteTimeout = cfg.WriteTimeout
	s.srv.EnableSMTPUTF8 = true
	s.srv.ErrorLog = slogLogger{log}
	return s
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Printf(format string, v ...any) { s.l.Warn(fmt.Sprintf(format, v...)) }
func (s slogLogger) Println(v ...any)               { s.l.Warn(fmt.Sprint(v...)) }

// Serve accepts connections on l (wrap it with proxyproto first if needed).
func (s *Server) Serve(l net.Listener) error { return s.srv.Serve(l) }

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Close stops the server immediately.
func (s *Server) Close() error { return s.srv.Close() }

// NewSession implements smtp.Backend.
func (s *Server) NewSession(c *smtp.Conn) (smtp.Session, error) {
	return &session{srv: s, clientIP: proxyproto.IPOf(c.Conn().RemoteAddr())}, nil
}

type session struct {
	srv      *Server
	clientIP string
	from     string
	rcpts    []string
}

func (ss *session) Reset() { ss.from, ss.rcpts = "", nil }

func (ss *session) Logout() error { return nil }

func (ss *session) Mail(from string, opts *smtp.MailOptions) error {
	if opts != nil && opts.Size > ss.srv.cfg.MaxMessageSize {
		return &smtp.SMTPError{Code: 552, EnhancedCode: smtp.EnhancedCode{5, 3, 4}, Message: "Message too large"}
	}
	ss.from = from
	return nil
}

// Rcpt validates the recipient up front so Postfix learns about unknown
// users and full mailboxes before sending the body.
func (ss *session) Rcpt(to string, _ *smtp.RcptOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := ss.srv.del.CheckRecipient(ctx, to, 0); err != nil {
		return ss.smtpErr(err)
	}
	ss.rcpts = append(ss.rcpts, to)
	return nil
}

// Data is used when the client speaks plain SMTP semantics; LMTPData is
// what Postfix uses. Both deliver to every recipient.
func (ss *session) Data(r io.Reader) error {
	raw, err := ss.read(r)
	if err != nil {
		return err
	}
	var first error
	for _, rcpt := range ss.rcpts {
		if err := ss.deliver(rcpt, raw); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// LMTPData delivers per recipient and reports each status.
func (ss *session) LMTPData(r io.Reader, sc smtp.StatusCollector) error {
	raw, err := ss.read(r)
	if err != nil {
		for _, rcpt := range ss.rcpts {
			sc.SetStatus(rcpt, err)
		}
		return nil
	}
	for _, rcpt := range ss.rcpts {
		sc.SetStatus(rcpt, ss.deliver(rcpt, raw))
	}
	return nil
}

func (ss *session) read(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r, ss.srv.cfg.MaxMessageSize+1))
	if err != nil {
		return nil, &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0}, Message: "Read error"}
	}
	if n > ss.srv.cfg.MaxMessageSize {
		return nil, &smtp.SMTPError{Code: 552, EnhancedCode: smtp.EnhancedCode{5, 3, 4}, Message: "Message too large"}
	}
	return buf.Bytes(), nil
}

func (ss *session) deliver(rcpt string, raw []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// The delivery service logs each stored copy with its Message-ID.
	if _, err := ss.srv.del.Deliver(ctx, delivery.Request{Recipient: rcpt, Sender: ss.from, Raw: raw, ClientIP: ss.clientIP}); err != nil {
		ss.srv.log.WarnContext(ctx, "delivery failed", slog.String("rcpt", rcpt), slog.String("from", ss.from), slog.String("client_ip", ss.clientIP), slog.String("error", err.Error()))
		return ss.smtpErr(err)
	}
	return nil
}

func (ss *session) smtpErr(err error) error {
	code, enh, text := ss.srv.del.Status(err)
	var e smtp.EnhancedCode
	_, _ = fmt.Sscanf(enh, "%d.%d.%d", &e[0], &e[1], &e[2])
	if errors.Is(err, context.DeadlineExceeded) {
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 4, 2}, Message: "Backend timeout"}
	}
	return &smtp.SMTPError{Code: code, EnhancedCode: e, Message: text}
}

var _ smtp.LMTPSession = (*session)(nil)
