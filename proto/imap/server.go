// Package imap is the IMAP4rev1 protocol adapter (RFC 3501 plus IDLE,
// UIDPLUS, MOVE, SPECIAL-USE, NAMESPACE, UNSELECT, ENABLE, SASL-IR, LITERAL+,
// AUTH=PLAIN/OAUTHBEARER/XOAUTH2) on top of github.com/emersion/go-imap/v2's
// imapserver. It only talks to the mailbox, delivery and auth services; TLS
// is terminated by HAProxy in front (PROXY protocol gives the client IP).
package imap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"

	"kittiwake/metrics"
	"kittiwake/proto/proxyproto"
	authsvc "kittiwake/service/auth"
	"kittiwake/service/delivery"
	"kittiwake/service/mailbox"
	"kittiwake/store/storage"
)

// Config for the server.
type Config struct {
	Hostname       string
	MaxLiteralSize int64
	IdleTimeout    time.Duration
	// CompatPrefix and CompatDelim advertise a second personal namespace
	// that maps onto the same folders, for clients still configured for a
	// Dovecot or Courier layout such as prefix "INBOX." with delimiter ".":
	// "INBOX.Projects.2026" is "Projects/2026". Empty = off.
	CompatPrefix string
	CompatDelim  string
}

// Server is the IMAP server.
type Server struct {
	cfg  Config
	auth *authsvc.Service
	mb   *mailbox.Service
	del  *delivery.Service
	log  *slog.Logger
	srv  *imapserver.Server
}

// New returns a server.
func New(cfg Config, authSvc *authsvc.Service, mb *mailbox.Service, del *delivery.Service, log *slog.Logger) *Server {
	if cfg.Hostname == "" {
		cfg.Hostname = "kittiwake"
	}
	if cfg.MaxLiteralSize <= 0 {
		cfg.MaxLiteralSize = 50 << 20
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 29 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, auth: authSvc, mb: mb, del: del, log: log}
	caps := imap.CapSet{
		imap.CapIMAP4rev1: {}, imap.CapIdle: {}, imap.CapUIDPlus: {}, imap.CapMove: {}, imap.CapSpecialUse: {},
		imap.CapNamespace: {}, imap.CapUnselect: {}, imap.CapEnable: {}, imap.CapSASLIR: {}, imap.CapLiteralPlus: {},
		imap.CapListExtended: {}, imap.CapChildren: {},
		imap.AuthCap(saslPlain): {}, imap.AuthCap(saslOAuthBearer): {}, imap.AuthCap(saslXOAuth2): {},
	}
	s.srv = imapserver.New(&imapserver.Options{
		NewSession:   s.newSession,
		Caps:         caps,
		Logger:       slogLogger{log},
		InsecureAuth: true, // TLS is terminated upstream by HAProxy
	})
	return s
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Printf(format string, v ...any) { s.l.Warn(fmt.Sprintf(format, v...)) }

// Serve accepts connections on l.
func (s *Server) Serve(l net.Listener) error {
	err := s.srv.Serve(l)
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Shutdown closes the server (go-imap has no graceful drain; connections are closed).
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Close() }

// Close closes the server.
func (s *Server) Close() error { return s.srv.Close() }

func (s *Server) newSession(c *imapserver.Conn) (imapserver.Session, *imapserver.GreetingData, error) {
	metrics.Connections.WithLabelValues("imap").Inc()
	sess := &session{srv: s, conn: c, clientIP: proxyproto.IPOf(c.NetConn().RemoteAddr()), ctx: context.Background()}
	return sess, &imapserver.GreetingData{}, nil
}

// ---- error helpers ----

func noErr(code imap.ResponseCode, text string) error {
	return &imap.Error{Type: imap.StatusResponseTypeNo, Code: code, Text: text}
}

func badErr(text string) error {
	return &imap.Error{Type: imap.StatusResponseTypeBad, Text: text}
}

// mapErr translates service errors to IMAP status responses.
func (s *Server) mapErr(err error, what string) error {
	var ie *imap.Error
	if errors.As(err, &ie) {
		return err
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mailbox.ErrNotFound):
		return noErr(imap.ResponseCodeNonExistent, what+": no such mailbox or message")
	case errors.Is(err, mailbox.ErrHold):
		return noErr(imap.ResponseCodeNoPerm, "Mailbox is on legal hold (read-only)")
	case errors.Is(err, mailbox.ErrSealed):
		return noErr(imap.ResponseCodeUnavailable, "Mailbox sealed")
	case errors.Is(err, mailbox.ErrForbidden):
		return noErr(imap.ResponseCodeNoPerm, "Permission denied")
	case errors.Is(err, mailbox.ErrNotEmpty):
		return noErr(imap.ResponseCodeHasChildren, "Mailbox has children")
	case errors.Is(err, mailbox.ErrInvalid):
		return noErr(imap.ResponseCodeCannot, what+": "+err.Error())
	case errors.Is(err, delivery.ErrOverQuota):
		return noErr(imap.ResponseCodeOverQuota, "Mailbox over quota")
	case errors.Is(err, delivery.ErrTooLarge):
		return noErr(imap.ResponseCodeTooBig, "Message too large")
	case errors.Is(err, delivery.ErrHold):
		return noErr(imap.ResponseCodeNoPerm, "Mailbox is on legal hold (read-only)")
	case errors.Is(err, delivery.ErrMailboxDisabled):
		return noErr(imap.ResponseCodeNoPerm, "Mailbox disabled")
	case errors.Is(err, delivery.ErrNoFolder):
		return noErr(imap.ResponseCodeTryCreate, "No such mailbox")
	case errors.Is(err, storage.ErrUnavailable), errors.Is(err, delivery.ErrUnavailable):
		s.log.Warn("imap: mailbox storage unavailable", slog.String("op", what), slog.String("error", err.Error()))
		return noErr(imap.ResponseCodeUnavailable, "Mailbox storage temporarily unavailable, try again later")
	}
	s.log.Error("imap: internal error", slog.String("op", what), slog.String("error", err.Error()))
	return noErr(imap.ResponseCodeServerBug, what+" failed")
}
