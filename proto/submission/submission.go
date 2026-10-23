// Package submission is the SMTP message submission adapter (RFC 6409)
// on top of github.com/emersion/go-smtp. HAProxy terminates implicit TLS
// on port 465 (RFC 8314) and forwards with PROXY v2; the daemon never
// speaks TLS itself. Clients authenticate (SASL PLAIN, LOGIN, OAUTHBEARER,
// XOAUTH2) and every message goes through the submit service, the same
// path as JMAP EmailSubmission. See README.md "Sending mail".
package submission

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
	"github.com/google/uuid"

	"kittiwake/metrics"
	"kittiwake/proto/proxyproto"
	"kittiwake/service/actor"
	authsvc "kittiwake/service/auth"
	"kittiwake/service/submit"
	"kittiwake/store/identity"
)

// Protocol is the name in audit records, logs and metrics.
const Protocol = "submission"

const serviceTimeout = 30 * time.Second

// Config for the server.
type Config struct {
	Hostname       string
	MaxMessageSize int64
	MaxRecipients  int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	// SaveToSent stores a Sent copy of every message (off by default:
	// SMTP clients save their own over IMAP).
	SaveToSent bool
	// RequireTLSProxy offers AUTH only when the PROXY v2 header says the
	// client connection to the proxy was TLS.
	RequireTLSProxy bool
	// SendTimeout bounds one submission including the relay hop.
	SendTimeout time.Duration
}

// Server is the submission server.
type Server struct {
	srv  *smtp.Server
	auth *authsvc.Service
	sub  *submit.Service
	log  *slog.Logger
	cfg  Config
}

// New returns a server.
func New(cfg Config, auth *authsvc.Service, sub *submit.Service, log *slog.Logger) *Server {
	if cfg.Hostname == "" {
		cfg.Hostname = "kittiwake"
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
	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = 3 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Server{auth: auth, sub: sub, log: log, cfg: cfg}
	s.srv = smtp.NewServer(s)
	s.srv.Domain = cfg.Hostname
	s.srv.MaxMessageBytes = cfg.MaxMessageSize
	s.srv.MaxRecipients = cfg.MaxRecipients
	s.srv.ReadTimeout = cfg.ReadTimeout
	s.srv.WriteTimeout = cfg.WriteTimeout
	s.srv.EnableSMTPUTF8 = true
	// TLS is HAProxy's; whether AUTH is offered is decided per connection
	// from the PROXY header (RequireTLSProxy).
	s.srv.AllowInsecureAuth = true
	s.srv.ErrorLog = slogLogger{log}
	return s
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Printf(format string, v ...any) { s.l.Warn(fmt.Sprintf(format, v...)) }
func (s slogLogger) Println(v ...any)               { s.l.Warn(fmt.Sprint(v...)) }

// Serve accepts connections on l (wrap it with proxyproto first).
func (s *Server) Serve(l net.Listener) error { return s.srv.Serve(l) }

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Close stops the server immediately.
func (s *Server) Close() error { return s.srv.Close() }

// NewSession implements smtp.Backend.
func (s *Server) NewSession(c *smtp.Conn) (smtp.Session, error) {
	metrics.Connections.WithLabelValues(Protocol).Inc()
	return &session{
		srv:      s,
		clientIP: proxyproto.IPOf(c.Conn().RemoteAddr()),
		tls:      !s.cfg.RequireTLSProxy || proxyproto.TLSOf(c.Conn()),
	}, nil
}

type session struct {
	srv      *Server
	clientIP string
	tls      bool
	act      *actor.Actor
	from     string
	rcpts    []string
}

var (
	errAuthRequired = &smtp.SMTPError{Code: 530, EnhancedCode: smtp.EnhancedCode{5, 7, 0}, Message: "Authentication required"}
	errEncryption   = &smtp.SMTPError{Code: 538, EnhancedCode: smtp.EnhancedCode{5, 7, 11}, Message: "Encryption required; connect with TLS on port 465"}
	errTOTP         = &smtp.SMTPError{Code: 535, EnhancedCode: smtp.EnhancedCode{5, 7, 8}, Message: "This password requires a one-time code; use an application password or OAUTHBEARER"}
	errDisabled     = &smtp.SMTPError{Code: 535, EnhancedCode: smtp.EnhancedCode{5, 7, 8}, Message: "Account disabled"}
	errNoSend       = &smtp.SMTPError{Code: 535, EnhancedCode: smtp.EnhancedCode{5, 7, 8}, Message: "Account may not send mail"}
	errAuthTemp     = &smtp.SMTPError{Code: 454, EnhancedCode: smtp.EnhancedCode{4, 7, 0}, Message: "Temporary authentication failure"}
	errNullSender   = &smtp.SMTPError{Code: 550, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "Null sender not accepted"}
	errBadSender    = &smtp.SMTPError{Code: 550, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "Sender address not owned by the authenticated account"}
	errTooLarge     = &smtp.SMTPError{Code: 552, EnhancedCode: smtp.EnhancedCode{5, 3, 4}, Message: "Message too large"}
	errTemp         = &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0}, Message: "Temporary failure, try again later"}
	errRelayDown    = &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 4, 1}, Message: "Relay unavailable, try again later"}
	errTimeout      = &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 4, 2}, Message: "Backend timeout"}
	errNotPermitted = &smtp.SMTPError{Code: 550, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "Sending not permitted for this mailbox"}
	errNoRecipients = &smtp.SMTPError{Code: 554, EnhancedCode: smtp.EnhancedCode{5, 5, 1}, Message: "No valid recipients"}
)

func (ss *session) Reset() { ss.from, ss.rcpts = "", nil }

func (ss *session) Logout() error {
	metrics.Connections.WithLabelValues(Protocol).Dec()
	return nil
}

// AuthMechanisms is empty, so EHLO advertises no AUTH, on a connection
// that did not arrive over TLS when RequireTLSProxy is set.
func (ss *session) AuthMechanisms() []string {
	if !ss.tls {
		return nil
	}
	return []string{sasl.Plain, sasl.Login, sasl.OAuthBearer, "XOAUTH2"}
}

// Auth implements smtp.AuthSession.
func (ss *session) Auth(mech string) (sasl.Server, error) {
	if !ss.tls {
		return nil, errEncryption
	}
	switch strings.ToUpper(mech) {
	case sasl.Plain:
		return sasl.NewPlainServer(func(identity, username, password string) error {
			if identity != "" && !strings.EqualFold(identity, username) {
				return smtp.ErrAuthFailed // no proxy authentication
			}
			return ss.password(username, password)
		}), nil
	case sasl.Login:
		return &loginServer{auth: ss.password}, nil
	case sasl.OAuthBearer:
		return sasl.NewOAuthBearerServer(func(opts sasl.OAuthBearerOptions) *sasl.OAuthBearerError {
			if err := ss.bearer(opts.Username, opts.Token); err != nil {
				return &sasl.OAuthBearerError{Status: "invalid_token", Schemes: "bearer"}
			}
			return nil
		}), nil
	case "XOAUTH2":
		return &xoauth2Server{auth: ss.bearer}, nil
	}
	return nil, smtp.ErrAuthUnknownMechanism
}

func (ss *session) password(username, password string) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceTimeout)
	defer cancel()
	a, err := ss.srv.auth.VerifyPassword(ctx, username, password, Protocol, ss.clientIP)
	return ss.authenticated(a, err)
}

func (ss *session) bearer(username, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceTimeout)
	defer cancel()
	a, err := ss.srv.auth.VerifyOAuthBearer(ctx, username, token, Protocol, ss.clientIP)
	return ss.authenticated(a, err)
}

// authenticated binds the actor. Sending needs the send role, so an
// account without it cannot log in here at all.
func (ss *session) authenticated(a actor.Actor, err error) error {
	if err != nil {
		switch {
		case errors.Is(err, authsvc.ErrTOTPRequired):
			return errTOTP
		case errors.Is(err, authsvc.ErrAccountDisabled):
			return errDisabled
		case errors.Is(err, authsvc.ErrInvalidCredentials), errors.Is(err, authsvc.ErrInvalidToken), errors.Is(err, authsvc.ErrTokenExpired):
			return smtp.ErrAuthFailed
		}
		ss.srv.log.Warn("submission auth backend error", slog.String("client_ip", ss.clientIP), slog.String("error", err.Error()))
		return errAuthTemp
	}
	if a.MailboxID == uuid.Nil || !a.HasRole(identity.RoleSend) {
		ss.srv.log.Info("submission login without send role", slog.String("account", a.Username), slog.String("client_ip", ss.clientIP))
		return errNoSend
	}
	a.Protocol, a.ClientIP = Protocol, ss.clientIP
	ss.act = &a
	return nil
}

func (ss *session) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(actor.With(context.Background(), *ss.act), ss.srv.cfg.SendTimeout)
}

// Mail checks the envelope sender before the client uploads anything.
func (ss *session) Mail(from string, opts *smtp.MailOptions) error {
	if ss.act == nil {
		return errAuthRequired
	}
	if from == "" {
		return errNullSender
	}
	if opts != nil && opts.Size > ss.srv.cfg.MaxMessageSize {
		return errTooLarge
	}
	ctx, cancel := ss.ctx()
	defer cancel()
	if err := ss.srv.sub.CheckSender(ctx, ss.act.MailboxID, from); err != nil {
		if errors.Is(err, submit.ErrBadSender) {
			ss.srv.log.Info("submission sender refused", slog.String("account", ss.act.Username), slog.String("from", from), slog.String("client_ip", ss.clientIP))
			return errBadSender
		}
		return ss.smtpErr(err)
	}
	ss.from = from
	return nil
}

// Rcpt collects recipients; the submit service validates them and the
// relay decides whether it accepts them.
func (ss *session) Rcpt(to string, _ *smtp.RcptOptions) error {
	if ss.act == nil {
		return errAuthRequired
	}
	ss.rcpts = append(ss.rcpts, to)
	return nil
}

// Data relays the message synchronously: the client gets 250 only once
// the relay accepted it.
func (ss *session) Data(r io.Reader) error {
	if ss.act == nil {
		return errAuthRequired
	}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r, ss.srv.cfg.MaxMessageSize+1))
	if err != nil {
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0}, Message: "Read error"}
	}
	if n > ss.srv.cfg.MaxMessageSize {
		return errTooLarge
	}
	ctx, cancel := ss.ctx()
	defer cancel()
	save := ss.srv.cfg.SaveToSent
	// The submit service audits and logs every attempt with its Message-ID.
	if _, err := ss.srv.sub.Send(ctx, submit.Request{MailboxID: ss.act.MailboxID, Raw: buf.Bytes(), From: ss.from, To: ss.rcpts, SaveToSent: &save}); err != nil {
		return ss.smtpErr(err)
	}
	return nil
}

func (ss *session) smtpErr(err error) error {
	var se *smtp.SMTPError
	switch {
	case errors.Is(err, submit.ErrRelay) && errors.As(err, &se):
		return se // the relay's own reply
	case errors.Is(err, submit.ErrRelay):
		ss.srv.log.Warn("submission relay failed", slog.String("account", ss.act.Username), slog.String("error", err.Error()))
		return errRelayDown
	case errors.Is(err, submit.ErrBadSender):
		return errBadSender
	case errors.Is(err, submit.ErrForbidden), errors.Is(err, submit.ErrHold):
		return errNotPermitted
	case errors.Is(err, submit.ErrTooLarge):
		return errTooLarge
	case errors.Is(err, submit.ErrNoRecipients):
		return errNoRecipients
	case errors.Is(err, submit.ErrInvalid):
		return &smtp.SMTPError{Code: 554, EnhancedCode: smtp.EnhancedCode{5, 6, 0}, Message: strings.TrimPrefix(err.Error(), "submit: ")}
	case errors.Is(err, context.DeadlineExceeded):
		return errTimeout
	}
	ss.srv.log.Warn("submission failed", slog.String("account", ss.act.Username), slog.String("error", err.Error()))
	return errTemp
}

var _ smtp.AuthSession = (*session)(nil)
