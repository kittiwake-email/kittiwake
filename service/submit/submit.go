// Package submit sends outbound mail: it validates the sender identity,
// relays the message to the configured SMTP host and stores a copy in the
// Sent folder. JMAP EmailSubmission is a thin adapter over it. See
// README.md "Sending mail".
package submit

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/mail"
	"strings"
	"time"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
	"github.com/google/uuid"

	"dovetail/metrics"
	"dovetail/service/actor"
	"dovetail/service/auditor"
	"dovetail/service/delivery"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
)

// Errors.
var (
	ErrForbidden    = errors.New("submit: forbidden")
	ErrHold         = errors.New("submit: mailbox on hold, sending disabled")
	ErrBadSender    = errors.New("submit: From address is not one of the account's identities")
	ErrNoRecipients = errors.New("submit: no recipients")
	ErrTooLarge     = errors.New("submit: message too large")
	ErrRelay        = errors.New("submit: relay refused the message")
	ErrInvalid      = errors.New("submit: invalid message")
)

// Config for the service.
type Config struct {
	RelayHost      string // host:port of the SMTP relay (Postfix)
	Username       string // optional SASL PLAIN credentials
	Password       string
	StartTLS       bool
	Hostname       string // EHLO name and Received: host
	MaxMessageSize int64
	Timeout        time.Duration
	MaxRecipients  int
}

// Relay delivers a message to the MTA. The default implementation speaks
// SMTP; tests inject their own.
type Relay interface {
	Send(ctx context.Context, from string, to []string, data []byte) error
}

// Service is the submit service.
type Service struct {
	ids   *identity.Store
	idx   *index.Store
	del   *delivery.Service
	audit *auditor.Auditor
	relay Relay
	log   *slog.Logger
	cfg   Config
	Now   func() time.Time
}

// New returns the service. relay may be nil to use the SMTP relay from cfg.
func New(cfg Config, ids *identity.Store, idx *index.Store, del *delivery.Service, aud *auditor.Auditor, relay Relay, log *slog.Logger) *Service {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Minute
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 50 << 20
	}
	if cfg.MaxRecipients <= 0 {
		cfg.MaxRecipients = 100
	}
	if cfg.Hostname == "" {
		cfg.Hostname = "dovetail"
	}
	if log == nil {
		log = slog.Default()
	}
	if relay == nil {
		relay = &smtpRelay{cfg: cfg}
	}
	return &Service{ids: ids, idx: idx, del: del, audit: aud, relay: relay, log: log, cfg: cfg, Now: time.Now}
}

// Identity is an address the mailbox may send as.
type Identity struct {
	ID    string // stable id: the address itself
	Email string
	Name  string
}

// Identities lists the addresses the acting account may use as From: its
// own username and the aliases that deliver to its mailbox (catch-alls
// excluded), all carrying the account's display name. Other accounts
// attached to the same mailbox are not identities of this one. An admin
// inspecting a mailbox sees the identities of every account on it.
func (s *Service) Identities(ctx context.Context, mb uuid.UUID) ([]Identity, error) {
	a := actor.From(ctx)
	if !(a.MailboxID == mb || a.IsAdminReader()) {
		return nil, ErrForbidden
	}
	var out []Identity
	seen := map[string]bool{}
	add := func(addr, name string) {
		if addr == "" || strings.HasPrefix(addr, "@") || seen[addr] {
			return
		}
		seen[addr] = true
		out = append(out, Identity{ID: addr, Email: addr, Name: name})
	}
	name := ""
	if a.AccountID != uuid.Nil {
		acct, err := s.ids.GetAccountByUsernameCached(ctx, a.Username)
		if err != nil {
			return nil, err
		}
		name = acct.Name
		add(acct.Username, name)
	} else {
		users, _, err := s.ids.ListMailboxAccounts(ctx, mb, "", 1000)
		if err != nil {
			return nil, err
		}
		for _, u := range users {
			add(u, "")
		}
	}
	aliases, err := s.ids.ListMailboxAliases(ctx, mb)
	if err != nil {
		return nil, err
	}
	for _, al := range aliases {
		add(al, name)
	}
	return out, nil
}

// SetName changes the acting account's display name (the From: name of
// all its identities).
func (s *Service) SetName(ctx context.Context, name string) (identity.Account, error) {
	a := actor.From(ctx)
	if a.AccountID == uuid.Nil {
		return identity.Account{}, ErrForbidden
	}
	name = strings.TrimSpace(name)
	if len(name) > 256 || strings.ContainsAny(name, "\r\n") {
		return identity.Account{}, fmt.Errorf("%w: name", ErrInvalid)
	}
	acct, err := s.ids.UpdateAccount(ctx, a.AccountID, func(x *identity.Account) error { x.Name = name; return nil })
	if err != nil {
		return identity.Account{}, err
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: acct.MailboxID, Action: "name-change", Detail: name})
	return acct, nil
}

// Request is an outbound submission.
type Request struct {
	MailboxID uuid.UUID
	Raw       []byte // complete RFC 5322 message
	// Envelope. From defaults to the message's From/Sender; To defaults to
	// the message's To/Cc/Bcc addresses.
	From string
	To   []string
	// SaveToSent stores a copy in the Sent folder (default true when nil).
	SaveToSent *bool
}

// Result describes a successful submission.
type Result struct {
	EnvelopeFrom string
	Recipients   []string
	SentCopy     *delivery.Delivered // nil when not saved
	SentAt       time.Time
}

// Send validates, relays and stores the Sent copy.
func (s *Service) Send(ctx context.Context, req Request) (Result, error) {
	a := actor.From(ctx)
	fail := func(detail string, err error) (Result, error) {
		metrics.Deliveries.WithLabelValues("submit-refused").Inc()
		s.audit.Log(ctx, auditor.Event{Mailbox: req.MailboxID, Action: "send", Detail: detail, Result: audit.ResultDenied})
		return Result{}, err
	}
	if !(a.MailboxID == req.MailboxID && a.HasRole(identity.RoleSend)) {
		return fail("role send required", ErrForbidden)
	}
	m, err := s.ids.GetMailboxCached(ctx, req.MailboxID)
	if err != nil {
		return Result{}, err
	}
	if !m.Active() {
		return fail("mailbox "+m.State, ErrForbidden)
	}
	if m.Hold != nil && !m.Hold.AllowSend {
		return fail("on hold", ErrHold)
	}
	if int64(len(req.Raw)) > s.cfg.MaxMessageSize {
		return fail("too large", ErrTooLarge)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(req.Raw))
	if err != nil {
		return fail("unparseable", fmt.Errorf("%w: %v", ErrInvalid, err))
	}
	ids, err := s.Identities(ctx, req.MailboxID)
	if err != nil {
		return Result{}, err
	}
	allowed := func(addr string) bool {
		addr = strings.ToLower(strings.TrimSpace(addr))
		for _, id := range ids {
			if id.Email == addr {
				return true
			}
		}
		return false
	}
	// Header From (every address) and Sender must be allowed.
	for _, h := range []string{"From", "Sender"} {
		if v := msg.Header.Get(h); v != "" {
			list, err := mail.ParseAddressList(v)
			if err != nil {
				return fail("bad "+h, fmt.Errorf("%w: %s header", ErrInvalid, h))
			}
			for _, ad := range list {
				if !allowed(ad.Address) {
					return fail(h+"="+ad.Address, ErrBadSender)
				}
			}
		}
	}
	from := strings.ToLower(strings.TrimSpace(req.From))
	if from == "" {
		from = firstAddress(msg.Header.Get("Sender"))
		if from == "" {
			from = firstAddress(msg.Header.Get("From"))
		}
	}
	if from == "" || !allowed(from) {
		return fail("envelope from="+from, ErrBadSender)
	}
	to := req.To
	if len(to) == 0 {
		for _, h := range []string{"To", "Cc", "Bcc"} {
			if v := msg.Header.Get(h); v != "" {
				if list, err := mail.ParseAddressList(v); err == nil {
					for _, ad := range list {
						to = append(to, ad.Address)
					}
				}
			}
		}
	}
	to = dedup(to)
	if len(to) == 0 {
		return fail("no recipients", ErrNoRecipients)
	}
	if len(to) > s.cfg.MaxRecipients {
		return fail("too many recipients", fmt.Errorf("%w: too many recipients", ErrInvalid))
	}
	for _, r := range to {
		if _, _, err := identity.NormalizeAddress(r); err != nil {
			return fail("bad recipient "+r, fmt.Errorf("%w: recipient %q", ErrInvalid, r))
		}
	}
	// Strip Bcc from what leaves the building; prepend our Received header.
	outgoing := stripHeader(req.Raw, "Bcc")
	now := s.Now().UTC()
	received := fmt.Sprintf("Received: from [%s] by %s (Dovetail) with %s id %s for <%s>; %s\r\n",
		a.ClientIP, s.cfg.Hostname, strings.ToUpper(a.Protocol), a.SessionID, from, now.Format(time.RFC1123Z))
	outgoing = append([]byte(received), outgoing...)

	rctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	if err := s.relay.Send(rctx, from, to, outgoing); err != nil {
		metrics.Deliveries.WithLabelValues("submit-relay-error").Inc()
		s.audit.Log(ctx, auditor.Event{Mailbox: req.MailboxID, Action: "send", Detail: fmt.Sprintf("message-id=%s relay: %v", strings.Trim(msg.Header.Get("Message-ID"), "<> "), err), Result: audit.ResultError})
		return Result{}, fmt.Errorf("%w: %v", ErrRelay, err)
	}
	res := Result{EnvelopeFrom: from, Recipients: to, SentAt: now}
	metrics.Deliveries.WithLabelValues("submitted").Inc()
	msgID := strings.Trim(msg.Header.Get("Message-ID"), "<> ")
	s.audit.Log(ctx, auditor.Event{Mailbox: req.MailboxID, Action: "send", Target: from, Detail: fmt.Sprintf("message-id=%s to=%s", msgID, strings.Join(to, ","))})
	s.log.InfoContext(ctx, "submitted", slog.String("message_id", msgID), slog.String("from", from), slog.String("to", strings.Join(to, ",")),
		slog.String("mailbox", req.MailboxID.String()), slog.String("account", a.Username), slog.Int("bytes", len(outgoing)))

	if req.SaveToSent == nil || *req.SaveToSent {
		root, err := s.idx.EnsureRoot(ctx, req.MailboxID)
		if err != nil {
			return res, nil // sent; the copy is best effort
		}
		if sent := root.FolderByRole(index.RoleSent); sent != nil {
			d, err := s.del.Import(ctx, req.MailboxID, sent.ID, req.Raw, []string{index.FlagSeen}, now)
			if err != nil {
				s.log.WarnContext(ctx, "sent copy failed", slog.String("mailbox", req.MailboxID.String()), slog.String("error", err.Error()))
			} else {
				res.SentCopy = &d
			}
		}
	}
	return res, nil
}

func firstAddress(v string) string {
	if v == "" {
		return ""
	}
	list, err := mail.ParseAddressList(v)
	if err != nil || len(list) == 0 {
		return ""
	}
	return strings.ToLower(list[0].Address)
}

func dedup(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// stripHeader removes a top-level header (with its folded continuation
// lines) from a raw message.
func stripHeader(raw []byte, name string) []byte {
	sep := bytes.Index(raw, []byte("\r\n\r\n"))
	nl := []byte("\r\n")
	if sep < 0 {
		sep = bytes.Index(raw, []byte("\n\n"))
		nl = []byte("\n")
		if sep < 0 {
			return raw
		}
	}
	head, body := raw[:sep+len(nl)], raw[sep+len(nl):]
	lines := bytes.Split(head, nl)
	var out [][]byte
	skip := false
	prefix := []byte(strings.ToLower(name) + ":")
	for _, l := range lines {
		if len(l) > 0 && (l[0] == ' ' || l[0] == '\t') {
			if !skip {
				out = append(out, l)
			}
			continue
		}
		skip = bytes.HasPrefix(bytes.ToLower(l), prefix)
		if !skip {
			out = append(out, l)
		}
	}
	return append(bytes.Join(out, nl), body...)
}

// ---- SMTP relay ----

type smtpRelay struct{ cfg Config }

func (r *smtpRelay) Send(ctx context.Context, from string, to []string, data []byte) error {
	d := net.Dialer{Timeout: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", r.cfg.RelayHost)
	if err != nil {
		return err
	}
	var c *smtp.Client
	if r.cfg.StartTLS {
		host, _, _ := net.SplitHostPort(r.cfg.RelayHost)
		c, err = smtp.NewClientStartTLS(conn, &tls.Config{ServerName: host})
		if err != nil {
			_ = conn.Close()
			return err
		}
	} else {
		c = smtp.NewClient(conn)
	}
	defer c.Close()
	if err := c.Hello(r.cfg.Hostname); err != nil {
		return err
	}
	if r.cfg.Username != "" {
		if err := c.Auth(sasl.NewPlainClient("", r.cfg.Username, r.cfg.Password)); err != nil {
			return err
		}
	}
	if err := c.Mail(from, &smtp.MailOptions{Size: int64(len(data))}); err != nil {
		return err
	}
	for _, rcpt := range to {
		if err := c.Rcpt(rcpt, nil); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
