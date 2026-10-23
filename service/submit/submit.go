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

	"kittiwake/metrics"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
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

// AccountHeader carries the sending account's UUID on every outbound
// message so the relay (Postfix header checks, milters) can tie it to an
// account. Copies supplied by the client are removed first.
const AccountHeader = "X-Kittiwake-Account"

// Config for the service.
type Config struct {
	RelayHost string // host:port of the SMTP relay (Postfix)
	Username  string // optional SASL PLAIN credentials
	Password  string
	// TLS: "off", "opportunistic" (default: STARTTLS when offered, no
	// certificate verification) or "required" (STARTTLS and a verified
	// certificate, unless TLSInsecureSkipVerify).
	TLS                   string
	TLSServerName         string
	TLSInsecureSkipVerify bool
	// XClient passes the account UUID (LOGIN) and client address (ADDR) to
	// the relay with Postfix XCLIENT on every session; see config.SMTP.
	XClient        bool
	Hostname       string // EHLO name and Received: host
	MaxMessageSize int64
	Timeout        time.Duration
	MaxRecipients  int
	MaxAliasDepth  int // bounds the alias chains followed for identities
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
	if cfg.MaxAliasDepth <= 0 {
		cfg.MaxAliasDepth = 5
	}
	if cfg.Hostname == "" {
		cfg.Hostname = "kittiwake"
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
// own username and the enabled aliases that reach it, all carrying the
// account's display name. An alias reaches it with the mailbox as a
// destination or with an address destination that is the account's own
// address or another such alias (chains up to MaxAliasDepth); catch-alls
// never count. Other accounts attached to the same mailbox are not
// identities of this one, nor are aliases that only reach the mailbox
// through them. An admin inspecting a mailbox sees the identities of every
// account on it.
func (s *Service) Identities(ctx context.Context, mb uuid.UUID) ([]Identity, error) {
	a := actor.From(ctx)
	if !(a.MailboxID == mb || a.IsAdminReader()) {
		return nil, ErrForbidden
	}
	var out []Identity
	seen := map[string]bool{}
	name := ""
	var frontier []string // addresses whose address-destination aliases still need listing
	if a.AccountID != uuid.Nil {
		acct, err := s.ids.GetAccountByUsernameCached(ctx, a.Username)
		if err != nil {
			return nil, err
		}
		name = acct.Name
		seen[acct.Username] = true
		out = append(out, Identity{ID: acct.Username, Email: acct.Username, Name: name})
		frontier = append(frontier, acct.Username)
	} else {
		users, _, err := s.ids.ListMailboxAccounts(ctx, mb, "", 1000)
		if err != nil {
			return nil, err
		}
		for _, u := range users {
			if !seen[u] {
				seen[u] = true
				out = append(out, Identity{ID: u, Email: u})
				frontier = append(frontier, u)
			}
		}
	}
	// addAlias records an enabled, non-catch-all alias; it reports whether
	// the alias is new, so its own reverse listing is followed.
	addAlias := func(addr string) (bool, error) {
		if addr == "" || strings.HasPrefix(addr, "@") || seen[addr] {
			return false, nil
		}
		seen[addr] = true
		al, err := s.ids.GetAliasCached(ctx, addr)
		if err != nil {
			if errors.Is(err, identity.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		if !al.Enabled {
			return false, nil
		}
		out = append(out, Identity{ID: addr, Email: addr, Name: name})
		return true, nil
	}
	direct, err := s.ids.ListMailboxAliases(ctx, mb)
	if err != nil {
		return nil, err
	}
	for _, addr := range direct {
		if ok, err := addAlias(addr); err != nil {
			return nil, err
		} else if ok {
			frontier = append(frontier, addr)
		}
	}
	for depth := 0; depth < s.cfg.MaxAliasDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, dest := range frontier {
			to, err := s.ids.ListAliasesTo(ctx, dest)
			if err != nil {
				return nil, err
			}
			for _, addr := range to {
				if ok, err := addAlias(addr); err != nil {
					return nil, err
				} else if ok {
					next = append(next, addr)
				}
			}
		}
		frontier = next
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

// CheckSender reports whether the acting account may use addr as its
// envelope sender, so an SMTP adapter can refuse MAIL FROM before the
// client uploads the message. Send checks again.
func (s *Service) CheckSender(ctx context.Context, mb uuid.UUID, addr string) error {
	ids, err := s.Identities(ctx, mb)
	if err != nil {
		return err
	}
	addr = strings.ToLower(strings.TrimSpace(addr))
	for _, id := range ids {
		if id.Email == addr {
			return nil
		}
	}
	return ErrBadSender
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
	if err := actor.CheckAccount(ctx, s.ids, a, identity.RoleSend, s.Now()); err != nil {
		return fail("account access revoked", ErrForbidden)
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
	now := s.Now().UTC()
	// RFC 6409 §8.3/8.4: the submission server may add a missing
	// Message-ID and Date. The Sent copy gets the same headers.
	var add []string
	if msg.Header.Get("Message-ID") == "" {
		add = append(add, fmt.Sprintf("Message-ID: <%s@%s>", uuid.NewString(), s.cfg.Hostname))
	}
	if msg.Header.Get("Date") == "" {
		add = append(add, "Date: "+now.Format(time.RFC1123Z))
	}
	if len(add) > 0 {
		req.Raw = append([]byte(strings.Join(add, "\r\n")+"\r\n"), req.Raw...)
		if msg, err = mail.ReadMessage(bytes.NewReader(req.Raw)); err != nil {
			return fail("unparseable", fmt.Errorf("%w: %v", ErrInvalid, err))
		}
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
	// Strip Bcc and any client-supplied account header from what leaves the
	// building; prepend our Received and account headers.
	outgoing := stripHeader(stripHeader(req.Raw, "Bcc"), AccountHeader)
	id := ""
	if a.SessionID != "" {
		id = " id " + a.SessionID
	}
	received := fmt.Sprintf("Received: from [%s] by %s (Kittiwake) with %s%s for <%s>; %s\r\n",
		a.ClientIP, s.cfg.Hostname, withProtocol(a.Protocol), id, from, now.Format(time.RFC1123Z))
	outgoing = append([]byte(received+AccountHeader+": "+a.AccountID.String()+"\r\n"), outgoing...)

	rctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	if err := s.relay.Send(rctx, from, to, outgoing); err != nil {
		metrics.Deliveries.WithLabelValues("submit-relay-error").Inc()
		s.audit.Log(ctx, auditor.Event{Mailbox: req.MailboxID, Action: "send", Detail: fmt.Sprintf("message-id=%s relay: %v", strings.Trim(msg.Header.Get("Message-ID"), "<> "), err), Result: audit.ResultError})
		return Result{}, fmt.Errorf("%w: %w", ErrRelay, err)
	}
	res := Result{EnvelopeFrom: from, Recipients: to, SentAt: now}
	metrics.Deliveries.WithLabelValues("submitted").Inc()
	msgID := strings.Trim(msg.Header.Get("Message-ID"), "<> ")
	s.audit.Log(ctx, auditor.Event{Mailbox: req.MailboxID, Action: "send", Target: from, Detail: fmt.Sprintf("message-id=%s to=%s", msgID, strings.Join(to, ","))})
	s.log.InfoContext(ctx, "submitted", slog.String("message_id", msgID), slog.String("from", from), slog.String("to", strings.Join(to, ",")),
		slog.String("mailbox", req.MailboxID.String()), slog.String("account", a.Username), slog.Int("bytes", len(outgoing)))

	if req.SaveToSent == nil || *req.SaveToSent {
		d, err := s.del.SaveSent(ctx, req.MailboxID, req.Raw, now)
		if err != nil {
			s.log.WarnContext(ctx, "sent copy failed", slog.String("mailbox", req.MailboxID.String()), slog.String("error", err.Error()))
		} else {
			res.SentCopy = &d
		}
	}
	return res, nil
}

// withProtocol is the Received: "with" keyword for a protocol: ESMTPSA
// (RFC 3848) for authenticated SMTP submission, the protocol name otherwise.
func withProtocol(proto string) string {
	if proto == "submission" {
		return "ESMTPSA"
	}
	return strings.ToUpper(proto)
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

// ErrNoStartTLS: smtp.tls is required and the relay does not offer it.
var ErrNoStartTLS = errors.New("submit: relay does not offer STARTTLS")

func (r *smtpRelay) dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: 30 * time.Second}
	return d.DialContext(ctx, "tcp", r.cfg.RelayHost)
}

func (r *smtpRelay) mode() string {
	if r.cfg.TLS == "" {
		return "opportunistic"
	}
	return r.cfg.TLS
}

// tlsConfig is the STARTTLS client configuration for the TLS mode.
func (r *smtpRelay) tlsConfig() *tls.Config {
	host, _, err := net.SplitHostPort(r.cfg.RelayHost)
	if err != nil {
		host = r.cfg.RelayHost
	}
	tc := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	if r.cfg.TLSServerName != "" {
		tc.ServerName = r.cfg.TLSServerName
	}
	if r.mode() == "opportunistic" || r.cfg.TLSInsecureSkipVerify {
		tc.InsecureSkipVerify = true //nolint:gosec // opportunistic encryption, documented
	}
	return tc
}

func (r *smtpRelay) Send(ctx context.Context, from string, to []string, data []byte) error {
	c, err := r.connect(ctx, actor.From(ctx))
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Hello(r.cfg.Hostname); err != nil {
		return err
	}
	if r.cfg.Username != "" && !r.cfg.XClient {
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
