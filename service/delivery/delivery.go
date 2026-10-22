// Package delivery stores inbound (LMTP) and imported (JMAP, IMAP APPEND,
// Sent copies) messages: recipient resolution, domain/mailbox/hold/quota
// checks, parsing, attachment dedup, message objects, index entry, change
// log, search events, notifications and audit. See README.md "LMTP
// delivery" and "Delivery ordering and crash safety".
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"dovetail/mail"
	"dovetail/metrics"
	"dovetail/service/actor"
	"dovetail/service/auditor"
	"dovetail/store/attach"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
	"dovetail/store/message"
	"dovetail/store/queue"
)

// Errors. LMTP maps them to reply codes with Status.
var (
	ErrUnknownDomain    = errors.New("delivery: unknown or disabled domain")
	ErrUnknownRecipient = errors.New("delivery: no such recipient")
	ErrMailboxDisabled  = errors.New("delivery: mailbox disabled")
	ErrMailboxSuspended = errors.New("delivery: mailbox suspended")
	ErrHold             = errors.New("delivery: mailbox on hold, delivery suspended")
	ErrOverQuota        = errors.New("delivery: mailbox over quota")
	ErrTooLarge         = errors.New("delivery: message too large")
	ErrAliasLoop        = errors.New("delivery: alias loop or too deep")
	ErrNoFolder         = errors.New("delivery: folder not found")
)

// Config for the service.
type Config struct {
	MaxMessageSize    int64
	InlineMax         int64
	KeepRaw           string // never | signed | always
	SpamHeader        string
	SpamValue         string
	MaxAliasDepth     int
	DefaultQuota      identity.QuotaLimits
	OverQuotaTempFail bool
	// SearchEvents publishes index/remove events to the task queue; off
	// unless a search backend consumes them (one write per message).
	SearchEvents bool
	// LookupTTL caches domain/account/alias/mailbox lookups per process so
	// a stream of deliveries to the same recipient costs no identity reads.
	LookupTTL time.Duration
	// QuotaStaleness: how old a cached root may be for the quota check.
	QuotaStaleness time.Duration
}

// Task types this service enqueues.
const (
	TaskSearchIndex = "search-index"
)

// SearchIndexTask is the payload of a search-index task.
type SearchIndexTask struct {
	Mailbox uuid.UUID `json:"mailbox"`
	Message uuid.UUID `json:"message"`
	Op      string    `json:"op"` // "index" or "remove"
}

// Service is the delivery service.
type Service struct {
	ids   *identity.Store
	idx   *index.Store
	msgs  *message.Store
	atts  *attach.Store
	queue *queue.Store
	audit *auditor.Auditor
	log   *slog.Logger
	cfg   Config
	Now   func() time.Time

	lookups lookupCache
}

// lookupCache is a small TTL cache for identity objects keyed by kind+key.
type lookupCache struct {
	mu    sync.Mutex
	items map[string]cacheItem
}

type cacheItem struct {
	v   any
	err error
	at  time.Time
}

const maxCachedLookups = 50000

func (c *lookupCache) get(key string, now time.Time, ttl time.Duration, load func() (any, error)) (any, error) {
	if ttl > 0 {
		c.mu.Lock()
		if it, ok := c.items[key]; ok && now.Sub(it.at) < ttl {
			c.mu.Unlock()
			return it.v, it.err
		}
		c.mu.Unlock()
	}
	v, err := load()
	if ttl > 0 && (err == nil || errors.Is(err, identity.ErrNotFound)) {
		c.mu.Lock()
		if c.items == nil {
			c.items = map[string]cacheItem{}
		}
		if len(c.items) >= maxCachedLookups {
			for k := range c.items { // drop arbitrary entries
				delete(c.items, k)
				if len(c.items) < maxCachedLookups/2 {
					break
				}
			}
		}
		c.items[key] = cacheItem{v: v, err: err, at: now}
		c.mu.Unlock()
	}
	return v, err
}

// Invalidate drops cached identity lookups (tests, admin changes).
func (s *Service) Invalidate() {
	s.lookups.mu.Lock()
	s.lookups.items = nil
	s.lookups.mu.Unlock()
}

func (s *Service) getDomain(ctx context.Context, name string) (identity.Domain, error) {
	v, err := s.lookups.get("d:"+name, s.Now(), s.cfg.LookupTTL, func() (any, error) {
		d, err := s.ids.GetDomain(ctx, name)
		return d.Value, err
	})
	if err != nil {
		return identity.Domain{}, err
	}
	return v.(identity.Domain), nil
}

func (s *Service) getAccount(ctx context.Context, username string) (identity.Account, error) {
	v, err := s.lookups.get("a:"+username, s.Now(), s.cfg.LookupTTL, func() (any, error) {
		a, err := s.ids.GetAccountByUsername(ctx, username)
		return a.Value, err
	})
	if err != nil {
		return identity.Account{}, err
	}
	return v.(identity.Account), nil
}

func (s *Service) getAlias(ctx context.Context, key string) (identity.Alias, error) {
	v, err := s.lookups.get("l:"+key, s.Now(), s.cfg.LookupTTL, func() (any, error) {
		a, err := s.ids.GetAlias(ctx, key)
		return a.Value, err
	})
	if err != nil {
		return identity.Alias{}, err
	}
	return v.(identity.Alias), nil
}

func (s *Service) getMailbox(ctx context.Context, id uuid.UUID) (identity.Mailbox, error) {
	v, err := s.lookups.get("m:"+id.String(), s.Now(), s.cfg.LookupTTL, func() (any, error) {
		m, err := s.ids.GetMailbox(ctx, id)
		return m.Value, err
	})
	if err != nil {
		return identity.Mailbox{}, err
	}
	return v.(identity.Mailbox), nil
}

// New returns the service. queue may be nil (events dropped).
func New(ids *identity.Store, idx *index.Store, msgs *message.Store, atts *attach.Store, q *queue.Store, aud *auditor.Auditor, log *slog.Logger, cfg Config) *Service {
	if cfg.MaxAliasDepth <= 0 {
		cfg.MaxAliasDepth = 5
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 50 << 20
	}
	if cfg.QuotaStaleness <= 0 {
		cfg.QuotaStaleness = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{ids: ids, idx: idx, msgs: msgs, atts: atts, queue: q, audit: aud, log: log, cfg: cfg, Now: time.Now}
}

// Target is one mailbox an address resolves to.
type Target struct {
	MailboxID uuid.UUID
	// Via describes the resolution: the account username, the alias
	// address, or "@domain" for the catch-all.
	Via string
}

// Resolve maps a recipient address to mailboxes: exact account → alias →
// catch-all → reject. Disabled domains and mailboxes are reported.
func (s *Service) Resolve(ctx context.Context, address string) ([]Target, error) {
	addr, domain, err := identity.NormalizeAddress(address)
	if err != nil {
		return nil, ErrUnknownRecipient
	}
	d, err := s.getDomain(ctx, domain)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return nil, ErrUnknownDomain
		}
		return nil, err
	}
	if !d.Enabled {
		return nil, ErrUnknownDomain
	}
	seen := map[string]bool{}
	return s.resolve(ctx, addr, domain, 0, seen)
}

func (s *Service) resolve(ctx context.Context, addr, domain string, depth int, seen map[string]bool) ([]Target, error) {
	if depth > s.cfg.MaxAliasDepth || seen[addr] {
		return nil, ErrAliasLoop
	}
	seen[addr] = true
	// 1. Exact account.
	if a, err := s.getAccount(ctx, addr); err == nil {
		if a.MailboxID != uuid.Nil {
			return []Target{{MailboxID: a.MailboxID, Via: a.Username}}, nil
		}
	} else if !errors.Is(err, identity.ErrNotFound) && !errors.Is(err, identity.ErrInvalid) {
		return nil, err
	}
	// 2. Alias, 3. catch-all.
	for _, key := range []string{addr, "@" + domain} {
		al, err := s.getAlias(ctx, key)
		if err != nil {
			if errors.Is(err, identity.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if !al.Enabled {
			continue
		}
		var out []Target
		for _, dst := range al.Destinations {
			if dst.MailboxID != uuid.Nil {
				out = append(out, Target{MailboxID: dst.MailboxID, Via: key})
				continue
			}
			_, ddom, err := identity.NormalizeAddress(dst.Address)
			if err != nil {
				continue
			}
			sub, err := s.resolve(ctx, dst.Address, ddom, depth+1, seen)
			if err != nil {
				if errors.Is(err, ErrUnknownRecipient) {
					continue
				}
				return nil, err
			}
			out = append(out, sub...)
		}
		if len(out) > 0 {
			return dedupTargets(out), nil
		}
	}
	return nil, ErrUnknownRecipient
}

func dedupTargets(in []Target) []Target {
	seen := map[uuid.UUID]bool{}
	var out []Target
	for _, t := range in {
		if !seen[t.MailboxID] {
			seen[t.MailboxID] = true
			out = append(out, t)
		}
	}
	return out
}

// CheckRecipient validates that the address can receive mail right now
// (used by LMTP RCPT TO and the Postfix socketmap). It returns the targets
// or the same errors Deliver would.
func (s *Service) CheckRecipient(ctx context.Context, address string, size int64) ([]Target, error) {
	targets, err := s.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	for _, t := range targets {
		if _, err := s.admission(ctx, t.MailboxID, size); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// admission checks mailbox enabled, hold and quota for an incoming size.
func (s *Service) admission(ctx context.Context, mb uuid.UUID, size int64) (identity.Mailbox, error) {
	m, err := s.getMailbox(ctx, mb)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return identity.Mailbox{}, ErrUnknownRecipient
		}
		return identity.Mailbox{}, err
	}
	switch m.State {
	case identity.StateSuspended:
		return m, ErrMailboxSuspended
	case identity.StateDisabled:
		return m, ErrMailboxDisabled
	}
	if m.Hold != nil && !m.Hold.AcceptDelivery {
		return m, ErrHold
	}
	if size > s.cfg.MaxMessageSize {
		return m, ErrTooLarge
	}
	limits := s.effectiveQuota(m.Quota)
	if limits.HardBytes > 0 || limits.HardMessages > 0 {
		root, err := s.idx.GetRootCached(ctx, mb, s.cfg.QuotaStaleness)
		if err != nil && !errors.Is(err, index.ErrNotFound) {
			return m, err
		}
		if err == nil {
			if limits.HardBytes > 0 && root.Usage.Bytes+size > limits.HardBytes {
				return m, ErrOverQuota
			}
			if limits.HardMessages > 0 && root.Usage.Messages+1 > limits.HardMessages {
				return m, ErrOverQuota
			}
			if (limits.SoftBytes > 0 && root.Usage.Bytes+size > limits.SoftBytes) || (limits.SoftMessages > 0 && root.Usage.Messages+1 > limits.SoftMessages) {
				s.log.WarnContext(ctx, "mailbox over soft quota", slog.String("mailbox", mb.String()),
					slog.Int64("bytes", root.Usage.Bytes), slog.Int64("messages", root.Usage.Messages))
			}
		}
	}
	return m, nil
}

func (s *Service) effectiveQuota(q identity.QuotaLimits) identity.QuotaLimits {
	d := s.cfg.DefaultQuota
	if q.SoftBytes == 0 {
		q.SoftBytes = d.SoftBytes
	}
	if q.HardBytes == 0 {
		q.HardBytes = d.HardBytes
	}
	if q.SoftMessages == 0 {
		q.SoftMessages = d.SoftMessages
	}
	if q.HardMessages == 0 {
		q.HardMessages = d.HardMessages
	}
	return q
}

// EffectiveQuota returns the limits that apply to a mailbox.
func (s *Service) EffectiveQuota(m identity.Mailbox) identity.QuotaLimits {
	return s.effectiveQuota(m.Quota)
}

// Request is an inbound delivery.
type Request struct {
	Recipient string
	Sender    string // MAIL FROM, for audit
	Raw       []byte
	ClientIP  string // the MTA's address
}

// Delivered describes one stored copy.
type Delivered struct {
	MailboxID uuid.UUID
	MessageID uuid.UUID // Dovetail's message uuid
	FolderID  string
	UID       uint32
	Via       string
	// MessageIDHeader is the message's Message-ID header, for logs.
	MessageIDHeader string
	// Record is the index entry as written (protocol adapters that hold a
	// folder snapshot can apply it without re-reading).
	Record index.Record
}

// Deliver stores an inbound message in every mailbox the recipient resolves
// to. It fails as a whole if any target refuses admission (checked before
// anything is written), so the MTA gets one answer per recipient.
func (s *Service) Deliver(ctx context.Context, req Request) ([]Delivered, error) {
	out, err := s.deliver(ctx, req)
	metrics.Deliveries.WithLabelValues(resultLabel(err)).Inc()
	if err == nil {
		metrics.DeliveryBytes.Add(float64(len(req.Raw) * len(out)))
	}
	return out, err
}

func resultLabel(err error) string {
	switch {
	case err == nil:
		return "delivered"
	case errors.Is(err, ErrUnknownDomain), errors.Is(err, ErrUnknownRecipient), errors.Is(err, ErrAliasLoop):
		return "unknown-recipient"
	case errors.Is(err, ErrOverQuota):
		return "over-quota"
	case errors.Is(err, ErrMailboxDisabled):
		return "disabled"
	case errors.Is(err, ErrMailboxSuspended):
		return "suspended"
	case errors.Is(err, ErrHold):
		return "hold"
	case errors.Is(err, ErrTooLarge):
		return "too-large"
	}
	return "error"
}

func (s *Service) deliver(ctx context.Context, req Request) ([]Delivered, error) {
	if int64(len(req.Raw)) > s.cfg.MaxMessageSize {
		return nil, ErrTooLarge
	}
	targets, err := s.Resolve(ctx, req.Recipient)
	if err != nil {
		return nil, err
	}
	for _, t := range targets {
		if _, err := s.admission(ctx, t.MailboxID, int64(len(req.Raw))); err != nil {
			return nil, err
		}
	}
	parsed, err := mail.Parse(req.Raw, mail.Options{InlineMax: s.cfg.InlineMax})
	if err != nil {
		return nil, fmt.Errorf("delivery: parse: %w", err)
	}
	spam := s.isSpam(parsed.Doc)
	sys := actor.System(audit.ProtoLMTP)
	sys.ClientIP = req.ClientIP
	actx := actor.With(ctx, sys)
	var out []Delivered
	for _, t := range targets {
		if err := s.idx.EnsureExists(ctx, t.MailboxID); err != nil {
			return out, err
		}
		root, err := s.idx.GetRootCached(ctx, t.MailboxID, s.cfg.QuotaStaleness)
		if err != nil {
			return out, err
		}
		folder := root.FolderByRole(index.RoleInbox)
		if spam {
			if j := root.FolderByRole(index.RoleJunk); j != nil {
				folder = j
			}
		}
		if folder == nil {
			return out, ErrNoFolder
		}
		d, err := s.store(actx, t.MailboxID, folder.ID, parsed, req.Raw, nil, s.Now().UTC())
		if err != nil {
			s.audit.Log(actx, auditor.Event{Mailbox: t.MailboxID, Action: "deliver", Target: req.Recipient,
				Detail: fmt.Sprintf("from=%s message-id=%s error=%s", req.Sender, parsed.Doc.MessageID, err), Result: audit.ResultError})
			return out, err
		}
		d.Via = t.Via
		d.MessageIDHeader = parsed.Doc.MessageID
		out = append(out, d)
		s.audit.Log(actx, auditor.Event{Mailbox: t.MailboxID, Action: "deliver", Target: fmt.Sprintf("%s/%d", d.FolderID, d.UID),
			Detail: fmt.Sprintf("rcpt=%s via=%s from=%s message-id=%s msg=%s spam=%v", req.Recipient, t.Via, req.Sender, parsed.Doc.MessageID, d.MessageID, spam)})
		s.log.InfoContext(ctx, "delivered", slog.String("message_id", parsed.Doc.MessageID), slog.String("rcpt", req.Recipient),
			slog.String("via", t.Via), slog.String("from", req.Sender), slog.String("mailbox", t.MailboxID.String()),
			slog.String("msg", d.MessageID.String()), slog.String("folder", d.FolderID), slog.Uint64("uid", uint64(d.UID)), slog.Bool("spam", spam))
	}
	return out, nil
}

func (s *Service) isSpam(doc *mail.Document) bool {
	if s.cfg.SpamHeader == "" {
		return false
	}
	for _, h := range doc.Headers {
		if strings.EqualFold(h.Name, s.cfg.SpamHeader) && strings.EqualFold(strings.TrimSpace(h.Value), s.cfg.SpamValue) {
			return true
		}
	}
	return false
}

// Import stores a message supplied by the user (JMAP Email/import, IMAP
// APPEND, drafts, Sent copies) into a folder with the given flags. The
// actor in ctx is audited. Quota and hold are enforced.
func (s *Service) Import(ctx context.Context, mb uuid.UUID, folderID string, raw []byte, flags []string, internalDate time.Time) (Delivered, error) {
	if int64(len(raw)) > s.cfg.MaxMessageSize {
		return Delivered{}, ErrTooLarge
	}
	m, err := s.admission(ctx, mb, int64(len(raw)))
	if err != nil && !errors.Is(err, ErrHold) {
		return Delivered{}, err
	}
	if m.Hold != nil {
		return Delivered{}, ErrHold // user mutations are refused on hold
	}
	parsed, err := mail.Parse(raw, mail.Options{InlineMax: s.cfg.InlineMax})
	if err != nil {
		return Delivered{}, fmt.Errorf("delivery: parse: %w", err)
	}
	if err := s.idx.EnsureExists(ctx, mb); err != nil {
		return Delivered{}, err
	}
	if internalDate.IsZero() {
		internalDate = s.Now().UTC()
	}
	d, err := s.store(ctx, mb, folderID, parsed, raw, flags, internalDate)
	if err != nil {
		s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "append", Target: folderID, Detail: fmt.Sprintf("message-id=%s error=%s", parsed.Doc.MessageID, err), Result: audit.ResultError})
		return Delivered{}, err
	}
	d.MessageIDHeader = parsed.Doc.MessageID
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "append", Target: fmt.Sprintf("%s/%d", d.FolderID, d.UID), Detail: fmt.Sprintf("message-id=%s msg=%s", parsed.Doc.MessageID, d.MessageID)})
	return d, nil
}

// store writes attachments, then lets the index allocate the UID, writes the
// single message object at its final location, and lets the index commit
// the entry. A crash before the index write leaves an orphan object (swept
// later) but never a dangling index entry.
func (s *Service) store(ctx context.Context, mb uuid.UUID, folderID string, parsed *mail.Parsed, raw []byte, flags []string, internalDate time.Time) (Delivered, error) {
	msgID := uuid.New()
	docJSON, err := json.Marshal(parsed.Doc)
	if err != nil {
		return Delivered{}, err
	}
	keepRaw := s.cfg.KeepRaw == "always" || (s.cfg.KeepRaw == "signed" && parsed.Signed)

	// Attachments (content + reference in one transaction each) are
	// independent of the index: write them while the index allocates.
	attDone := make(chan error, 1)
	go func() {
		for _, a := range parsed.Attachments {
			if err := s.atts.Put(ctx, a.Hash, a.Data, a.ContentType, mb, msgID); err != nil {
				attDone <- fmt.Errorf("delivery: attachment %s: %w", a.Hash, err)
				return
			}
		}
		attDone <- nil
	}()

	// Root transaction (UID, modseq, counters, change log) → message object
	// at its location (concurrently with the thread maps, inside Append) →
	// folder record (commit point). Round trips on the critical path: 3.
	rec := recordFor(msgID, parsed.Doc, flags, internalDate)
	rec, err = s.idx.Append(ctx, mb, folderID, rec, func(uid uint32, _ uint64) error {
		loc := index.Location{FolderID: folderID, UID: uid}.String()
		var pg sync.WaitGroup
		var rawErr error
		if keepRaw {
			pg.Add(1)
			go func() {
				defer pg.Done()
				if err := s.msgs.PutRaw(ctx, mb, msgID, raw); err != nil {
					rawErr = fmt.Errorf("delivery: raw object: %w", err)
				}
			}()
		}
		err := s.msgs.Put(ctx, mb, msgID, docJSON, loc)
		pg.Wait()
		if err != nil {
			return fmt.Errorf("delivery: message object: %w", err)
		}
		if rawErr != nil {
			return rawErr
		}
		// Attachments must exist before the index entry commits.
		return <-attDone
	})
	if err != nil {
		if errors.Is(err, index.ErrNotFound) {
			err = ErrNoFolder
		}
		return Delivered{}, err
	}
	// Events and notifications are off the critical path.
	if s.cfg.SearchEvents && s.queue != nil {
		if _, err := s.queue.Enqueue(ctx, TaskSearchIndex, SearchIndexTask{Mailbox: mb, Message: msgID, Op: "index"}, time.Time{}); err != nil {
			s.log.WarnContext(ctx, "enqueue search-index failed", slog.String("error", err.Error()))
		}
	}
	payload, _ := json.Marshal(map[string]any{"type": "email", "folder": folderID, "uid": rec.UID, "modseq": rec.ModSeq})
	go func() {
		nctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.idx.Notify(nctx, mb, payload); err != nil {
			s.log.DebugContext(nctx, "notify failed", slog.String("error", err.Error()))
		}
	}()
	return Delivered{MailboxID: mb, MessageID: msgID, FolderID: folderID, UID: rec.UID, Record: rec}, nil
}

// recordFor builds the index record from the parsed document.
func recordFor(msgID uuid.UUID, doc *mail.Document, flags []string, internalDate time.Time) index.Record {
	rec := index.Record{
		MessageID: msgID, Flags: flags, Size: doc.Size, InternalDate: internalDate,
		Subject: doc.Subject, Date: doc.Date, MsgIDHeader: doc.MessageID, InReplyTo: doc.InReplyTo, References: doc.References,
		HasAttachments: doc.HasAttachments,
	}
	if len(doc.From) > 0 {
		f := doc.From[0]
		rec.FromEmail = f.Email
		if f.Name != "" {
			rec.From = f.Name + " <" + f.Email + ">"
		} else {
			rec.From = f.Email
		}
	}
	if rec.Date.IsZero() {
		rec.Date = internalDate
	}
	if len(rec.Subject) > 200 {
		rec.Subject = rec.Subject[:200]
	}
	if len(rec.From) > 120 {
		rec.From = rec.From[:120]
	}
	return rec
}

// Status maps a delivery error to an SMTP/LMTP reply: code, enhanced status
// code and text. Unknown errors are temporary failures.
func (s *Service) Status(err error) (code int, enhanced, text string) {
	switch {
	case err == nil:
		return 250, "2.0.0", "OK"
	case errors.Is(err, ErrUnknownDomain):
		return 550, "5.1.2", "Domain not handled here"
	case errors.Is(err, ErrUnknownRecipient), errors.Is(err, ErrAliasLoop):
		return 550, "5.1.1", "No such user"
	case errors.Is(err, ErrMailboxDisabled):
		return 550, "5.2.1", "Mailbox disabled"
	case errors.Is(err, ErrMailboxSuspended):
		return 450, "4.2.1", "Mailbox temporarily suspended, try again later"
	case errors.Is(err, ErrHold):
		return 450, "4.2.1", "Mailbox temporarily not accepting mail"
	case errors.Is(err, ErrOverQuota):
		if s.cfg.OverQuotaTempFail {
			return 452, "4.2.2", "Mailbox full, try again later"
		}
		return 552, "5.2.2", "Mailbox full"
	case errors.Is(err, ErrTooLarge):
		return 552, "5.3.4", "Message too large"
	case errors.Is(err, ErrNoFolder):
		return 451, "4.3.0", "Mailbox folder missing"
	}
	return 451, "4.3.0", "Temporary failure"
}
