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

	"kittiwake/mail"
	"kittiwake/metrics"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
	"kittiwake/store/storage"
)

// Errors. LMTP maps them to reply codes with Status.
var (
	ErrUnknownDomain    = errors.New("delivery: unknown or disabled domain")
	ErrUnknownRecipient = errors.New("delivery: no such recipient")
	ErrMailboxDisabled  = errors.New("delivery: mailbox disabled")
	ErrMailboxSuspended = errors.New("delivery: mailbox suspended")
	// ErrUnavailable: the mailbox's Ceph storage is not reachable from this
	// process (LMTP defers, the MTA retries).
	ErrUnavailable = errors.New("delivery: mailbox temporarily unavailable")
	ErrHold        = errors.New("delivery: mailbox on hold, delivery suspended")
	ErrOverQuota   = index.ErrOverQuota
	ErrTooLarge    = errors.New("delivery: message too large")
	ErrAliasLoop   = errors.New("delivery: alias loop or too deep")
	ErrForbidden   = actor.ErrForbidden
	ErrNoFolder    = errors.New("delivery: folder not found")
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
	// RootTTL: how old a cached mailbox root may be for the quota check
	// (identity lookups are cached by the identity store, see its CacheTTL).
	RootTTL time.Duration
	// StorageAvailable reports whether the mailbox's storage is connected
	// (no I/O: a registry lookup on the cached mailbox record). nil = always.
	StorageAvailable func(ctx context.Context, mailbox uuid.UUID) error
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
	queue queue.Store
	audit *auditor.Auditor
	log   *slog.Logger
	cfg   Config
	Now   func() time.Time
}

// Invalidate drops cached identity lookups in this process (tests, admin
// changes made through another store instance).
func (s *Service) Invalidate() { s.ids.InvalidateCache() }

func (s *Service) getDomain(ctx context.Context, name string) (identity.Domain, error) {
	return s.ids.GetDomainCached(ctx, name)
}

func (s *Service) getAccount(ctx context.Context, username string) (identity.Account, error) {
	return s.ids.GetAccountByUsernameCached(ctx, username)
}

func (s *Service) getAlias(ctx context.Context, key string) (identity.Alias, error) {
	return s.ids.GetAliasCached(ctx, key)
}

func (s *Service) getMailbox(ctx context.Context, id uuid.UUID) (identity.Mailbox, error) {
	return s.ids.GetMailboxCached(ctx, id)
}

// New returns the service. queue may be nil (events dropped).
func New(ids *identity.Store, idx *index.Store, msgs *message.Store, atts *attach.Store, q queue.Store, aud *auditor.Auditor, log *slog.Logger, cfg Config) *Service {
	if cfg.MaxAliasDepth <= 0 {
		cfg.MaxAliasDepth = 5
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 50 << 20
	}
	if cfg.RootTTL <= 0 {
		cfg.RootTTL = 5 * time.Second
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
	if s.cfg.StorageAvailable != nil {
		if err := s.cfg.StorageAvailable(ctx, mb); err != nil {
			return m, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
	}
	if m.Hold != nil && !m.Hold.AcceptDelivery {
		return m, ErrHold
	}
	if size > s.cfg.MaxMessageSize {
		return m, ErrTooLarge
	}
	limits := s.effectiveQuota(m.Quota)
	if limits.HardBytes > 0 || limits.HardMessages > 0 {
		root, err := s.idx.GetRootCached(ctx, mb, s.cfg.RootTTL)
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
	MessageID uuid.UUID // Kittiwake's message uuid
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
	case errors.Is(err, ErrUnavailable), errors.Is(err, storage.ErrUnavailable):
		return "unavailable"
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
		root, err := s.idx.GetRootCached(ctx, t.MailboxID, s.cfg.RootTTL)
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
	a := actor.From(ctx)
	if (!a.IsAdmin() && !a.IsSystem() && (a.MailboxID != mb || !a.HasRole(identity.RoleWrite))) || (a.IsAdminReader() && !a.IsAdmin()) {
		return Delivered{}, ErrForbidden
	}
	if err := actor.CheckAccount(ctx, s.ids, a, identity.RoleWrite, s.Now()); err != nil {
		return Delivered{}, ErrForbidden
	}

	return s.importMessage(ctx, mb, folderID, raw, flags, internalDate)
}

// SaveSent permits a send-only account to save its own Sent copy. It cannot
// choose another mailbox, folder or flags; ordinary imports still require write.
func (s *Service) SaveSent(ctx context.Context, mb uuid.UUID, raw []byte, internalDate time.Time) (Delivered, error) {
	a := actor.From(ctx)
	if a.MailboxID != mb || actor.CheckAccount(ctx, s.ids, a, identity.RoleSend, s.Now()) != nil {
		return Delivered{}, ErrForbidden
	}
	root, err := s.idx.EnsureRoot(ctx, mb)
	if err != nil {
		return Delivered{}, err
	}
	sent := root.FolderByRole(index.RoleSent)
	if sent == nil {
		return Delivered{}, index.ErrNotFound
	}
	return s.importMessage(ctx, mb, sent.ID, raw, []string{index.FlagSeen}, internalDate)
}

func (s *Service) importMessage(ctx context.Context, mb uuid.UUID, folderID string, raw []byte, flags []string, internalDate time.Time) (Delivered, error) {
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
	m, err := s.getMailbox(ctx, mb)
	if err != nil {
		return Delivered{}, err
	}
	limits := s.effectiveQuota(m.Quota)
	ctx = index.WithQuota(ctx, limits.HardBytes, limits.HardMessages)

	msgID := uuid.New()
	keepRaw := s.cfg.KeepRaw == "always" || (s.cfg.KeepRaw == "signed" && parsed.Signed)
	parsed.Doc.RawStored = &keepRaw
	if keepRaw {
		parsed.Doc.Wire = mail.WireMetadata(raw)
		parsed.Doc.Size = int64(len(raw))
	}
	docJSON, err := json.Marshal(parsed.Doc)
	if err != nil {
		return Delivered{}, err
	}

	// Attachments (content + reference in one transaction each) are
	// independent of the index: write them while the index allocates.
	var attErr error
	attDone := make(chan struct{})
	go func() {
		defer close(attDone)
		for _, a := range parsed.Attachments {
			if err := s.atts.Put(ctx, a.Hash, a.Data, a.ContentType, mb, msgID); err != nil {
				attErr = fmt.Errorf("delivery: attachment %s: %w", a.Hash, err)
				return
			}
		}
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
		<-attDone
		return attErr
	})
	if err != nil {
		// Nothing committed: take back what was written so a failed
		// delivery (quota, storage trouble) leaves no object behind. The
		// No counters were published before preparation. An accepted journal
		// must retain private data so another process can complete it.
		<-attDone
		if !errors.Is(err, index.ErrPending) {
			s.undoStore(ctx, mb, msgID, parsed)
		}
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

// undoStore removes the message objects and attachment references of a
// store that did not commit. Best effort, with its own deadline: the
// request context may already be gone.
func (s *Service) undoStore(ctx context.Context, mb, msgID uuid.UUID, parsed *mail.Parsed) {
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := s.msgs.Delete(uctx, mb, msgID); err != nil {
		s.log.WarnContext(uctx, "delivery: undo message object failed; the orphan sweep will remove it", slog.String("mailbox", mb.String()), slog.String("message", msgID.String()), slog.String("error", err.Error()))
	}
	for _, a := range parsed.Attachments {
		if err := s.atts.RemoveRef(uctx, a.Hash, mb, msgID); err != nil {
			s.log.WarnContext(uctx, "delivery: undo attachment reference failed; attachment gc will drop it", slog.String("hash", a.Hash), slog.String("error", err.Error()))
		}
	}
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
	case errors.Is(err, ErrUnavailable), errors.Is(err, storage.ErrUnavailable):
		return 451, "4.3.2", "Mailbox storage temporarily unavailable, try again later"
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
