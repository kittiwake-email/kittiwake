// Package mailbox is the service every mail protocol (JMAP, IMAP, POP3)
// talks to: folders, messages, flags, moves, destruction, search, change
// tracking and blobs. Authorisation (roles, mailbox ownership, legal hold)
// and audit happen here, once, for all protocols.
package mailbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/mail"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

// Errors.
var (
	ErrForbidden = errors.New("mailbox: forbidden")
	ErrHold      = errors.New("mailbox: on legal hold, read-only")
	ErrSealed    = errors.New("mailbox: sealed")
	ErrNotFound  = errors.New("mailbox: not found")
	ErrNotEmpty  = errors.New("mailbox: folder not empty")
	ErrInvalid   = errors.New("mailbox: invalid")
	ErrNoHorizon = index.ErrNoHorizon
)

// Config for the service.
type Config struct {
	TrashRetention time.Duration
	DefaultQuota   identity.QuotaLimits
	// RootTTL: how old the root returned by RootCached may be.
	RootTTL time.Duration
}

// Service is the mailbox service.
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

// New returns the service. queue may be nil.
func New(ids *identity.Store, idx *index.Store, msgs *message.Store, atts *attach.Store, q queue.Store, aud *auditor.Auditor, log *slog.Logger, cfg Config) *Service {
	if cfg.TrashRetention <= 0 {
		cfg.TrashRetention = 30 * 24 * time.Hour
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{ids: ids, idx: idx, msgs: msgs, atts: atts, queue: q, audit: aud, log: log, cfg: cfg, Now: time.Now}
}

// ---- authorisation ----

// authorize checks that the actor may perform role on mailbox mb and that
// the mailbox state allows it. write=true also enforces legal hold.
func (s *Service) authorize(ctx context.Context, mb uuid.UUID, role string, write bool) (identity.Mailbox, error) {
	a := actor.From(ctx)
	// Cached: one client's command stream re-reads the same mailbox for
	// every command; a suspend or hold applies within the identity cache TTL.
	m, err := s.ids.GetMailboxCached(ctx, mb)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return identity.Mailbox{}, ErrNotFound
		}
		return identity.Mailbox{}, err
	}
	switch {
	case a.IsAdmin():
		// Support access: allowed, audited with the admin as actor.
	case a.IsAdminReader() && !write:
		// Read-only admin: inspection only.
	case a.MailboxID == mb && a.HasRole(role):
		if m.Hold != nil && m.Hold.Sealed {
			return m, ErrSealed
		}
		if !m.Active() {
			return m, ErrForbidden // suspended or disabled: users are locked out
		}
	default:
		return m, ErrForbidden
	}
	if write && m.Hold != nil {
		return m, ErrHold
	}
	return m, nil
}

func (s *Service) deny(ctx context.Context, mb uuid.UUID, action, target string, err error) error {
	res := audit.ResultDenied
	if !errors.Is(err, ErrForbidden) && !errors.Is(err, ErrHold) && !errors.Is(err, ErrSealed) {
		res = audit.ResultError
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: action, Target: target, Detail: err.Error(), Result: res})
	return err
}

// ---- folders ----

// Root returns the mailbox root (folders, counters, modseq).
func (s *Service) Root(ctx context.Context, mb uuid.UUID) (index.Root, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return index.Root{}, s.deny(ctx, mb, "list", "", err)
	}
	return s.idx.EnsureRoot(ctx, mb)
}

// RootCached returns the root as last seen by this process when younger
// than Config.RootTTL (folder-name resolution and state strings, where a
// slightly old copy is harmless: this process's own writes refresh it, and
// a name miss falls back to a fresh read).
func (s *Service) RootCached(ctx context.Context, mb uuid.UUID) (index.Root, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return index.Root{}, s.deny(ctx, mb, "list", "", err)
	}
	return s.idx.GetRootCached(ctx, mb, s.cfg.RootTTL)
}

// ListDesc lists records newest first, starting below beforeUID (0 = the
// newest). The first page is one omap read however large the folder.
func (s *Service) ListDesc(ctx context.Context, mb uuid.UUID, folderID string, beforeUID uint32, max int) ([]index.Record, bool, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, false, s.deny(ctx, mb, "list", folderID, err)
	}
	recs, more, err := s.idx.ListFolderDesc(ctx, mb, folderID, beforeUID, max)
	if err != nil {
		return nil, false, mapIndexErr(err)
	}
	return recs, more, nil
}

// CreateFolder creates a folder.
func (s *Service) CreateFolder(ctx context.Context, mb uuid.UUID, name string) (*index.Folder, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return nil, s.deny(ctx, mb, "folder-create", name, err)
	}
	if _, err := s.idx.EnsureRoot(ctx, mb); err != nil {
		return nil, err
	}
	f, err := s.idx.CreateFolder(ctx, mb, name, "")
	if err != nil {
		return nil, s.deny(ctx, mb, "folder-create", name, mapIndexErr(err))
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "folder-create", Target: f.ID, Detail: name})
	s.notify(ctx, mb, "folder", f.ID)
	return f, nil
}

// RenameFolder renames a folder.
func (s *Service) RenameFolder(ctx context.Context, mb uuid.UUID, id, newName string) error {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return s.deny(ctx, mb, "folder-rename", id, err)
	}
	if err := s.idx.RenameFolder(ctx, mb, id, newName); err != nil {
		return s.deny(ctx, mb, "folder-rename", id, mapIndexErr(err))
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "folder-rename", Target: id, Detail: newName})
	s.notify(ctx, mb, "folder", id)
	return nil
}

// DeleteFolder deletes a folder. With removeMessages the messages go to
// retention; otherwise a non-empty folder is refused.
func (s *Service) DeleteFolder(ctx context.Context, mb uuid.UUID, id string, removeMessages bool) error {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return s.deny(ctx, mb, "folder-delete", id, err)
	}
	if removeMessages {
		for {
			recs, _, err := s.idx.ListFolder(ctx, mb, id, 0, 500)
			if err != nil {
				return s.deny(ctx, mb, "folder-delete", id, mapIndexErr(err))
			}
			if len(recs) == 0 {
				break
			}
			for _, r := range recs {
				if err := s.remove(ctx, mb, id, r.UID, "destroy"); err != nil {
					return err
				}
			}
		}
	}
	if err := s.idx.DeleteFolder(ctx, mb, id); err != nil {
		return s.deny(ctx, mb, "folder-delete", id, mapIndexErr(err))
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "folder-delete", Target: id})
	s.notify(ctx, mb, "folder", id)
	return nil
}

// SetSubscribed changes IMAP subscription state.
func (s *Service) SetSubscribed(ctx context.Context, mb uuid.UUID, id string, subscribed bool) error {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, false); err != nil { // allowed on hold: no data change
		return s.deny(ctx, mb, "folder-subscribe", id, err)
	}
	_, err := s.idx.UpdateFolder(ctx, mb, id, func(f *index.Folder) error { f.Subscribed = subscribed; return nil })
	if err != nil {
		return mapIndexErr(err)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "folder-subscribe", Target: id, Detail: fmt.Sprint(subscribed)})
	return nil
}

// ---- messages ----

// List lists records in a folder.
func (s *Service) List(ctx context.Context, mb uuid.UUID, folderID string, afterUID uint32, max int) ([]index.Record, bool, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, false, s.deny(ctx, mb, "list", folderID, err)
	}
	recs, more, err := s.idx.ListFolder(ctx, mb, folderID, afterUID, max)
	if err != nil {
		return nil, false, mapIndexErr(err)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "list", Target: folderID})
	return recs, more, nil
}

// locate resolves a message uuid to its index location via the message
// object's xattr (no per-message omap entry needed).
func (s *Service) locate(ctx context.Context, mb uuid.UUID, msg uuid.UUID) (index.Location, error) {
	loc, err := s.msgs.Location(ctx, mb, msg)
	if err != nil {
		if errors.Is(err, message.ErrNotFound) {
			return index.Location{}, ErrNotFound
		}
		return index.Location{}, err
	}
	l, err := index.ParseLocation(loc)
	if err != nil {
		return index.Location{}, ErrNotFound
	}
	return l, nil
}

// Get returns a record by message id.
func (s *Service) Get(ctx context.Context, mb uuid.UUID, msg uuid.UUID) (index.Location, index.Record, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return index.Location{}, index.Record{}, s.deny(ctx, mb, "fetch", msg.String(), err)
	}
	loc, err := s.locate(ctx, mb, msg)
	if err != nil {
		return index.Location{}, index.Record{}, err
	}
	rec, err := s.idx.GetRecord(ctx, mb, loc.FolderID, loc.UID)
	if err != nil || rec.MessageID != msg {
		return index.Location{}, index.Record{}, ErrNotFound
	}
	return loc, rec, nil
}

// GetByUID returns a record by folder and UID.
func (s *Service) GetByUID(ctx context.Context, mb uuid.UUID, folderID string, uid uint32) (index.Record, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return index.Record{}, s.deny(ctx, mb, "fetch", fmt.Sprintf("%s/%d", folderID, uid), err)
	}
	rec, err := s.idx.GetRecord(ctx, mb, folderID, uid)
	return rec, mapIndexErr(err)
}

// Document returns the parsed message.
func (s *Service) Document(ctx context.Context, mb uuid.UUID, msg uuid.UUID) (*mail.Document, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, s.deny(ctx, mb, "fetch", msg.String(), err)
	}
	data, err := s.msgs.GetDoc(ctx, mb, msg)
	if err != nil {
		if errors.Is(err, message.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var doc mail.Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("mailbox: decode document: %w", err)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "fetch", Target: msg.String()})
	return &doc, nil
}

// Raw returns the exact original bytes of a message.
func (s *Service) Raw(ctx context.Context, mb uuid.UUID, msg uuid.UUID) ([]byte, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, s.deny(ctx, mb, "fetch-raw", msg.String(), err)
	}
	return s.raw(ctx, mb, msg)
}

// raw returns the kept original bytes when present, else renders the
// message from its document (deterministic; Document.Size is its length).
func (s *Service) raw(ctx context.Context, mb uuid.UUID, msg uuid.UUID) ([]byte, error) {
	if kept, err := s.msgs.GetRaw(ctx, mb, msg); err == nil {
		s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "fetch-raw", Target: msg.String()})
		return kept, nil
	} else if !errors.Is(err, message.ErrNotFound) {
		return nil, err
	}
	data, err := s.msgs.GetDoc(ctx, mb, msg)
	if err != nil {
		if errors.Is(err, message.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var doc mail.Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("mailbox: decode document %s: %w", msg, err)
	}
	out, err := mail.Render(&doc, func(h string) ([]byte, error) { return s.atts.Get(ctx, mb, h) })
	if err != nil {
		return nil, fmt.Errorf("mailbox: render %s: %w", msg, err)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "fetch-raw", Target: msg.String(), Detail: "message-id=" + doc.MessageID})
	return out, nil
}

// Attachment returns attachment content by hash and its content type.
func (s *Service) Attachment(ctx context.Context, mb uuid.UUID, hash string) ([]byte, string, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, "", s.deny(ctx, mb, "fetch-attachment", hash, err)
	}
	if len(hash) != 64 {
		return nil, "", ErrNotFound
	}
	data, err := s.atts.Get(ctx, mb, hash)
	if err != nil {
		if errors.Is(err, attach.ErrNotFound) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	ct := "application/octet-stream"
	if info, err := s.atts.Stat(ctx, mb, hash); err == nil && info.ContentType != "" {
		ct = info.ContentType
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "fetch-attachment", Target: hash})
	return data, ct, nil
}

// Changes returns the change log since a modseq.
func (s *Service) Changes(ctx context.Context, mb uuid.UUID, since uint64, max int) ([]index.Change, bool, uint64, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, false, 0, s.deny(ctx, mb, "changes", "", err)
	}
	if _, err := s.idx.EnsureRoot(ctx, mb); err != nil {
		return nil, false, 0, err
	}
	ch, more, cur, err := s.idx.Changes(ctx, mb, since, max)
	return ch, more, cur, err
}

// Thread returns the locations of a thread's messages.
func (s *Service) Thread(ctx context.Context, mb uuid.UUID, tid string) ([]index.Location, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, s.deny(ctx, mb, "thread", tid, err)
	}
	locs, err := s.idx.ThreadMembers(ctx, mb, tid)
	return locs, mapIndexErr(err)
}

// FlagOp describes a flag change.
type FlagOp struct {
	Add     []string
	Remove  []string
	Replace []string // when non-nil, replaces all flags (Add/Remove ignored)
}

// SetFlags changes a message's flags.
func (s *Service) SetFlags(ctx context.Context, mb uuid.UUID, msg uuid.UUID, op FlagOp) (index.Record, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return index.Record{}, s.deny(ctx, mb, "flag-change", msg.String(), err)
	}
	loc, err := s.locate(ctx, mb, msg)
	if err != nil {
		return index.Record{}, err
	}
	return s.setFlagsAt(ctx, mb, loc.FolderID, loc.UID, op)
}

// SetFlagsByUID changes flags by folder/UID (IMAP STORE).
func (s *Service) SetFlagsByUID(ctx context.Context, mb uuid.UUID, folderID string, uid uint32, op FlagOp) (index.Record, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return index.Record{}, s.deny(ctx, mb, "flag-change", fmt.Sprintf("%s/%d", folderID, uid), err)
	}
	return s.setFlagsAt(ctx, mb, folderID, uid, op)
}

func (s *Service) setFlagsAt(ctx context.Context, mb uuid.UUID, folderID string, uid uint32, op FlagOp) (index.Record, error) {
	for _, f := range append(append([]string{}, op.Add...), op.Replace...) {
		if err := validateFlag(f); err != nil {
			return index.Record{}, err
		}
	}
	rec, err := s.idx.SetFlags(ctx, mb, folderID, uid, func(flags []string) []string {
		if op.Replace != nil {
			return op.Replace
		}
		out := flags[:0:0]
		for _, f := range flags {
			if !containsFold(op.Remove, f) {
				out = append(out, f)
			}
		}
		for _, f := range op.Add {
			if !containsFold(out, f) {
				out = append(out, f)
			}
		}
		return out
	})
	if err != nil {
		return index.Record{}, mapIndexErr(err)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "flag-change", Target: fmt.Sprintf("%s/%d", folderID, uid), Detail: strings.Join(rec.Flags, " ")})
	s.notify(ctx, mb, "email", folderID)
	return rec, nil
}

func validateFlag(f string) error {
	if f == "" || len(f) > 255 || strings.ContainsAny(f, " ()[]{}\"\\%*\x00\r\n") && !strings.HasPrefix(f, `\`) {
		return fmt.Errorf("%w: flag %q", ErrInvalid, f)
	}
	if strings.HasPrefix(f, `\`) {
		switch strings.ToLower(f) {
		case `\seen`, `\answered`, `\flagged`, `\deleted`, `\draft`:
		default:
			return fmt.Errorf("%w: system flag %q", ErrInvalid, f)
		}
	}
	return nil
}

func containsFold(list []string, v string) bool {
	for _, x := range list {
		if strings.EqualFold(x, v) {
			return true
		}
	}
	return false
}

// Move moves a message to another folder.
func (s *Service) Move(ctx context.Context, mb uuid.UUID, msg uuid.UUID, toFolder string) (index.Record, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return index.Record{}, s.deny(ctx, mb, "move", msg.String(), err)
	}
	loc, err := s.locate(ctx, mb, msg)
	if err != nil {
		return index.Record{}, err
	}
	return s.moveAt(ctx, mb, loc.FolderID, loc.UID, toFolder)
}

// MoveByUID moves by folder/UID (IMAP MOVE).
func (s *Service) MoveByUID(ctx context.Context, mb uuid.UUID, folderID string, uid uint32, toFolder string) (index.Record, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return index.Record{}, s.deny(ctx, mb, "move", fmt.Sprintf("%s/%d", folderID, uid), err)
	}
	return s.moveAt(ctx, mb, folderID, uid, toFolder)
}

func (s *Service) moveAt(ctx context.Context, mb uuid.UUID, folderID string, uid uint32, toFolder string) (index.Record, error) {
	rec, err := s.idx.Move(ctx, mb, folderID, uid, toFolder, func(loc index.Location) error {
		cur, err := s.idx.GetRecord(ctx, mb, folderID, uid)
		if err != nil {
			return err
		}
		return s.msgs.SetLocation(ctx, mb, cur.MessageID, loc.String())
	})
	if err != nil {
		return index.Record{}, mapIndexErr(err)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "move", Target: fmt.Sprintf("%s/%d", folderID, uid), Detail: fmt.Sprintf("to %s/%d", toFolder, rec.UID)})
	s.notify(ctx, mb, "email", toFolder)
	return rec, nil
}

// Destroy removes a message from its folder into retention (JMAP
// Email/set destroy). Nothing is deleted immediately.
func (s *Service) Destroy(ctx context.Context, mb uuid.UUID, msg uuid.UUID) error {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return s.deny(ctx, mb, "destroy", msg.String(), err)
	}
	loc, err := s.locate(ctx, mb, msg)
	if err != nil {
		return err
	}
	return s.remove(ctx, mb, loc.FolderID, loc.UID, "destroy")
}

// Expunge removes messages flagged \Deleted from a folder into retention
// (IMAP EXPUNGE). Returns the expunged UIDs.
func (s *Service) Expunge(ctx context.Context, mb uuid.UUID, folderID string, only []uint32) ([]uint32, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleWrite, true); err != nil {
		return nil, s.deny(ctx, mb, "expunge", folderID, err)
	}
	var out []uint32
	var after uint32
	for {
		recs, more, err := s.idx.ListFolder(ctx, mb, folderID, after, 500)
		if err != nil {
			return out, mapIndexErr(err)
		}
		for _, r := range recs {
			after = r.UID
			if !r.HasFlag(index.FlagDeleted) {
				continue
			}
			if only != nil && !containsUID(only, r.UID) {
				continue
			}
			if err := s.remove(ctx, mb, folderID, r.UID, "expunge"); err != nil {
				return out, err
			}
			out = append(out, r.UID)
		}
		if !more {
			break
		}
	}
	return out, nil
}

func containsUID(list []uint32, u uint32) bool {
	for _, x := range list {
		if x == u {
			return true
		}
	}
	return false
}

func (s *Service) remove(ctx context.Context, mb uuid.UUID, folderID string, uid uint32, reason string) error {
	entry, err := s.idx.Remove(ctx, mb, folderID, uid, reason, s.Now().Add(s.cfg.TrashRetention))
	if err != nil {
		return mapIndexErr(err)
	}
	if s.queue != nil {
		_, _ = s.queue.Enqueue(ctx, "search-index", map[string]any{"mailbox": mb, "message": entry.MessageID, "op": "remove"}, time.Time{})
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: reason, Target: fmt.Sprintf("%s/%d", folderID, uid), Detail: "msg=" + entry.MessageID.String()})
	s.notify(ctx, mb, "email", folderID)
	return nil
}

// ---- query ----

// Filter is a JMAP-style Email/query filter (the subset served from the
// index without a search backend).
type Filter struct {
	InFolder      string
	Text          string // subject, from or preview contains
	Subject       string
	From          string
	HasKeyword    string
	NotKeyword    string
	Before        time.Time // receivedAt <
	After         time.Time // receivedAt >=
	HasAttachment *bool
	MinSize       int64
	MaxSize       int64
}

// Sort options.
const (
	SortReceivedAt = "receivedAt"
	SortSentAt     = "sentAt"
	SortSubject    = "subject"
	SortFrom       = "from"
	SortSize       = "size"
)

// Query describes an Email/query.
type Query struct {
	Filter   Filter
	Sort     string
	Asc      bool
	Position int
	Limit    int
	// CollapseThreads keeps only the newest message per thread.
	CollapseThreads bool
}

// QueryResult is the result of Query.
type QueryResult struct {
	Records []index.Record
	Folders []string // folder id per record (parallel to Records)
	Total   int
	ModSeq  uint64
}

// Query evaluates a query over the index.
func (s *Service) Query(ctx context.Context, mb uuid.UUID, q Query) (QueryResult, error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return QueryResult{}, s.deny(ctx, mb, "search", "", err)
	}
	root, err := s.idx.EnsureRoot(ctx, mb)
	if err != nil {
		return QueryResult{}, err
	}
	var folders []*index.Folder
	if q.Filter.InFolder != "" {
		f := root.FolderByID(q.Filter.InFolder)
		if f == nil {
			return QueryResult{}, ErrNotFound
		}
		folders = []*index.Folder{f}
	} else {
		folders = root.Folders
	}
	type item struct {
		rec index.Record
		fid string
	}
	// Fast path: the default listing (one folder, newest first, no filter
	// beyond the folder, no thread collapsing) reads only the page it
	// needs from the newest-first index instead of the whole folder.
	if q.Filter.InFolder != "" && !q.Asc && (q.Sort == "" || q.Sort == SortReceivedAt) && !q.CollapseThreads && q.Position >= 0 && q.Limit > 0 && q.Filter == (Filter{InFolder: q.Filter.InFolder}) {
		f := folders[0]
		recs, _, err := s.idx.ListFolderDesc(ctx, mb, f.ID, 0, q.Position+q.Limit)
		if err != nil {
			return QueryResult{}, mapIndexErr(err)
		}
		res := QueryResult{Total: int(f.Total), ModSeq: root.ModSeq}
		if q.Position < len(recs) {
			for _, r := range recs[q.Position:] {
				res.Records = append(res.Records, r)
				res.Folders = append(res.Folders, f.ID)
			}
		}
		s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "search", Target: f.ID})
		return res, nil
	}
	var items []item
	for _, f := range folders {
		var after uint32
		for {
			recs, more, err := s.idx.ListFolder(ctx, mb, f.ID, after, 1000)
			if err != nil {
				return QueryResult{}, mapIndexErr(err)
			}
			for _, r := range recs {
				after = r.UID
				if matches(q.Filter, r) {
					items = append(items, item{r, f.ID})
				}
			}
			if !more {
				break
			}
		}
	}
	less := func(a, b item) bool {
		switch q.Sort {
		case SortSentAt:
			return a.rec.Date.Before(b.rec.Date)
		case SortSubject:
			return strings.ToLower(a.rec.Subject) < strings.ToLower(b.rec.Subject)
		case SortFrom:
			return strings.ToLower(a.rec.From) < strings.ToLower(b.rec.From)
		case SortSize:
			return a.rec.Size < b.rec.Size
		default:
			return a.rec.InternalDate.Before(b.rec.InternalDate)
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if q.Asc {
			return less(items[i], items[j])
		}
		return less(items[j], items[i])
	})
	if q.CollapseThreads {
		seen := map[string]bool{}
		out := items[:0]
		for _, it := range items {
			if !seen[it.rec.ThreadID] {
				seen[it.rec.ThreadID] = true
				out = append(out, it)
			}
		}
		items = out
	}
	total := len(items)
	pos := q.Position
	if pos < 0 {
		pos = max(0, total+pos)
	}
	if pos > total {
		pos = total
	}
	end := total
	if q.Limit > 0 && pos+q.Limit < end {
		end = pos + q.Limit
	}
	res := QueryResult{Total: total, ModSeq: root.ModSeq}
	for _, it := range items[pos:end] {
		res.Records = append(res.Records, it.rec)
		res.Folders = append(res.Folders, it.fid)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "search", Target: q.Filter.InFolder})
	return res, nil
}

func matches(f Filter, r index.Record) bool {
	has := func(hay, needle string) bool {
		return needle == "" || strings.Contains(strings.ToLower(hay), strings.ToLower(needle))
	}
	if f.Text != "" && !(has(r.Subject, f.Text) || has(r.From, f.Text)) {
		return false
	}
	if !has(r.Subject, f.Subject) || !has(r.From, f.From) {
		return false
	}
	if f.HasKeyword != "" && !r.HasFlag(f.HasKeyword) {
		return false
	}
	if f.NotKeyword != "" && r.HasFlag(f.NotKeyword) {
		return false
	}
	if !f.Before.IsZero() && !r.InternalDate.Before(f.Before) {
		return false
	}
	if !f.After.IsZero() && r.InternalDate.Before(f.After) {
		return false
	}
	if f.HasAttachment != nil && r.HasAttachments != *f.HasAttachment {
		return false
	}
	if f.MinSize > 0 && r.Size < f.MinSize {
		return false
	}
	if f.MaxSize > 0 && r.Size > f.MaxSize {
		return false
	}
	return true
}

// ---- quota ----

// QuotaInfo is usage and effective limits.
type QuotaInfo struct {
	Usage  index.Usage
	Limits identity.QuotaLimits
}

// Quota returns usage and limits for the mailbox.
func (s *Service) Quota(ctx context.Context, mb uuid.UUID) (QuotaInfo, error) {
	m, err := s.authorize(ctx, mb, identity.RoleRead, false)
	if err != nil {
		return QuotaInfo{}, s.deny(ctx, mb, "quota", "", err)
	}
	root, err := s.idx.EnsureRoot(ctx, mb)
	if err != nil {
		return QuotaInfo{}, err
	}
	lim := m.Quota
	d := s.cfg.DefaultQuota
	if lim.SoftBytes == 0 {
		lim.SoftBytes = d.SoftBytes
	}
	if lim.HardBytes == 0 {
		lim.HardBytes = d.HardBytes
	}
	if lim.SoftMessages == 0 {
		lim.SoftMessages = d.SoftMessages
	}
	if lim.HardMessages == 0 {
		lim.HardMessages = d.HardMessages
	}
	return QuotaInfo{Usage: root.Usage, Limits: lim}, nil
}

// ---- notifications ----

func (s *Service) notify(ctx context.Context, mb uuid.UUID, kind, id string) {
	payload, _ := json.Marshal(map[string]string{"type": kind, "id": id})
	if err := s.idx.Notify(ctx, mb, payload); err != nil {
		s.log.DebugContext(ctx, "notify failed", slog.String("error", err.Error()))
	}
}

// Watch subscribes to change notifications for a mailbox (IMAP IDLE, JMAP push).
func (s *Service) Watch(ctx context.Context, mb uuid.UUID) (<-chan []byte, func(), error) {
	if _, err := s.authorize(ctx, mb, identity.RoleRead, false); err != nil {
		return nil, nil, err
	}
	if _, err := s.idx.EnsureRoot(ctx, mb); err != nil {
		return nil, nil, err
	}
	w, err := s.idx.Watch(ctx, mb)
	if err != nil {
		return nil, nil, err
	}
	return w.Events(), func() { _ = w.Close() }, nil
}

func mapIndexErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, index.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, index.ErrNotEmpty):
		return ErrNotEmpty
	case errors.Is(err, index.ErrInvalid), errors.Is(err, index.ErrExists):
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return err
}
