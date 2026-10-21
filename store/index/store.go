package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

const (
	maxRootAttempts  = 64
	rootVersion      = 1
	changeLogKeep    = 10000 // entries kept per mailbox
	changeTrimEvery  = 256   // trim when modseq crosses a multiple of this
	maxFolderDepth   = 16
	maxFolderNameLen = 255
	uidKeyWidth      = 10
	modseqKeyWidth   = 20
)

// Store is the mailbox index store on the index pool.
type Store struct {
	io  rados.IOContext
	Now func() time.Time
}

// New returns a store on the index pool context.
func New(indexPool rados.IOContext) *Store {
	return &Store{io: indexPool, Now: time.Now}
}

// ---- object names and keys ----

func rootOID(mb uuid.UUID) string               { return mb.String() }
func folderOID(mb uuid.UUID, fid string) string { return mb.String() + "/" + fid }
func changesOID(mb uuid.UUID) string            { return mb.String() + "/changes" }
func retentionOID(mb uuid.UUID) string          { return mb.String() + "/retention" }

func uidKey(uid uint32) string   { return fmt.Sprintf("%0*d", uidKeyWidth, uid) }
func modseqKey(m uint64) string  { return fmt.Sprintf("%0*d", modseqKeyWidth, m) }
func msgKey(id uuid.UUID) string { return "msg/" + id.String() }
func msgidKey(h string) string   { return "msgid/" + strings.ToLower(h) }
func threadKey(tid string, id uuid.UUID) string {
	return "thread/" + tid + "/" + id.String()
}
func retentionKey(purgeAt time.Time, id uuid.UUID) string {
	return fmt.Sprintf("%020d/%s", purgeAt.Unix(), id.String())
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, rados.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, rados.ErrExists):
		return ErrExists
	case errors.Is(err, rados.ErrVersionMismatch):
		return ErrConflict
	}
	return err
}

// ---- root ----

type versionedRoot struct {
	root    Root
	version uint64
}

func (s *Store) getRoot(ctx context.Context, mb uuid.UUID) (versionedRoot, error) {
	data, ver, err := s.io.Get(ctx, rootOID(mb))
	if err != nil {
		return versionedRoot{}, mapErr(err)
	}
	var r Root
	if err := json.Unmarshal(data, &r); err != nil {
		return versionedRoot{}, fmt.Errorf("index: decode root %s: %w", mb, err)
	}
	return versionedRoot{root: r, version: ver}, nil
}

// GetRoot returns the mailbox root. ErrNotFound if the mailbox index does
// not exist yet (see EnsureRoot).
func (s *Store) GetRoot(ctx context.Context, mb uuid.UUID) (Root, error) {
	vr, err := s.getRoot(ctx, mb)
	return vr.root, err
}

// updateRoot applies fn under optimistic concurrency with retries. fn may
// return a value that is passed through on success.
func (s *Store) updateRoot(ctx context.Context, mb uuid.UUID, fn func(r *Root) error) (Root, error) {
	for attempt := 0; attempt < maxRootAttempts; attempt++ {
		if attempt > 0 {
			if err := backoff(ctx, attempt); err != nil {
				return Root{}, err
			}
		}
		vr, err := s.getRoot(ctx, mb)
		if err != nil {
			return Root{}, err
		}
		r := vr.root
		// Deep-copy folders so fn can mutate freely even if we retry.
		r.Folders = cloneFolders(vr.root.Folders)
		if err := fn(&r); err != nil {
			return Root{}, err
		}
		data, err := json.Marshal(&r)
		if err != nil {
			return Root{}, err
		}
		_, err = s.io.Write(ctx, rootOID(mb), rados.NewWriteTx().AssertVersion(vr.version).WriteFull(data))
		if err == nil {
			return r, nil
		}
		if !errors.Is(err, rados.ErrVersionMismatch) {
			return Root{}, mapErr(err)
		}
	}
	return Root{}, ErrConflict
}

// backoff sleeps a jittered, exponentially growing delay (capped) before a
// retry of an optimistic root update.
func backoff(ctx context.Context, attempt int) error {
	d := time.Duration(1<<uint(min(attempt, 6))) * 500 * time.Microsecond // 1ms .. 32ms
	d += time.Duration(rand.Int64N(int64(d)))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func cloneFolders(in []*Folder) []*Folder {
	out := make([]*Folder, len(in))
	for i, f := range in {
		c := *f
		c.Keywords = append([]string(nil), f.Keywords...)
		out[i] = &c
	}
	return out
}

// DefaultFolders is the folder set created for a new mailbox.
var DefaultFolders = []struct{ Name, Role string }{
	{"INBOX", RoleInbox},
	{"Drafts", RoleDrafts},
	{"Sent", RoleSent},
	{"Junk", RoleJunk},
	{"Trash", RoleTrash},
	{"Archive", RoleArchive},
}

// EnsureRoot creates the mailbox root with the default folders if it does
// not exist, and returns it.
func (s *Store) EnsureRoot(ctx context.Context, mb uuid.UUID) (Root, error) {
	if vr, err := s.getRoot(ctx, mb); err == nil {
		return vr.root, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Root{}, err
	}
	now := s.Now().UTC()
	r := Root{Version: rootVersion, MailboxID: mb, NextFolderID: 1, ModSeq: 0, Horizon: 1, Created: now}
	validity := uint32(now.Unix())
	for _, d := range DefaultFolders {
		r.ModSeq++
		r.Folders = append(r.Folders, &Folder{
			ID: fmt.Sprintf("f%d", r.NextFolderID), Type: CollectionMailFolder, Name: d.Name, Role: d.Role,
			UIDValidity: validity, UIDNext: 1, ModSeq: r.ModSeq, Subscribed: true, Created: now,
		})
		r.NextFolderID++
	}
	data, err := json.Marshal(&r)
	if err != nil {
		return Root{}, err
	}
	_, err = s.io.Write(ctx, rootOID(mb), rados.NewWriteTx().CreateExclusive().WriteFull(data))
	if errors.Is(err, rados.ErrExists) {
		vr, err := s.getRoot(ctx, mb)
		return vr.root, err
	}
	if err != nil {
		return Root{}, mapErr(err)
	}
	// Folder objects and the change log exist as empty objects so omap reads
	// never hit ENOENT; record folder creations in the change log.
	for _, f := range r.Folders {
		if _, err := s.io.Write(ctx, folderOID(mb, f.ID), rados.NewWriteTx().WriteFull(nil)); err != nil {
			return Root{}, mapErr(err)
		}
		if err := s.logChange(ctx, mb, Change{ModSeq: f.ModSeq, Kind: KindFolder, Op: OpCreated, ID: f.ID, Timestamp: now}); err != nil {
			return Root{}, err
		}
	}
	if _, err := s.io.Write(ctx, retentionOID(mb), rados.NewWriteTx().WriteFull(nil)); err != nil {
		return Root{}, mapErr(err)
	}
	return r, nil
}

// Exists reports whether the mailbox index exists.
func (s *Store) Exists(ctx context.Context, mb uuid.UUID) (bool, error) {
	_, err := s.io.Stat(ctx, rootOID(mb))
	if errors.Is(err, rados.ErrNotFound) {
		return false, nil
	}
	return err == nil, mapErr(err)
}

// ---- change log ----

func (s *Store) logChange(ctx context.Context, mb uuid.UUID, c Change) error {
	data, err := json.Marshal(&c)
	if err != nil {
		return err
	}
	_, err = s.io.Write(ctx, changesOID(mb), rados.NewWriteTx().OmapSet(map[string][]byte{modseqKey(c.ModSeq): data}))
	return mapErr(err)
}

// Changes returns changes with modseq > since, in order, up to max. current
// is the mailbox's current modseq. ErrNoHorizon if since is older than the
// log retains (client must resync).
func (s *Store) Changes(ctx context.Context, mb uuid.UUID, since uint64, max int) (changes []Change, more bool, current uint64, err error) {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		return nil, false, 0, err
	}
	if since+1 < vr.root.Horizon {
		return nil, false, vr.root.ModSeq, ErrNoHorizon
	}
	if since >= vr.root.ModSeq {
		return nil, false, vr.root.ModSeq, nil
	}
	kv, more, err := s.io.OmapList(ctx, changesOID(mb), "", modseqKey(since), max)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, false, vr.root.ModSeq, nil
		}
		return nil, false, 0, mapErr(err)
	}
	for _, e := range kv {
		var c Change
		if err := json.Unmarshal(e.Value, &c); err != nil {
			return nil, false, 0, fmt.Errorf("index: decode change: %w", err)
		}
		changes = append(changes, c)
	}
	return changes, more, vr.root.ModSeq, nil
}

// TrimChanges drops change log entries older than the newest keep entries
// and advances the horizon. Called opportunistically after writes.
func (s *Store) TrimChanges(ctx context.Context, mb uuid.UUID, keep int) error {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		return err
	}
	if vr.root.ModSeq <= uint64(keep) {
		return nil
	}
	newHorizon := vr.root.ModSeq - uint64(keep) + 1
	if newHorizon <= vr.root.Horizon {
		return nil
	}
	// Remove keys < newHorizon in batches.
	for {
		kv, _, err := s.io.OmapList(ctx, changesOID(mb), "", "", 512)
		if err != nil {
			return mapErr(err)
		}
		var rm []string
		for _, e := range kv {
			if e.Key < modseqKey(newHorizon) {
				rm = append(rm, e.Key)
			}
		}
		if len(rm) == 0 {
			break
		}
		if _, err := s.io.Write(ctx, changesOID(mb), rados.NewWriteTx().OmapRemove(rm...)); err != nil {
			return mapErr(err)
		}
		if len(rm) < len(kv) {
			break
		}
	}
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		if newHorizon > r.Horizon {
			r.Horizon = newHorizon
		}
		return nil
	})
	return err
}

// ---- folders ----

// ValidateFolderName checks a full folder name.
func ValidateFolderName(name string) error {
	if name == "" || len(name) > maxFolderNameLen*maxFolderDepth {
		return fmt.Errorf("%w: folder name length", ErrInvalid)
	}
	if strings.HasPrefix(name, Delimiter) || strings.HasSuffix(name, Delimiter) || strings.Contains(name, Delimiter+Delimiter) {
		return fmt.Errorf("%w: folder name %q", ErrInvalid, name)
	}
	parts := strings.Split(name, Delimiter)
	if len(parts) > maxFolderDepth {
		return fmt.Errorf("%w: folder too deep", ErrInvalid)
	}
	for _, p := range parts {
		if len(p) > maxFolderNameLen || strings.ContainsAny(p, "\x00\r\n") {
			return fmt.Errorf("%w: folder component %q", ErrInvalid, p)
		}
	}
	if strings.EqualFold(parts[0], "INBOX") && parts[0] != "INBOX" {
		return fmt.Errorf("%w: INBOX is case-insensitive; use INBOX", ErrInvalid)
	}
	return nil
}

// CreateFolder creates a folder with the full name. Missing parents are
// created too. Returns the new folder.
func (s *Store) CreateFolder(ctx context.Context, mb uuid.UUID, name, role string) (*Folder, error) {
	if err := ValidateFolderName(name); err != nil {
		return nil, err
	}
	var created []*Folder
	now := s.Now().UTC()
	_, err := s.updateRoot(ctx, mb, func(r *Root) error {
		created = nil
		if r.FolderByName(name) != nil {
			return ErrExists
		}
		if role != "" && r.FolderByRole(role) != nil {
			return fmt.Errorf("%w: role %s already assigned", ErrExists, role)
		}
		parts := strings.Split(name, Delimiter)
		for i := 1; i <= len(parts); i++ {
			n := strings.Join(parts[:i], Delimiter)
			if r.FolderByName(n) != nil {
				continue
			}
			r.ModSeq++
			f := &Folder{
				ID: fmt.Sprintf("f%d", r.NextFolderID), Type: CollectionMailFolder, Name: n,
				UIDValidity: uint32(now.Unix()), UIDNext: 1, ModSeq: r.ModSeq, Subscribed: true, Created: now,
			}
			if i == len(parts) {
				f.Role = role
			}
			r.NextFolderID++
			r.Folders = append(r.Folders, f)
			created = append(created, f)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, f := range created {
		if _, err := s.io.Write(ctx, folderOID(mb, f.ID), rados.NewWriteTx().WriteFull(nil)); err != nil {
			return nil, mapErr(err)
		}
		if err := s.logChange(ctx, mb, Change{ModSeq: f.ModSeq, Kind: KindFolder, Op: OpCreated, ID: f.ID, Timestamp: now}); err != nil {
			return nil, err
		}
	}
	return created[len(created)-1], nil
}

// RenameFolder renames a folder (and its children) to newName.
func (s *Store) RenameFolder(ctx context.Context, mb uuid.UUID, id, newName string) error {
	if err := ValidateFolderName(newName); err != nil {
		return err
	}
	var changed []Change
	now := s.Now().UTC()
	_, err := s.updateRoot(ctx, mb, func(r *Root) error {
		changed = nil
		f := r.FolderByID(id)
		if f == nil {
			return ErrNotFound
		}
		if f.Role == RoleInbox {
			return fmt.Errorf("%w: INBOX cannot be renamed", ErrInvalid)
		}
		if r.FolderByName(newName) != nil {
			return ErrExists
		}
		if strings.HasPrefix(newName, f.Name+Delimiter) {
			return fmt.Errorf("%w: cannot move a folder under itself", ErrInvalid)
		}
		// Parent of the new name must exist (or be created).
		if i := strings.LastIndex(newName, Delimiter); i > 0 {
			parent := newName[:i]
			if r.FolderByName(parent) == nil {
				return fmt.Errorf("%w: parent folder %q", ErrNotFound, parent)
			}
		}
		old := f.Name
		for _, g := range r.Folders {
			if g.ID == f.ID || strings.HasPrefix(g.Name, old+Delimiter) {
				g.Name = newName + strings.TrimPrefix(g.Name, old)
				r.ModSeq++
				g.ModSeq = r.ModSeq
				changed = append(changed, Change{ModSeq: r.ModSeq, Kind: KindFolder, Op: OpUpdated, ID: g.ID, Timestamp: now})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, c := range changed {
		if err := s.logChange(ctx, mb, c); err != nil {
			return err
		}
	}
	return nil
}

// DeleteFolder removes an empty folder without children. Callers move or
// remove the messages first.
func (s *Store) DeleteFolder(ctx context.Context, mb uuid.UUID, id string) error {
	var ch Change
	now := s.Now().UTC()
	_, err := s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(id)
		if f == nil {
			return ErrNotFound
		}
		if f.Role != "" {
			return fmt.Errorf("%w: folder with role %s cannot be deleted", ErrInvalid, f.Role)
		}
		if f.Total != 0 {
			return ErrNotEmpty
		}
		for _, g := range r.Folders {
			if strings.HasPrefix(g.Name, f.Name+Delimiter) {
				return fmt.Errorf("%w: folder has children", ErrNotEmpty)
			}
		}
		out := r.Folders[:0]
		for _, g := range r.Folders {
			if g.ID != id {
				out = append(out, g)
			}
		}
		r.Folders = out
		r.ModSeq++
		ch = Change{ModSeq: r.ModSeq, Kind: KindFolder, Op: OpDestroyed, ID: id, Timestamp: now}
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.io.Delete(ctx, folderOID(mb, id)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	return s.logChange(ctx, mb, ch)
}

// UpdateFolder applies fn to folder metadata (subscription, keywords) and
// logs an update.
func (s *Store) UpdateFolder(ctx context.Context, mb uuid.UUID, id string, fn func(f *Folder) error) (*Folder, error) {
	var out *Folder
	var ch Change
	now := s.Now().UTC()
	_, err := s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(id)
		if f == nil {
			return ErrNotFound
		}
		name, role := f.Name, f.Role
		if err := fn(f); err != nil {
			return err
		}
		f.Name, f.Role = name, role // not changeable here
		r.ModSeq++
		f.ModSeq = r.ModSeq
		c := *f
		out = &c
		ch = Change{ModSeq: r.ModSeq, Kind: KindFolder, Op: OpUpdated, ID: id, Timestamp: now}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, s.logChange(ctx, mb, ch)
}

// ---- messages ----

func (s *Store) getRecord(ctx context.Context, mb uuid.UUID, fid string, uid uint32) (Record, error) {
	m, err := s.io.OmapGet(ctx, folderOID(mb, fid), []string{uidKey(uid)})
	if err != nil {
		return Record{}, mapErr(err)
	}
	v, ok := m[uidKey(uid)]
	if !ok {
		return Record{}, ErrNotFound
	}
	var rec Record
	if err := json.Unmarshal(v, &rec); err != nil {
		return Record{}, fmt.Errorf("index: decode record: %w", err)
	}
	return rec, nil
}

func (s *Store) putRecord(ctx context.Context, mb uuid.UUID, fid string, rec Record) error {
	data, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	_, err = s.io.Write(ctx, folderOID(mb, fid), rados.NewWriteTx().OmapSet(map[string][]byte{uidKey(rec.UID): data}))
	return mapErr(err)
}

// GetRecord returns one record.
func (s *Store) GetRecord(ctx context.Context, mb uuid.UUID, fid string, uid uint32) (Record, error) {
	return s.getRecord(ctx, mb, fid, uid)
}

// Locate finds where a message (by message uuid) lives.
func (s *Store) Locate(ctx context.Context, mb uuid.UUID, msg uuid.UUID) (Location, error) {
	m, err := s.io.OmapGet(ctx, rootOID(mb), []string{msgKey(msg)})
	if err != nil {
		return Location{}, mapErr(err)
	}
	v, ok := m[msgKey(msg)]
	if !ok {
		return Location{}, ErrNotFound
	}
	return parseLocation(string(v))
}

func parseLocation(s string) (Location, error) {
	i := strings.LastIndex(s, "/")
	if i < 0 {
		return Location{}, fmt.Errorf("index: corrupt location %q", s)
	}
	var uid uint32
	if _, err := fmt.Sscanf(s[i+1:], "%d", &uid); err != nil {
		return Location{}, fmt.Errorf("index: corrupt location %q", s)
	}
	return Location{FolderID: s[:i], UID: uid}, nil
}

func (l Location) String() string { return fmt.Sprintf("%s/%d", l.FolderID, l.UID) }

// GetByMessageID returns a record by message uuid together with its folder.
func (s *Store) GetByMessageID(ctx context.Context, mb uuid.UUID, msg uuid.UUID) (Location, Record, error) {
	loc, err := s.Locate(ctx, mb, msg)
	if err != nil {
		return Location{}, Record{}, err
	}
	rec, err := s.getRecord(ctx, mb, loc.FolderID, loc.UID)
	return loc, rec, err
}

// ListFolder lists records with UID > afterUID in UID order.
func (s *Store) ListFolder(ctx context.Context, mb uuid.UUID, fid string, afterUID uint32, max int) (records []Record, more bool, err error) {
	after := ""
	if afterUID > 0 {
		after = uidKey(afterUID)
	}
	kv, more, err := s.io.OmapList(ctx, folderOID(mb, fid), "", after, max)
	if err != nil {
		return nil, false, mapErr(err)
	}
	for _, e := range kv {
		var rec Record
		if err := json.Unmarshal(e.Value, &rec); err != nil {
			return nil, false, fmt.Errorf("index: decode record: %w", err)
		}
		records = append(records, rec)
	}
	return records, more, nil
}

// threadFor determines the thread id for a new record, from its own
// Message-ID and the references it carries, consulting the root omap.
func (s *Store) threadFor(ctx context.Context, mb uuid.UUID, rec *Record) (string, error) {
	var keys []string
	for _, id := range rec.InReplyTo {
		keys = append(keys, msgidKey(id))
	}
	for _, id := range rec.References {
		keys = append(keys, msgidKey(id))
	}
	if rec.MsgIDHeader != "" {
		keys = append(keys, msgidKey(rec.MsgIDHeader))
	}
	if len(keys) > 0 {
		m, err := s.io.OmapGet(ctx, rootOID(mb), keys)
		if err != nil {
			return "", mapErr(err)
		}
		// Prefer the most direct parent.
		for _, k := range keys {
			if v, ok := m[k]; ok && len(v) > 0 {
				return string(v), nil
			}
		}
	}
	return rec.MessageID.String(), nil
}

// Append adds a record to a folder: allocates UID and modseq, updates
// counters and usage, writes the record, the root maps and the change log.
// rec.MessageID must be set (the message objects already exist).
func (s *Store) Append(ctx context.Context, mb uuid.UUID, fid string, rec Record) (Record, error) {
	if rec.MessageID == uuid.Nil {
		return Record{}, fmt.Errorf("%w: record needs a message id", ErrInvalid)
	}
	now := s.Now().UTC()
	if rec.InternalDate.IsZero() {
		rec.InternalDate = now
	}
	tid, err := s.threadFor(ctx, mb, &rec)
	if err != nil {
		return Record{}, err
	}
	rec.ThreadID = tid
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(fid)
		if f == nil {
			return ErrNotFound
		}
		r.ModSeq++
		rec.UID = f.UIDNext
		rec.ModSeq = r.ModSeq
		f.UIDNext++
		f.ModSeq = r.ModSeq
		f.Total++
		f.Size += rec.Size
		if !rec.HasFlag(FlagSeen) {
			f.Unseen++
		}
		for _, fl := range rec.Flags {
			if !strings.HasPrefix(fl, `\`) && !containsFold(f.Keywords, fl) {
				f.Keywords = append(f.Keywords, fl)
			}
		}
		r.Usage.Bytes += rec.Size
		r.Usage.Messages++
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	if err := s.putRecord(ctx, mb, fid, rec); err != nil {
		return Record{}, err
	}
	maps := map[string][]byte{
		msgKey(rec.MessageID):         []byte(Location{FolderID: fid, UID: rec.UID}.String()),
		threadKey(tid, rec.MessageID): []byte(Location{FolderID: fid, UID: rec.UID}.String()),
	}
	if rec.MsgIDHeader != "" {
		// Do not overwrite an existing mapping (first message wins).
		m, err := s.io.OmapGet(ctx, rootOID(mb), []string{msgidKey(rec.MsgIDHeader)})
		if err != nil {
			return Record{}, mapErr(err)
		}
		if _, exists := m[msgidKey(rec.MsgIDHeader)]; !exists {
			maps[msgidKey(rec.MsgIDHeader)] = []byte(tid)
		}
	}
	if _, err := s.io.Write(ctx, rootOID(mb), rados.NewWriteTx().OmapSet(maps)); err != nil {
		return Record{}, mapErr(err)
	}
	if err := s.logChange(ctx, mb, Change{ModSeq: rec.ModSeq, Kind: KindEmail, Op: OpCreated, ID: rec.MessageID.String(), FolderID: fid, UID: rec.UID, Timestamp: now}); err != nil {
		return Record{}, err
	}
	s.maybeTrim(ctx, mb, rec.ModSeq)
	return rec, nil
}

func (s *Store) maybeTrim(ctx context.Context, mb uuid.UUID, modseq uint64) {
	if modseq%changeTrimEvery == 0 {
		_ = s.TrimChanges(ctx, mb, changeLogKeep)
	}
}

func containsFold(list []string, v string) bool {
	for _, x := range list {
		if equalFold(x, v) {
			return true
		}
	}
	return false
}

// SetFlags replaces the record's flags with the result of fn and bumps the
// modseq. Returns the updated record.
func (s *Store) SetFlags(ctx context.Context, mb uuid.UUID, fid string, uid uint32, fn func(flags []string) []string) (Record, error) {
	rec, err := s.getRecord(ctx, mb, fid, uid)
	if err != nil {
		return Record{}, err
	}
	wasSeen := rec.HasFlag(FlagSeen)
	newFlags := normalizeFlags(fn(append([]string(nil), rec.Flags...)))
	rec.Flags = newFlags
	isSeen := rec.HasFlag(FlagSeen)
	now := s.Now().UTC()
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(fid)
		if f == nil {
			return ErrNotFound
		}
		r.ModSeq++
		rec.ModSeq = r.ModSeq
		f.ModSeq = r.ModSeq
		if wasSeen && !isSeen {
			f.Unseen++
		} else if !wasSeen && isSeen {
			f.Unseen--
		}
		for _, fl := range newFlags {
			if !strings.HasPrefix(fl, `\`) && !containsFold(f.Keywords, fl) {
				f.Keywords = append(f.Keywords, fl)
			}
		}
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	if err := s.putRecord(ctx, mb, fid, rec); err != nil {
		return Record{}, err
	}
	if err := s.logChange(ctx, mb, Change{ModSeq: rec.ModSeq, Kind: KindEmail, Op: OpUpdated, ID: rec.MessageID.String(), FolderID: fid, UID: uid, Timestamp: now}); err != nil {
		return Record{}, err
	}
	s.maybeTrim(ctx, mb, rec.ModSeq)
	return rec, nil
}

// normalizeFlags dedups (case-insensitively) and canonicalises system flags.
func normalizeFlags(in []string) []string {
	var out []string
	for _, f := range in {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		for _, sys := range []string{FlagSeen, FlagAnswered, FlagFlagged, FlagDeleted, FlagDraft} {
			if equalFold(f, sys) {
				f = sys
			}
		}
		if !containsFold(out, f) {
			out = append(out, f)
		}
	}
	return out
}

// Move moves a message to another folder, assigning a new UID there. The
// message uuid (JMAP id) stays the same. Returns the new record.
func (s *Store) Move(ctx context.Context, mb uuid.UUID, from string, uid uint32, to string) (Record, error) {
	if from == to {
		return s.getRecord(ctx, mb, from, uid)
	}
	rec, err := s.getRecord(ctx, mb, from, uid)
	if err != nil {
		return Record{}, err
	}
	oldUID := rec.UID
	now := s.Now().UTC()
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		src, dst := r.FolderByID(from), r.FolderByID(to)
		if src == nil || dst == nil {
			return ErrNotFound
		}
		r.ModSeq++
		rec.UID = dst.UIDNext
		rec.ModSeq = r.ModSeq
		dst.UIDNext++
		src.ModSeq, dst.ModSeq = r.ModSeq, r.ModSeq
		src.Total--
		dst.Total++
		src.Size -= rec.Size
		dst.Size += rec.Size
		if !rec.HasFlag(FlagSeen) {
			src.Unseen--
			dst.Unseen++
		}
		for _, fl := range rec.Flags {
			if !strings.HasPrefix(fl, `\`) && !containsFold(dst.Keywords, fl) {
				dst.Keywords = append(dst.Keywords, fl)
			}
		}
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	// Write destination first, then remove source, then maps.
	if err := s.putRecord(ctx, mb, to, rec); err != nil {
		return Record{}, err
	}
	if _, err := s.io.Write(ctx, folderOID(mb, from), rados.NewWriteTx().OmapRemove(uidKey(oldUID))); err != nil {
		return Record{}, mapErr(err)
	}
	loc := []byte(Location{FolderID: to, UID: rec.UID}.String())
	if _, err := s.io.Write(ctx, rootOID(mb), rados.NewWriteTx().OmapSet(map[string][]byte{
		msgKey(rec.MessageID):                  []byte(loc),
		threadKey(rec.ThreadID, rec.MessageID): []byte(loc),
	})); err != nil {
		return Record{}, mapErr(err)
	}
	if err := s.logChange(ctx, mb, Change{ModSeq: rec.ModSeq, Kind: KindEmail, Op: OpUpdated, ID: rec.MessageID.String(), FolderID: to, UID: rec.UID, Timestamp: now}); err != nil {
		return Record{}, err
	}
	s.maybeTrim(ctx, mb, rec.ModSeq)
	return rec, nil
}

// Remove takes a message out of its folder and parks it in the retention
// set until purgeAt. The message objects are not touched.
func (s *Store) Remove(ctx context.Context, mb uuid.UUID, fid string, uid uint32, reason string, purgeAt time.Time) (RetentionEntry, error) {
	rec, err := s.getRecord(ctx, mb, fid, uid)
	if err != nil {
		return RetentionEntry{}, err
	}
	now := s.Now().UTC()
	var modseq uint64
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(fid)
		if f == nil {
			return ErrNotFound
		}
		r.ModSeq++
		modseq = r.ModSeq
		f.ModSeq = r.ModSeq
		f.Total--
		f.Size -= rec.Size
		if !rec.HasFlag(FlagSeen) {
			f.Unseen--
		}
		r.Usage.Bytes -= rec.Size
		r.Usage.Messages--
		return nil
	})
	if err != nil {
		return RetentionEntry{}, err
	}
	entry := RetentionEntry{MessageID: rec.MessageID, FolderID: fid, UID: uid, Size: rec.Size, DeletedAt: now, PurgeAt: purgeAt.UTC(), Reason: reason}
	data, err := json.Marshal(&entry)
	if err != nil {
		return RetentionEntry{}, err
	}
	// Retention entry first so a crash never loses the message's trail.
	if _, err := s.io.Write(ctx, retentionOID(mb), rados.NewWriteTx().OmapSet(map[string][]byte{retentionKey(entry.PurgeAt, rec.MessageID): data})); err != nil {
		return RetentionEntry{}, mapErr(err)
	}
	if _, err := s.io.Write(ctx, folderOID(mb, fid), rados.NewWriteTx().OmapRemove(uidKey(uid))); err != nil {
		return RetentionEntry{}, mapErr(err)
	}
	if _, err := s.io.Write(ctx, rootOID(mb), rados.NewWriteTx().OmapRemove(msgKey(rec.MessageID), threadKey(rec.ThreadID, rec.MessageID))); err != nil {
		return RetentionEntry{}, mapErr(err)
	}
	if err := s.logChange(ctx, mb, Change{ModSeq: modseq, Kind: KindEmail, Op: OpDestroyed, ID: rec.MessageID.String(), FolderID: fid, UID: uid, Timestamp: now}); err != nil {
		return RetentionEntry{}, err
	}
	s.maybeTrim(ctx, mb, modseq)
	return entry, nil
}

// ListRetention lists retention entries due before `before` (all if zero).
func (s *Store) ListRetention(ctx context.Context, mb uuid.UUID, before time.Time, max int) (entries []RetentionEntry, keys []string, more bool, err error) {
	kv, more, err := s.io.OmapList(ctx, retentionOID(mb), "", "", max)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, nil, false, nil
		}
		return nil, nil, false, mapErr(err)
	}
	for _, e := range kv {
		var re RetentionEntry
		if err := json.Unmarshal(e.Value, &re); err != nil {
			return nil, nil, false, fmt.Errorf("index: decode retention: %w", err)
		}
		if !before.IsZero() && re.PurgeAt.After(before) {
			more = false
			break
		}
		entries = append(entries, re)
		keys = append(keys, e.Key)
	}
	return entries, keys, more, nil
}

// DropRetention removes retention entries by key (after their objects are purged).
func (s *Store) DropRetention(ctx context.Context, mb uuid.UUID, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := s.io.Write(ctx, retentionOID(mb), rados.NewWriteTx().OmapRemove(keys...))
	return mapErr(err)
}

// ThreadMembers returns the locations of all messages in a thread.
func (s *Store) ThreadMembers(ctx context.Context, mb uuid.UUID, tid string) ([]Location, error) {
	kv, _, err := s.io.OmapList(ctx, rootOID(mb), "thread/"+tid+"/", "", 0)
	if err != nil {
		return nil, mapErr(err)
	}
	var out []Location
	for _, e := range kv {
		loc, err := parseLocation(string(e.Value))
		if err != nil {
			return nil, err
		}
		out = append(out, loc)
	}
	return out, nil
}

// Recalculate rebuilds folder counters and usage from the folder indexes.
func (s *Store) Recalculate(ctx context.Context, mb uuid.UUID) (Root, error) {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		return Root{}, err
	}
	type stats struct{ total, unseen, size int64 }
	per := map[string]stats{}
	for _, f := range vr.root.Folders {
		var st stats
		var after uint32
		for {
			recs, more, err := s.ListFolder(ctx, mb, f.ID, after, 1000)
			if err != nil {
				return Root{}, err
			}
			for _, r := range recs {
				st.total++
				st.size += r.Size
				if !r.HasFlag(FlagSeen) {
					st.unseen++
				}
				after = r.UID
			}
			if !more {
				break
			}
		}
		per[f.ID] = st
	}
	return s.updateRoot(ctx, mb, func(r *Root) error {
		var u Usage
		for _, f := range r.Folders {
			st := per[f.ID]
			f.Total, f.Unseen, f.Size = st.total, st.unseen, st.size
			u.Bytes += st.size
			u.Messages += st.total
		}
		r.Usage = u
		return nil
	})
}

// ---- notifications ----

// Notify tells watchers of the mailbox that something changed. Best effort.
func (s *Store) Notify(ctx context.Context, mb uuid.UUID, payload []byte) error {
	err := s.io.Notify(ctx, rootOID(mb), payload)
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return err
}

// Watch subscribes to mailbox change notifications.
func (s *Store) Watch(ctx context.Context, mb uuid.UUID) (rados.Watcher, error) {
	w, err := s.io.Watch(ctx, rootOID(mb))
	return w, mapErr(err)
}

// DeleteAll removes every index object of a mailbox (used when a mailbox is
// removed). The caller has already purged message and attachment objects.
func (s *Store) DeleteAll(ctx context.Context, mb uuid.UUID) error {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	for _, f := range vr.root.Folders {
		if err := s.io.Delete(ctx, folderOID(mb, f.ID)); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	for _, oid := range []string{changesOID(mb), retentionOID(mb), rootOID(mb)} {
		if err := s.io.Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	return nil
}
