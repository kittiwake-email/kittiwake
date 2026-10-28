package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/store/storage"
)

const (
	maxRootAttempts  = 64
	rootVersion      = 5
	changeLogKeep    = 10000 // entries kept per mailbox
	changeTrimEvery  = 256   // trim when modseq crosses a multiple of this
	maxFolderDepth   = 16
	maxFolderNameLen = 255
	modseqKeyWidth   = 20
	// shardBits: UIDs per folder index shard object (2^16 = 65536 keys,
	// well under Ceph's large-omap warning threshold of 200k).
	shardBits = 16
	// threadShards: number of per-mailbox thread/message-id map objects.
	threadShards = 256
)

// Store is the mailbox index store on the index pool.
type Store struct {
	r   storage.Resolver
	Now func() time.Time

	// roots caches the last root seen per mailbox so an optimistic update
	// can skip its initial read (the version assertion keeps it correct).
	roots sync.Map // uuid.UUID -> cachedRoot
	nroot atomic.Int64
}

type cachedRoot struct {
	vr versionedRoot
	at time.Time
}

const maxCachedRoots = 10000

// New returns a store on the index pool context.
func New(indexPool rados.IOContext) *Store {
	return NewMulti(storage.FixedPools(indexPool, indexPool, indexPool))
}

// NewMulti returns a store that resolves each mailbox's index pool through r.
func NewMulti(r storage.Resolver) *Store { return &Store{r: r, Now: time.Now} }

// pool returns the mailbox's index pool, or a context that fails every
// operation with the resolution error (storage unavailable).
func (s *Store) pool(ctx context.Context, mb uuid.UUID) rados.IOContext {
	_, io, err := s.r.Pool(ctx, mb, storage.RoleIndex)
	if err != nil {
		return rados.ErrorContext(err)
	}
	if st, ok := ctx.Value(stageKey{}).(*staged); ok {
		return &stageIO{IOContext: io, stage: st}
	}
	return io
}

func (s *Store) cacheRoot(mb uuid.UUID, vr versionedRoot) {
	next := &cachedRoot{vr: vr, at: s.Now()}
	for {
		old, ok := s.roots.Load(mb)
		if ok {
			if old.(*cachedRoot).vr.version > vr.version {
				return
			}
			if s.roots.CompareAndSwap(mb, old, next) {
				return
			}
		} else if _, loaded := s.roots.LoadOrStore(mb, next); !loaded {
			if s.nroot.Add(1) > maxCachedRoots {
				s.roots.Range(func(k, _ any) bool {
					if _, ok := s.roots.LoadAndDelete(k); ok {
						s.nroot.Add(-1)
					}
					return false
				})
			}
			return
		}
	}
}
func (s *Store) invalidateRoot(mb uuid.UUID) {
	if _, ok := s.roots.LoadAndDelete(mb); ok {
		s.nroot.Add(-1)
	}
}

func (s *Store) cached(mb uuid.UUID) (versionedRoot, time.Time, bool) {
	v, ok := s.roots.Load(mb)
	if !ok {
		return versionedRoot{}, time.Time{}, false
	}
	c := v.(*cachedRoot)
	return c.vr, c.at, true
}

// ---- object names and keys ----

func rootOID(mb uuid.UUID) string      { return mb.String() }
func retentionOID(mb uuid.UUID) string { return mb.String() + "/retention" }

// changeKey is the root omap key of a change-log entry.
func changeKey(m uint64) string { return "c/" + modseqKey(m) }

// folderOID returns the shard object holding uid for the folder.
func folderOID(mb uuid.UUID, fid string, uid uint32) string {
	k := uid >> shardBits
	if k == 0 {
		return mb.String() + "/" + fid
	}
	return fmt.Sprintf("%s/%s.%d", mb, fid, k)
}

// uidKey is the omap key of a UID: inverted so that ascending omap iteration
// yields the newest message first (the common listing).
func uidKey(uid uint32) string { return fmt.Sprintf("%010d", math.MaxUint32-uint64(uid)) }

func uidFromKey(k string) (uint32, bool) {
	var inv uint64
	if _, err := fmt.Sscanf(k, "%d", &inv); err != nil || inv > math.MaxUint32 {
		return 0, false
	}
	return uint32(math.MaxUint32 - inv), true
}

func modseqKey(m uint64) string { return fmt.Sprintf("%0*d", modseqKeyWidth, m) }

// threadOID returns the thread-map shard for a key (message-id or thread id).
func threadOID(mb uuid.UUID, key string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(key)))
	return fmt.Sprintf("%s/t.%s", mb, hex.EncodeToString(sum[:1]))
}

func msgidKey(h string) string { return "m/" + strings.ToLower(h) }
func threadKey(tid string, id uuid.UUID) string {
	return "t/" + tid + "/" + id.String()
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
	if st, ok := ctx.Value(stageKey{}).(*staged); ok {
		return versionedRoot{root: st.root, version: st.base.version}, nil
	}
	for {
		vr, err := s.readRoot(ctx, mb)
		if err != nil {
			return vr, err
		}
		if vr.root.Mutation == nil {
			s.cacheRoot(mb, vr)
			return vr, nil
		}
		if err := s.recoverMutation(ctx, mb, vr.root.Mutation, vr); err != nil {
			return versionedRoot{}, err
		}
		// Recovery can be completed by a reader after the original writer died.
		// Wake protocol watchers even when that writer cannot send its event.
		go func() {
			nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = s.Notify(nctx, mb, []byte(`{"type":"recovery"}`))
		}()
	}
}

func (s *Store) readRoot(ctx context.Context, mb uuid.UUID) (versionedRoot, error) {
	data, ver, err := s.pool(ctx, mb).Get(ctx, rootOID(mb))
	if err != nil {
		return versionedRoot{}, mapErr(err)
	}
	var r Root
	if err := json.Unmarshal(data, &r); err != nil {
		return versionedRoot{}, fmt.Errorf("index: decode root %s: %w", mb, err)
	}
	if r.Version != rootVersion {
		return versionedRoot{}, fmt.Errorf("index: root version %d requires a lab reset before format 5", r.Version)
	}
	vr := versionedRoot{root: r, version: ver}
	return vr, nil
}

// GetRoot returns the mailbox root. ErrNotFound if the mailbox index does
// not exist yet (see EnsureRoot).
func (s *Store) GetRoot(ctx context.Context, mb uuid.UUID) (Root, error) {
	vr, err := s.getRoot(ctx, mb)
	return vr.root, err
}

// GetRootCached returns the root as last seen by this process when that is
// younger than maxAge, else reads it. For approximate uses (quota checks).
func (s *Store) GetRootCached(ctx context.Context, mb uuid.UUID, maxAge time.Duration) (Root, error) {
	if vr, at, ok := s.cached(mb); ok && s.Now().Sub(at) < maxAge {
		return vr.root, nil
	}
	return s.GetRoot(ctx, mb)
}

// updateRoot applies fn under optimistic concurrency with retries.
// updateRoot applies fn under optimistic concurrency. The first attempt
// uses the cached root (no read); a version mismatch re-reads and retries.
// Change-log entries queued with Root.Log are written in the same
// transaction as the root data.
func (s *Store) updateRoot(ctx context.Context, mb uuid.UUID, fn func(r *Root) error) (Root, error) {
	if st, ok := ctx.Value(stageKey{}).(*staged); ok {
		r := st.root
		r.Folders = cloneFolders(r.Folders)
		if err := fn(&r); err != nil {
			return Root{}, err
		}
		st.root = r
		return r, nil
	}

	for attempt := 0; attempt < maxRootAttempts; attempt++ {
		var vr versionedRoot
		var err error
		if c, _, ok := s.cached(mb); ok && attempt == 0 {
			vr = c
		} else {
			if attempt > 0 {
				if err := backoff(ctx, attempt); err != nil {
					return Root{}, err
				}
			}
			if vr, err = s.getRoot(ctx, mb); err != nil {
				return Root{}, err
			}
		}
		r := vr.root
		r.Folders = cloneFolders(vr.root.Folders)
		r.pending = nil
		if err := fn(&r); err != nil {
			return Root{}, err
		}
		data, err := json.Marshal(&r)
		if err != nil {
			return Root{}, err
		}
		tx := rados.NewWriteTx().AssertVersion(vr.version).WriteFull(data)
		if len(r.pending) > 0 {
			kv := make(map[string][]byte, len(r.pending))
			for _, c := range r.pending {
				b, _ := json.Marshal(&c)
				kv[changeKey(c.ModSeq)] = b
			}
			tx.OmapSet(kv)
		}
		ver, err := s.pool(ctx, mb).Write(ctx, rootOID(mb), tx)
		if err == nil {
			r.pending = nil
			s.cacheRoot(mb, versionedRoot{root: r, version: ver})
			return r, nil
		}
		if !errors.Is(err, rados.ErrVersionMismatch) {
			return Root{}, mapErr(err)
		}
		s.invalidateRoot(mb)
	}
	return Root{}, ErrConflict
}

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
// not exist, and returns it. Folder shard objects are created on first use.
func (s *Store) EnsureRoot(ctx context.Context, mb uuid.UUID) (Root, error) {
	if vr, err := s.getRoot(ctx, mb); err == nil {
		return vr.root, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Root{}, err
	}
	now := s.Now().UTC()
	r := Root{AttachmentEpoch: uuid.NewString(), Version: rootVersion, MailboxID: mb, NextFolderID: 1, ModSeq: 0, Horizon: 1, Created: now}
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
	kv := map[string][]byte{}
	for _, f := range r.Folders {
		c := Change{ModSeq: f.ModSeq, Kind: KindFolder, Op: OpCreated, ID: f.ID, Timestamp: now}
		b, _ := json.Marshal(&c)
		kv[changeKey(c.ModSeq)] = b
	}
	ver, err := s.pool(ctx, mb).Write(ctx, rootOID(mb), rados.NewWriteTx().CreateExclusive().WriteFull(data).OmapSet(kv))
	if errors.Is(err, rados.ErrExists) {
		vr, err := s.getRoot(ctx, mb)
		return vr.root, err
	}
	if err != nil {
		return Root{}, mapErr(err)
	}
	s.cacheRoot(mb, versionedRoot{root: r, version: ver})
	return r, nil
}

// EnsureExists is EnsureRoot without the read when this process has seen
// the root before (delivery only needs existence; Append re-validates).
func (s *Store) EnsureExists(ctx context.Context, mb uuid.UUID) error {
	if _, _, ok := s.cached(mb); ok {
		return nil
	}
	_, err := s.EnsureRoot(ctx, mb)
	return err
}

// Exists reports whether the mailbox index exists.
func (s *Store) Exists(ctx context.Context, mb uuid.UUID) (bool, error) {
	_, err := s.pool(ctx, mb).Stat(ctx, rootOID(mb))
	if errors.Is(err, rados.ErrNotFound) {
		return false, nil
	}
	return err == nil, mapErr(err)
}

// ---- change log ----

// Changes returns changes with modseq > since, in order, up to max.
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
	kv, more, err := s.pool(ctx, mb).OmapList(ctx, rootOID(mb), "c/", changeKey(since), max)
	if err != nil {
		return nil, false, 0, mapErr(err)
	}
	for _, e := range kv {
		var c Change
		if err := json.Unmarshal(e.Value, &c); err != nil {
			return nil, false, 0, fmt.Errorf("index: decode change: %w", err)
		}
		if c.ModSeq > vr.root.ModSeq {
			more = false
			break
		}
		changes = append(changes, c)
	}
	if len(changes) == 0 || changes[0].ModSeq != since+1 {
		return nil, false, vr.root.ModSeq, ErrNoHorizon
	}
	return changes, more, vr.root.ModSeq, nil
}

// TrimChanges drops change log entries older than the newest keep entries.
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
	var rm []string
	for {
		kv, more, err := s.pool(ctx, mb).OmapList(ctx, rootOID(mb), "c/", "", 512)
		if err != nil {
			return mapErr(err)
		}
		n := len(rm)
		for _, e := range kv {
			if e.Key < changeKey(newHorizon) {
				rm = append(rm, e.Key)
			}
		}
		if len(rm) == n || !more || len(rm)-n < len(kv) {
			break
		}
		if _, err := s.pool(ctx, mb).Write(ctx, rootOID(mb), rados.NewWriteTx().OmapRemove(rm[n:]...)); err != nil {
			return mapErr(err)
		}
		rm = rm[:n]
	}
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		if newHorizon > r.Horizon {
			r.Horizon = newHorizon
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(rm) > 0 {
		if _, err := s.pool(ctx, mb).Write(ctx, rootOID(mb), rados.NewWriteTx().OmapRemove(rm...)); err != nil {
			return mapErr(err)
		}
	}
	return nil
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

// CreateFolder creates a folder with the full name (and missing parents).
func (s *Store) createFolder(ctx context.Context, mb uuid.UUID, name, role string) (*Folder, error) {
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
			r.Log(Change{ModSeq: f.ModSeq, Kind: KindFolder, Op: OpCreated, ID: f.ID, Timestamp: now})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created[len(created)-1], nil
}

// RenameFolder renames a folder (and its children) to newName.
func (s *Store) renameFolder(ctx context.Context, mb uuid.UUID, id, newName string) error {
	if err := ValidateFolderName(newName); err != nil {
		return err
	}
	now := s.Now().UTC()
	_, err := s.updateRoot(ctx, mb, func(r *Root) error {
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
		if i := strings.LastIndex(newName, Delimiter); i > 0 {
			if r.FolderByName(newName[:i]) == nil {
				return fmt.Errorf("%w: parent folder %q", ErrNotFound, newName[:i])
			}
		}
		old := f.Name
		for _, g := range r.Folders {
			if g.ID == f.ID || strings.HasPrefix(g.Name, old+Delimiter) {
				g.Name = newName + strings.TrimPrefix(g.Name, old)
				r.ModSeq++
				g.ModSeq = r.ModSeq
				r.Log(Change{ModSeq: r.ModSeq, Kind: KindFolder, Op: OpUpdated, ID: g.ID, Timestamp: now})
			}
		}
		return nil
	})
	return err
}

// DeleteFolder removes an empty folder without children.
func (s *Store) deleteFolder(ctx context.Context, mb uuid.UUID, id string) error {
	var shards []string
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
		shards = folderShards(mb, f)
		out := r.Folders[:0]
		for _, g := range r.Folders {
			if g.ID != id {
				out = append(out, g)
			}
		}
		r.Folders = out
		r.ModSeq++
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindFolder, Op: OpDestroyed, ID: id, Timestamp: now})
		return nil
	})
	if err != nil {
		return err
	}
	for _, oid := range shards {
		if err := s.pool(ctx, mb).Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	return nil
}

// folderShards lists the shard objects a folder may have used.
func folderShards(mb uuid.UUID, f *Folder) []string {
	var out []string
	last := uint32(0)
	if f.UIDNext > 1 {
		last = (f.UIDNext - 1) >> shardBits
	}
	for k := uint32(0); k <= last; k++ {
		out = append(out, folderOID(mb, f.ID, k<<shardBits))
	}
	return out
}

// UpdateFolder applies fn to folder metadata (subscription, keywords).
func (s *Store) updateFolder(ctx context.Context, mb uuid.UUID, id string, fn func(f *Folder) error) (*Folder, error) {
	var out *Folder
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
		f.Name, f.Role = name, role
		r.ModSeq++
		f.ModSeq = r.ModSeq
		c := *f
		out = &c
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindFolder, Op: OpUpdated, ID: id, Timestamp: now})
		return nil
	})
	return out, err
}

// ---- messages ----

func (s *Store) getRecord(ctx context.Context, mb uuid.UUID, fid string, uid uint32) (Record, error) {
	m, err := s.pool(ctx, mb).OmapGet(ctx, folderOID(mb, fid, uid), []string{uidKey(uid)})
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
	_, err = s.pool(ctx, mb).Write(ctx, folderOID(mb, fid, rec.UID), rados.NewWriteTx().OmapSet(map[string][]byte{uidKey(rec.UID): data}))
	return mapErr(err)
}

// GetRecord returns one record.
func (s *Store) GetRecord(ctx context.Context, mb uuid.UUID, fid string, uid uint32) (Record, error) {
	return s.getRecord(ctx, mb, fid, uid)
}

// listShard returns the records of one shard object, newest first.
func (s *Store) listShard(ctx context.Context, mb uuid.UUID, oid, after string, max int) ([]Record, bool, error) {
	kv, more, err := s.pool(ctx, mb).OmapList(ctx, oid, "", after, max)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, mapErr(err)
	}
	out := make([]Record, 0, len(kv))
	for _, e := range kv {
		var rec Record
		if err := json.Unmarshal(e.Value, &rec); err != nil {
			return nil, false, fmt.Errorf("index: decode record: %w", err)
		}
		out = append(out, rec)
	}
	return out, more, nil
}

// ListFolderDesc lists records newest first (UID descending), starting
// below beforeUID (0 = from the newest). This is the cheap listing: the
// first page costs one omap read.
func (s *Store) ListFolderDesc(ctx context.Context, mb uuid.UUID, fid string, beforeUID uint32, max int) (records []Record, more bool, err error) {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		return nil, false, err
	}
	f := vr.root.FolderByID(fid)
	if f == nil {
		return nil, false, ErrNotFound
	}
	if f.UIDNext <= 1 {
		return nil, false, nil
	}
	top := f.UIDNext - 1
	if beforeUID != 0 && beforeUID-1 < top {
		top = beforeUID - 1
	}
	if beforeUID == 1 {
		return nil, false, nil
	}
	after := ""
	if beforeUID != 0 {
		after = uidKey(beforeUID)
	}
	for k := int64(top >> shardBits); k >= 0; k-- {
		oid := folderOID(mb, fid, uint32(k)<<shardBits)
		want := 0
		if max > 0 {
			want = max - len(records)
		}
		recs, shardMore, err := s.listShard(ctx, mb, oid, after, want)
		if err != nil {
			return nil, false, err
		}
		records = append(records, recs...)
		if max > 0 && len(records) >= max {
			return records, shardMore || k > 0, nil
		}
		after = "" // next (older) shard from its newest key
	}
	return records, false, nil
}

// ListFolder lists records with UID > afterUID in UID order. The omap is
// newest-first, so a shard is read whole and reversed. This compatibility
// API is expensive for pagination; use ListFolderDesc for full scans and pages.
func (s *Store) ListFolder(ctx context.Context, mb uuid.UUID, fid string, afterUID uint32, max int) (records []Record, more bool, err error) {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		return nil, false, err
	}
	f := vr.root.FolderByID(fid)
	if f == nil {
		return nil, false, ErrNotFound
	}
	if f.UIDNext <= 1 {
		return nil, false, nil
	}
	top := (f.UIDNext - 1) >> shardBits
	for k := afterUID >> shardBits; k <= top; k++ {
		oid := folderOID(mb, fid, k<<shardBits)
		recs, _, err := s.listShard(ctx, mb, oid, "", 0)
		if err != nil {
			return nil, false, err
		}
		// recs are newest first; walk backwards for ascending UID.
		for i := len(recs) - 1; i >= 0; i-- {
			if recs[i].UID <= afterUID {
				continue
			}
			if max > 0 && len(records) >= max {
				return records, true, nil
			}
			records = append(records, recs[i])
		}
	}
	return records, false, nil
}

// threadFor determines the thread id for a new record from its own
// Message-ID and the references it carries, consulting the thread shards.
func (s *Store) threadFor(ctx context.Context, mb uuid.UUID, rec *Record) (string, error) {
	// Preserve duplicate Message-ID identity, then prefer the direct parent.
	var candidates []string
	if rec.MsgIDHeader != "" {
		candidates = append(candidates, rec.MsgIDHeader)
	}
	candidates = append(candidates, rec.InReplyTo...)
	for i := len(rec.References) - 1; i >= 0; i-- {
		candidates = append(candidates, rec.References[i])
	}
	byShard := map[string][]string{}
	for _, id := range candidates {
		oid := threadOID(mb, id)
		byShard[oid] = append(byShard[oid], msgidKey(id))
	}
	found := map[string]string{}
	for oid, keys := range byShard {
		m, err := s.mapGet(ctx, mb, oid, keys)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				continue
			}
			return "", mapErr(err)
		}
		for k, v := range m {
			if len(v) > 0 {
				var mapping messageIDMapping
				if err := json.Unmarshal(v, &mapping); err != nil {
					return "", err
				}
				found[k] = mapping.ThreadID
			}
		}
	}
	for _, id := range candidates {
		if tid, ok := found[msgidKey(id)]; ok {
			return tid, nil
		}
	}
	return rec.MessageID.String(), nil
}

// Append adds a record to a folder. It allocates the UID and modseq and
// updates counters, calls store (where the caller persists the message
// object at its final location), then writes the index record (the commit
// point), the thread maps and the change log. If store fails nothing is
// indexed; the consumed UID is a harmless gap and counters are repaired by
// Recalculate.
func (s *Store) append(ctx context.Context, mb uuid.UUID, fid string, rec Record, store func(uid uint32, modseq uint64) error) (Record, error) {
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
	rec.Flags = normalizeFlags(rec.Flags)
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(fid)
		if f == nil {
			return ErrNotFound
		}
		if lim, ok := ctx.Value(quotaKey{}).(quotaLimit); ok {
			if (lim.Bytes > 0 && (rec.Size > lim.Bytes || r.Usage.Bytes > lim.Bytes-rec.Size)) || (lim.Messages > 0 && r.Usage.Messages >= lim.Messages) {
				return ErrOverQuota
			}
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
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindEmail, Op: OpCreated, ID: rec.MessageID.String(), FolderID: fid, UID: rec.UID, Timestamp: now})
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	// The message object, the thread membership and the message-id map are
	// independent: write them concurrently. The folder record (commit
	// point) follows once all succeeded.
	loc := []byte(Location{FolderID: fid, UID: rec.UID}.String())
	var pg parallel
	if store != nil {
		pg.go_(func() error { return store(rec.UID, rec.ModSeq) })
	}
	pg.go_(func() error {
		return s.mapSet(ctx, mb, threadOID(mb, tid), threadKey(tid, rec.MessageID), loc)
	})
	if rec.MsgIDHeader != "" {
		// threadFor already resolved an existing mapping into tid, so an
		// unconditional write is idempotent (no read needed).
		pg.go_(func() error {
			return s.changeMessageID(ctx, mb, rec.MsgIDHeader, tid, 1)
		})
	}
	if err := pg.wait(); err != nil {
		s.unappend(ctx, mb, fid, rec, tid)
		return Record{}, err
	}
	if err := s.changeAttachments(ctx, mb, rec.Attachments, 1); err != nil {
		return Record{}, err
	}
	if err := s.setLocation(ctx, mb, rec.MessageID, Location{FolderID: fid, UID: rec.UID}); err != nil {
		return Record{}, err
	}
	if err := s.putRecord(ctx, mb, fid, rec); err != nil {
		s.unappend(ctx, mb, fid, rec, tid)
		return Record{}, err
	}
	s.maybeTrim(ctx, mb, rec.ModSeq)
	return rec, nil
}

// unappend compensates an Append that failed after the root transaction:
// the counters and quota usage go back, the change log records the
// destroy, and the thread membership is dropped, so a failed delivery
// leaves no trace in the index (the caller removes the message object).
// Best effort: a crash in the same window is caught by recalc-quota and
// the orphan sweep instead.
func (s *Store) unappend(ctx context.Context, mb uuid.UUID, fid string, rec Record, tid string) {
	now := s.Now().UTC()
	_, _ = s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(fid)
		if f == nil {
			return ErrNotFound
		}
		r.ModSeq++
		f.ModSeq = r.ModSeq
		f.Total--
		f.Size -= rec.Size
		if !rec.HasFlag(FlagSeen) {
			f.Unseen--
		}
		r.Usage.Bytes -= rec.Size
		r.Usage.Messages--
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindEmail, Op: OpDestroyed, ID: rec.MessageID.String(), FolderID: fid, UID: rec.UID, Timestamp: now})
		return nil
	})
	_ = s.mapRemove(ctx, mb, threadOID(mb, tid), threadKey(tid, rec.MessageID))
}

// parallel runs functions concurrently and returns the first error.
type parallel struct {
	wg   sync.WaitGroup
	mu   sync.Mutex
	errs error
}

func (p *parallel) go_(f func() error) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := f(); err != nil {
			p.mu.Lock()
			if p.errs == nil {
				p.errs = err
			}
			p.mu.Unlock()
		}
	}()
}

func (p *parallel) wait() error {
	p.wg.Wait()
	return p.errs
}

func (s *Store) maybeTrim(ctx context.Context, mb uuid.UUID, modseq uint64) {
	if ctx.Value(stageKey{}) != nil {
		return
	}
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
func (s *Store) setFlags(ctx context.Context, mb uuid.UUID, fid string, uid uint32, fn func(flags []string) []string) (Record, error) {
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
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindEmail, Op: OpUpdated, ID: rec.MessageID.String(), FolderID: fid, UID: uid, Timestamp: now})
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	if err := s.putRecord(ctx, mb, fid, rec); err != nil {
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
// message uuid (JMAP id) stays the same. relocate is called with the new
// location before the index is switched (the caller updates the message
// object's location). Returns the new record.
func (s *Store) move(ctx context.Context, mb uuid.UUID, from string, uid uint32, to string, relocate func(loc Location) error) (Record, error) {
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
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindEmail, Op: OpUpdated, ID: rec.MessageID.String(), FolderID: to, UID: rec.UID, Timestamp: now})
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	loc := Location{FolderID: to, UID: rec.UID}
	if err := s.setLocation(ctx, mb, rec.MessageID, loc); err != nil {
		return Record{}, err
	}
	if err := s.putRecord(ctx, mb, to, rec); err != nil {
		return Record{}, err
	}
	var pg parallel
	pg.go_(func() error {
		_, err := s.pool(ctx, mb).Write(ctx, folderOID(mb, from, oldUID), rados.NewWriteTx().OmapRemove(uidKey(oldUID)))
		return mapErr(err)
	})
	pg.go_(func() error {
		return s.mapSet(ctx, mb, threadOID(mb, rec.ThreadID), threadKey(rec.ThreadID, rec.MessageID), []byte(loc.String()))
	})
	if err := pg.wait(); err != nil {
		return Record{}, err
	}
	s.maybeTrim(ctx, mb, rec.ModSeq)
	return rec, nil
}

// Remove takes a message out of its folder and parks it in the retention
// set until purgeAt. The message objects are not touched.
func (s *Store) remove(ctx context.Context, mb uuid.UUID, fid string, uid uint32, reason string, purgeAt time.Time) (RetentionEntry, error) {
	rec, err := s.getRecord(ctx, mb, fid, uid)
	if err != nil {
		return RetentionEntry{}, err
	}
	now := s.Now().UTC()
	var modseq, retentionSeq uint64
	_, err = s.updateRoot(ctx, mb, func(r *Root) error {
		f := r.FolderByID(fid)
		if f == nil {
			return ErrNotFound
		}
		r.ModSeq++
		modseq = r.ModSeq
		retentionSeq = r.RetentionNext
		r.RetentionNext++
		f.ModSeq = r.ModSeq
		f.Total--
		f.Size -= rec.Size
		if !rec.HasFlag(FlagSeen) {
			f.Unseen--
		}
		r.Usage.Bytes -= rec.Size
		r.Usage.Messages--
		r.Log(Change{ModSeq: r.ModSeq, Kind: KindEmail, Op: OpDestroyed, ID: rec.MessageID.String(), FolderID: fid, UID: uid, Timestamp: now})
		return nil
	})
	if err != nil {
		return RetentionEntry{}, err
	}
	if err := s.setRetainedLocation(ctx, mb, rec.MessageID, retentionSeq, retentionKey(purgeAt.UTC(), rec.MessageID)); err != nil {
		return RetentionEntry{}, err
	}
	entry := RetentionEntry{Attachments: rec.Attachments, MessageID: rec.MessageID, FolderID: fid, UID: uid, Size: rec.Size, DeletedAt: now, PurgeAt: purgeAt.UTC(), Reason: reason}
	data, err := json.Marshal(&entry)
	if err != nil {
		return RetentionEntry{}, err
	}
	// Retention entry first so a crash never loses the message's trail,
	// then the index and thread removals concurrently.
	if _, err := s.pool(ctx, mb).Write(ctx, retentionShardOID(mb, retentionSeq>>shardBits), rados.NewWriteTx().OmapSet(map[string][]byte{retentionKey(entry.PurgeAt, rec.MessageID): data})); err != nil {
		return RetentionEntry{}, mapErr(err)
	}
	var pg parallel
	pg.go_(func() error {
		_, err := s.pool(ctx, mb).Write(ctx, folderOID(mb, fid, uid), rados.NewWriteTx().OmapRemove(uidKey(uid)))
		return mapErr(err)
	})
	pg.go_(func() error {
		return s.mapRemove(ctx, mb, threadOID(mb, rec.ThreadID), threadKey(rec.ThreadID, rec.MessageID))
	})
	if rec.MsgIDHeader != "" {
		pg.go_(func() error { return s.changeMessageID(ctx, mb, rec.MsgIDHeader, rec.ThreadID, -1) })
	}
	if err := pg.wait(); err != nil {
		return RetentionEntry{}, err
	}
	s.maybeTrim(ctx, mb, modseq)
	return entry, nil
}

// ThreadMembers returns the locations of all messages in a thread.
func (s *Store) ThreadMembers(ctx context.Context, mb uuid.UUID, tid string) ([]Location, error) {
	if _, err := s.getRoot(ctx, mb); err != nil {
		return nil, err
	}
	kv, err := s.mapList(ctx, mb, threadOID(mb, tid), "t/"+tid+"/")
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, nil
		}
		return nil, mapErr(err)
	}
	var out []Location
	for _, e := range kv {
		loc, err := ParseLocation(string(e.Value))
		if err != nil {
			return nil, err
		}
		out = append(out, loc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FolderID != out[j].FolderID {
			return out[i].FolderID < out[j].FolderID
		}
		return out[i].UID < out[j].UID
	})
	return out, nil
}

// ParseLocation parses "<folder>/<uid>".
func ParseLocation(s string) (Location, error) {
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

// Recalculate rebuilds folder counters and usage from the folder indexes.
func (s *Store) recalculate(ctx context.Context, mb uuid.UUID) (Root, error) {
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
			recs, more, err := s.ListFolderDesc(ctx, mb, f.ID, after, 1000)
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
	err := s.pool(ctx, mb).Notify(ctx, rootOID(mb), payload)
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return err
}

// Watch subscribes to mailbox change notifications.
func (s *Store) Watch(ctx context.Context, mb uuid.UUID) (rados.Watcher, error) {
	w, err := s.pool(ctx, mb).Watch(ctx, rootOID(mb))
	return w, mapErr(err)
}

// DeleteAll removes every index object of a mailbox.
func (s *Store) DeleteAll(ctx context.Context, mb uuid.UUID) error {
	vr, err := s.getRoot(ctx, mb)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	var oids []string
	for _, f := range vr.root.Folders {
		oids = append(oids, folderShards(mb, f)...)
	}
	for i := 0; i < threadShards; i++ {
		base := fmt.Sprintf("%s/t.%02x", mb, i)
		if err := s.walkMap(ctx, mb, base, func(oid string) error { oids = append(oids, oid); return nil }); err != nil {
			return err
		}
	}
	if vr.root.RetentionNext > 0 {
		for shard := uint64(0); shard <= (vr.root.RetentionNext-1)>>shardBits; shard++ {
			oids = append(oids, retentionShardOID(mb, shard))
		}
	}
	oids = append(oids, rootOID(mb))
	for _, oid := range oids {
		if err := s.pool(ctx, mb).Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	s.invalidateRoot(mb)
	return nil
}
