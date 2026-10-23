package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

// A prepared journal is a commit decision: never undo message data after it
// has been published. Any reader/writer may finish it. Object version fences
// prevent a delayed helper from overwriting a subsequent mutation.
type mutation struct {
	ID      string           `json:"id"`
	Root    json.RawMessage  `json:"root"`
	Changes []Change         `json:"changes,omitempty"`
	Writes  []*mutationWrite `json:"writes"`
}
type mutationWrite struct {
	OID     string         `json:"oid"`
	Version uint64         `json:"version"`
	Exists  bool           `json:"exists"`
	Delete  bool           `json:"delete,omitempty"`
	Tx      *rados.WriteTx `json:"tx"`
}
type stageKey struct{}
type staged struct {
	base   versionedRoot
	root   Root
	mu     sync.Mutex
	writes map[string]*mutationWrite
}
type stageIO struct {
	rados.IOContext
	stage *staged
}

func (io *stageIO) Write(ctx context.Context, oid string, tx *rados.WriteTx) (uint64, error) {
	io.stage.mu.Lock()
	defer io.stage.mu.Unlock()
	w := io.stage.writes[oid]
	if w == nil {
		w = &mutationWrite{OID: oid, Tx: rados.NewWriteTx()}
		io.stage.writes[oid] = w
	}
	w.Tx.Append(tx)
	return w.Version, nil
}
func (io *stageIO) Delete(ctx context.Context, oid string) error {
	if _, err := io.Write(ctx, oid, rados.NewWriteTx().OmapClear().WriteFull(nil)); err != nil {
		return err
	}
	// Keep an empty tombstone object so a delayed CreateExclusive cannot resurrect it.
	return nil
}

func (s *Store) mutate(ctx context.Context, mb uuid.UUID, fn func(context.Context) error) error {
	if ctx.Value(stageKey{}) != nil {
		return fn(ctx)
	}
	for attempt := 0; attempt < maxRootAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var vr versionedRoot
		var err error
		cached := false
		if c, _, ok := s.cached(mb); attempt == 0 && ok {
			vr, cached = c, true
		} else {
			vr, err = s.getRoot(ctx, mb)
			if err != nil {
				return err
			}
		}
		st := &staged{base: vr, root: vr.root, writes: map[string]*mutationWrite{}}
		st.root.Folders = cloneFolders(vr.root.Folders)
		st.root.pending = nil
		if err := fn(context.WithValue(ctx, stageKey{}, st)); err != nil {
			// A stale cache must not produce a false quota/folder rejection.
			if cached {
				fresh, readErr := s.getRoot(ctx, mb)
				if readErr != nil {
					return readErr
				}
				if fresh.version != vr.version {
					continue
				}
			}
			return err
		}
		st.root.Version = rootVersion
		final, err := json.Marshal(st.root)
		if err != nil {
			return err
		}
		m := &mutation{ID: uuid.NewString(), Root: final, Changes: st.root.pending}
		for _, w := range st.writes {
			m.Writes = append(m.Writes, w)
		}
		// Target versions can be read concurrently. The root CAS rejects the
		// entire plan if another writer changed any index state meanwhile.
		var versions parallel
		for _, w := range m.Writes {
			versions.go_(func() error {
				stat, err := s.pool(ctx, mb).Stat(ctx, w.OID)
				if err != nil && !errors.Is(err, rados.ErrNotFound) {
					return err
				}
				w.Version, w.Exists = stat.Version, err == nil
				return nil
			})
		}
		if err := versions.wait(); err != nil {
			return err
		}
		sort.Slice(m.Writes, func(i, j int) bool { return m.Writes[i].OID < m.Writes[j].OID })
		// Root-only mutations need no recovery journal: data and changes fit in
		// a single atomic transaction.
		if len(m.Writes) == 0 {
			tx, err := mutationFinalTx(vr.version, st.root, m.Changes)
			if err != nil {
				return err
			}
			ver, err := s.pool(ctx, mb).Write(ctx, rootOID(mb), tx)
			if errors.Is(err, rados.ErrVersionMismatch) {
				if err := backoff(ctx, attempt+1); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			s.cacheRoot(mb, versionedRoot{root: st.root, version: ver})
			s.maybeTrim(ctx, mb, st.root.ModSeq)
			return nil
		}
		prepared := vr.root
		prepared.Version = rootVersion
		prepared.Mutation = m
		data, err := json.Marshal(prepared)
		if err != nil {
			return err
		}
		preparedVersion, err := s.pool(ctx, mb).Write(ctx, rootOID(mb), rados.NewWriteTx().AssertVersion(vr.version).WriteFull(data))
		if errors.Is(err, rados.ErrVersionMismatch) {
			if err := backoff(ctx, attempt+1); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: prepare: %v", ErrPending, err)
		}
		s.invalidateRoot(mb)
		if err := s.recoverMutation(ctx, mb, m, versionedRoot{root: prepared, version: preparedVersion}); err != nil {
			return fmt.Errorf("%w: %v", ErrPending, err)
		}
		s.maybeTrim(ctx, mb, st.root.ModSeq)
		return nil
	}
	return ErrConflict
}

func (s *Store) recoverMutation(ctx context.Context, mb uuid.UUID, m *mutation, known ...versionedRoot) error {
	io := s.pool(ctx, mb)
	var pg parallel
	for _, w := range m.Writes {
		pg.go_(func() error {
			tx := rados.NewWriteTx()
			if w.Exists {
				tx.AssertVersion(w.Version)
			} else {
				tx.CreateExclusive()
			}
			if !w.Delete {
				tx.SetXattr("mutation", []byte(m.ID))
			}
			tx.Append(w.Tx)
			_, err := io.Write(ctx, w.OID, tx)
			if err == nil {
				return nil
			}
			if errors.Is(err, rados.ErrVersionMismatch) || errors.Is(err, rados.ErrExists) || errors.Is(err, rados.ErrNotFound) {
				attrs, aerr := io.GetXattrs(ctx, w.OID)
				if aerr == nil && string(attrs["mutation"]) == m.ID {
					return nil
				}
				if w.Delete && errors.Is(aerr, rados.ErrNotFound) {
					return nil
				}
				// Another helper may already have finalized this and later operations.
				root, rerr := s.readRoot(ctx, mb)
				if rerr == nil && (root.root.Mutation == nil || root.root.Mutation.ID != m.ID) {
					return nil
				}
			}
			return err
		})
	}
	if err := pg.wait(); err != nil {
		return err
	}
	for {
		var vr versionedRoot
		var err error
		if len(known) > 0 {
			vr, known = known[0], nil
		} else {
			vr, err = s.readRoot(ctx, mb)
			if err != nil {
				return err
			}
		}
		if vr.root.Mutation == nil || vr.root.Mutation.ID != m.ID {
			return nil
		}
		var final Root
		if err := json.Unmarshal(m.Root, &final); err != nil {
			return err
		}
		// Trimming can advance the horizon while a helper is completing writes.
		if vr.root.Horizon > final.Horizon {
			final.Horizon = vr.root.Horizon
		}
		tx, err := mutationFinalTx(vr.version, final, m.Changes)
		if err != nil {
			return err
		}
		ver, err := io.Write(ctx, rootOID(mb), tx)
		if errors.Is(err, rados.ErrVersionMismatch) {
			continue
		}
		if err != nil {
			return err
		}
		s.cacheRoot(mb, versionedRoot{root: final, version: ver})
		return nil
	}
}

func mutationFinalTx(version uint64, root Root, changes []Change) (*rados.WriteTx, error) {
	data, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	tx := rados.NewWriteTx().AssertVersion(version).WriteFull(data)
	kv := map[string][]byte{}
	for _, c := range changes {
		b, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}
		kv[changeKey(c.ModSeq)] = b
	}
	if len(kv) > 0 {
		tx.OmapSet(kv)
	}
	return tx, nil
}

func (s *Store) CreateFolder(ctx context.Context, mb uuid.UUID, name, role string) (out *Folder, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error { var e error; out, e = s.createFolder(ctx, mb, name, role); return e })
	return
}
func (s *Store) RenameFolder(ctx context.Context, mb uuid.UUID, id, name string) error {
	return s.mutate(ctx, mb, func(ctx context.Context) error { return s.renameFolder(ctx, mb, id, name) })
}
func (s *Store) DeleteFolder(ctx context.Context, mb uuid.UUID, id string) error {
	return s.mutate(ctx, mb, func(ctx context.Context) error { return s.deleteFolder(ctx, mb, id) })
}
func (s *Store) UpdateFolder(ctx context.Context, mb uuid.UUID, id string, fn func(*Folder) error) (out *Folder, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error { var e error; out, e = s.updateFolder(ctx, mb, id, fn); return e })
	return
}
func (s *Store) Append(ctx context.Context, mb uuid.UUID, fid string, rec Record, store func(uint32, uint64) error) (out Record, err error) {
	prepared := false
	original := store
	if original != nil {
		store = func(uid uint32, modseq uint64) error {
			if prepared {
				return nil
			}
			err := original(uid, modseq)
			prepared = err == nil
			return err
		}
	}
	err = s.mutate(ctx, mb, func(ctx context.Context) error { var e error; out, e = s.append(ctx, mb, fid, rec, store); return e })
	return
}
func (s *Store) SetFlags(ctx context.Context, mb uuid.UUID, fid string, uid uint32, fn func([]string) []string) (out Record, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error { var e error; out, e = s.setFlags(ctx, mb, fid, uid, fn); return e })
	return
}
func (s *Store) Move(ctx context.Context, mb uuid.UUID, from string, uid uint32, to string, relocate func(Location) error) (out Record, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error { var e error; out, e = s.move(ctx, mb, from, uid, to, nil); return e })
	// The message xattr is a compatibility hint, not the authoritative location.
	if err == nil && relocate != nil {
		_ = relocate(Location{FolderID: to, UID: out.UID})
	}
	return
}
func (s *Store) Remove(ctx context.Context, mb uuid.UUID, fid string, uid uint32, reason string, purgeAt time.Time) (out RetentionEntry, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error {
		var e error
		out, e = s.remove(ctx, mb, fid, uid, reason, purgeAt)
		return e
	})
	return
}
func (s *Store) DropRetention(ctx context.Context, mb uuid.UUID, keys ...string) error {
	return s.mutate(ctx, mb, func(ctx context.Context) error { return s.dropRetention(ctx, mb, keys...) })
}
func (s *Store) Recalculate(ctx context.Context, mb uuid.UUID) (out Root, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error { var e error; out, e = s.recalculate(ctx, mb); return e })
	return
}

func (s *Store) setLocation(ctx context.Context, mb, msg uuid.UUID, loc Location) error {
	data := []byte(loc.String())
	if loc.FolderID == "" {
		data = []byte("deleted")
	}
	_, err := s.pool(ctx, mb).Write(ctx, threadOID(mb, msg.String()), rados.NewWriteTx().OmapSet(map[string][]byte{"loc/" + msg.String(): data}))
	return err
}

// Location returns the committed index location; known=true with an empty
// folder means retained/deleted, and must never fall back to an old xattr.
func (s *Store) Location(ctx context.Context, mb, msg uuid.UUID) (loc Location, known bool, err error) {
	if _, err = s.getRoot(ctx, mb); err != nil {
		return
	}
	var kv map[string][]byte
	kv, err = s.pool(ctx, mb).OmapGet(ctx, threadOID(mb, msg.String()), []string{"loc/" + msg.String()})
	if errors.Is(err, rados.ErrNotFound) {
		return Location{}, false, nil
	}
	if err != nil {
		return
	}
	data, ok := kv["loc/"+msg.String()]
	if !ok {
		return Location{}, false, nil
	}
	if string(data) == "deleted" {
		return Location{}, true, nil
	}
	loc, err = ParseLocation(string(data))
	return loc, true, err
}

// WithQuota makes the hard-limit check part of the same optimistic commit as
// allocating the message, rather than trusting a process-local usage cache.
type quotaKey struct{}
type quotaLimit struct{ Bytes, Messages int64 }

func WithQuota(ctx context.Context, bytes, messages int64) context.Context {
	return context.WithValue(ctx, quotaKey{}, quotaLimit{bytes, messages})
}

// IsLive is conservative for legacy messages with no authoritative location.
// A message is only disposable once no committed folder references it.
func (s *Store) IsLive(ctx context.Context, mb, msg uuid.UUID, hint string) (bool, error) {
	loc, known, err := s.Location(ctx, mb, msg)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if known {
		return loc.FolderID != "", nil
	}
	if l, e := ParseLocation(hint); e == nil {
		r, e := s.getRecord(ctx, mb, l.FolderID, l.UID)
		if e == nil && r.MessageID == msg {
			return true, nil
		}
		if e != nil && !errors.Is(e, ErrNotFound) {
			return false, e
		}
	}
	root, err := s.GetRoot(ctx, mb)
	if err != nil {
		return false, err
	}
	for _, f := range root.Folders {
		var before uint32
		for {
			records, more, err := s.ListFolderDesc(ctx, mb, f.ID, before, 1000)
			if err != nil {
				return false, err
			}
			for _, r := range records {
				if r.MessageID == msg {
					return true, nil
				}
				before = r.UID
			}
			if !more || len(records) == 0 {
				break
			}
		}
	}
	return false, nil
}
