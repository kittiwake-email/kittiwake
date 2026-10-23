// Package attach persists deduplicated attachments in the RADOS attachments
// pool. The object name is the hex SHA-256 of the content; the omap holds one
// key per referencing message (`ref/<mailbox>/<message>`), so references are
// idempotent and never race on a counter. See README.md "Attachments".
package attach

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/store/storage"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("attach: not found")
	ErrHash     = errors.New("attach: content does not match hash")
)

// Store is the attachment store.
type Store struct {
	r   storage.Resolver
	Now func() time.Time
}

// New returns a store on the attachments pool context (one storage).
func New(attachmentsPool rados.IOContext) *Store {
	return NewMulti(storage.FixedPools(attachmentsPool, attachmentsPool, attachmentsPool))
}

// NewMulti returns a store that resolves each mailbox's attachments pool
// through r. Deduplication is per storage: the same content stored for
// mailboxes on two storage entries is two objects.
func NewMulti(r storage.Resolver) *Store { return &Store{r: r, Now: time.Now} }

func (s *Store) pool(ctx context.Context, mb uuid.UUID) rados.IOContext {
	_, io, err := s.r.Pool(ctx, mb, storage.RoleAttachments)
	if err != nil {
		return rados.ErrorContext(err)
	}
	return io
}

func (s *Store) poolOf(ctx context.Context, cid int64) rados.IOContext {
	io, err := s.r.PoolOf(ctx, cid, storage.RoleAttachments)
	if err != nil {
		return rados.ErrorContext(err)
	}
	return io
}

// Hash returns the hex SHA-256 of data.
func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func refKey(mb, msg uuid.UUID) string { return fmt.Sprintf("ref/%s/%s", mb, msg) }

func mapErr(err error) error {
	if errors.Is(err, rados.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

// Put stores the content under its hash if not present and adds a
// reference from the message. It is safe to call concurrently for the same
// content: exclusive create resolves the race, and the reference is an
// idempotent omap set. contentType is recorded as an xattr on first store.
func (s *Store) Put(ctx context.Context, hash string, data []byte, contentType string, mb, msg uuid.UUID) error {
	if Hash(data) != hash {
		return ErrHash
	}
	now := []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))
	// Content, xattrs and the first reference in one transaction; only when
	// the object already exists does a second (reference-only) write follow.
	tx := rados.NewWriteTx().CreateExclusive().WriteFull(data).
		SetXattr("content_type", []byte(contentType)).SetXattr("created", now).SetXattr("touched", now).
		SetXattr("size", []byte(strconv.Itoa(len(data)))).OmapSet(map[string][]byte{refKey(mb, msg): nil})
	for attempt := 0; attempt < 3; attempt++ {
		_, err := s.pool(ctx, mb).Write(ctx, hash, tx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, rados.ErrExists) {
			return err
		}
		err = s.AddRef(ctx, hash, mb, msg)
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		// The garbage collector removed the object between our exclusive
		// create and the reference: create it again.
	}
	return fmt.Errorf("attach: %s: lost a race with garbage collection three times", hash)
}

// AddRef records that message references the attachment. The object must exist.
func (s *Store) AddRef(ctx context.Context, hash string, mb, msg uuid.UUID) error {
	_, err := s.pool(ctx, mb).Write(ctx, hash, rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{refKey(mb, msg): nil}).SetXattr("touched", []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))))
	return mapErr(err)
}

// RemoveRef drops a message's reference. Missing object or key is not an error.
func (s *Store) RemoveRef(ctx context.Context, hash string, mb, msg uuid.UUID) error {
	if len(hash) != 64 {
		return ErrHash
	}
	// Queue first: a crash may leave an extra candidate, never an untracked removal.
	if _, err := s.pool(ctx, mb).Write(ctx, candidateOID(hash), rados.NewWriteTx().OmapSet(map[string][]byte{hash: nil})); err != nil {
		return err
	}

	_, err := s.pool(ctx, mb).Write(ctx, hash, rados.NewWriteTx().AssertExists().OmapRemove(refKey(mb, msg)).SetXattr("touched", []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))))
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return err
}

// Get returns the content.
func (s *Store) Get(ctx context.Context, mb uuid.UUID, hash string) ([]byte, error) {
	data, _, err := s.pool(ctx, mb).Get(ctx, hash)
	return data, mapErr(err)
}

// Info describes an attachment object.
type Info struct {
	Hash        string
	Size        uint64
	ContentType string
	Created     time.Time
	Touched     time.Time
	Refs        int
	// Version of the object when it was examined; a conditional delete
	// uses it so a reference added meanwhile is never lost.
	Version uint64
}

// Stat returns metadata and the reference count (counts up to 1<<20 refs).
func (s *Store) Stat(ctx context.Context, mb uuid.UUID, hash string) (Info, error) {
	return s.stat(ctx, s.pool(ctx, mb), hash)
}

// StatOn is Stat on a given storage (garbage collection).
func (s *Store) StatOn(ctx context.Context, storageID int64, hash string) (Info, error) {
	return s.stat(ctx, s.poolOf(ctx, storageID), hash)
}

func (s *Store) stat(ctx context.Context, io rados.IOContext, hash string) (Info, error) {
	return s.inspect(ctx, io, hash, 1<<20)
}

func (s *Store) inspect(ctx context.Context, io rados.IOContext, hash string, maxRefs int) (Info, error) {
	st, err := io.Stat(ctx, hash)
	if err != nil {
		return Info{}, mapErr(err)
	}
	info := Info{Hash: hash, Size: st.Size, Created: st.ModTime, Touched: st.ModTime, Version: st.Version}
	if x, err := io.GetXattrs(ctx, hash); err == nil {
		info.ContentType = string(x["content_type"])
		if c, err := strconv.ParseInt(string(x["created"]), 10, 64); err == nil {
			info.Created = time.Unix(c, 0).UTC()
		}
		if c, err := strconv.ParseInt(string(x["touched"]), 10, 64); err == nil {
			info.Touched = time.Unix(c, 0).UTC()
		}
	}
	kv, _, err := io.OmapList(ctx, hash, "ref/", "", maxRefs)
	if err != nil {
		return Info{}, mapErr(err)
	}
	info.Refs = len(kv)
	return info, nil
}

// Refs lists the referencing messages as "mailbox/message" strings.
func (s *Store) Refs(ctx context.Context, storageID int64, hash string) ([]string, error) {
	kv, _, err := s.poolOf(ctx, storageID).OmapList(ctx, hash, "ref/", "", 0)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]string, 0, len(kv))
	for _, e := range kv {
		out = append(out, e.Key[len("ref/"):])
	}
	return out, nil
}

// DeleteIfUnreferenced removes the object when it has no references and was
// not touched within grace. Returns true if deleted.
func (s *Store) DeleteIfUnreferenced(ctx context.Context, storageID int64, hash string, grace time.Duration) (bool, error) {
	info, err := s.GCInfo(ctx, storageID, hash)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if info.Refs > 0 || s.Now().Sub(info.Touched) < grace {
		return false, nil
	}
	// Conditional on the version seen: a delivery that added a reference
	// between our look and the delete bumps the version, and the delete
	// is refused instead of taking the attachment away from a message.
	_, err = s.poolOf(ctx, storageID).Write(ctx, hash, rados.NewWriteTx().AssertVersion(info.Version).Remove())
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) || errors.Is(err, rados.ErrVersionMismatch) {
			return false, nil
		}
		return false, mapErr(err)
	}
	return true, nil
}

// DropDanglingRefs removes references to messages that do not exist
// (deliveries that died after writing the attachment but before the index
// committed). exists answers for a mailbox/message pair on this storage.
// Returns how many references were dropped; the object itself is then an
// ordinary unreferenced attachment for DeleteIfUnreferenced.
func (s *Store) DropDanglingRefs(ctx context.Context, storageID int64, hash string, exists func(ctx context.Context, mb, msg uuid.UUID) (bool, error), expectedVersion ...uint64) (int, error) {
	st, err := s.poolOf(ctx, storageID).Stat(ctx, hash)
	if errors.Is(err, rados.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	version := st.Version
	if len(expectedVersion) > 0 && expectedVersion[0] != version {
		return 0, nil
	}
	refs, err := s.Refs(ctx, storageID, hash)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	dropped := 0
	for _, ref := range refs {
		if len(ref) != 73 || ref[36] != '/' {
			continue
		}
		mb, err1 := uuid.Parse(ref[:36])
		msg, err2 := uuid.Parse(ref[37:])
		if err1 != nil || err2 != nil {
			continue
		}
		ok, err := exists(ctx, mb, msg)
		if err != nil {
			return dropped, err
		}
		if ok {
			continue
		}
		ver, err := s.poolOf(ctx, storageID).Write(ctx, hash, rados.NewWriteTx().AssertVersion(version).OmapRemove(refKey(mb, msg)))
		if errors.Is(err, rados.ErrVersionMismatch) || errors.Is(err, rados.ErrNotFound) {
			return dropped, nil
		}
		if err != nil {
			return dropped, mapErr(err)
		}
		version = ver
		dropped++
	}
	return dropped, nil
}

// List walks all attachment hashes on every connected storage.
func (s *Store) List(ctx context.Context, fn func(storageID int64, hash string) error) error {
	return s.r.Each(ctx, storage.RoleAttachments, func(cid int64, io rados.IOContext) error {
		return io.List(ctx, func(oid string) error {
			if len(oid) != 64 {
				return nil
			}
			return fn(cid, oid)
		})
	})
}

// HasMailboxRef confines cluster-wide content hashes to an authorized mailbox.
func (s *Store) HasMailboxRef(ctx context.Context, hash string, mb uuid.UUID) (bool, error) {
	kv, _, err := s.pool(ctx, mb).OmapList(ctx, hash, "ref/"+mb.String()+"/", "", 1)
	if errors.Is(err, rados.ErrNotFound) {
		return false, nil
	}
	return len(kv) > 0, err
}

// Metadata does not enumerate references (which can number in the millions).
func (s *Store) Metadata(ctx context.Context, mb uuid.UUID, hash string) (Info, error) {
	x, err := s.pool(ctx, mb).GetXattrs(ctx, hash)
	if err != nil {
		return Info{}, mapErr(err)
	}
	return Info{Hash: hash, ContentType: string(x["content_type"])}, nil
}

func candidateOID(hash string) string { return "gc/" + hash[:2] }

// Candidates visits only hashes affected by a reference removal. The shard
// version prevents cleanup from losing a concurrently enqueued candidate.
func (s *Store) Candidates(ctx context.Context, fn func(int64, string) (bool, error)) error {
	return s.r.Each(ctx, storage.RoleAttachments, func(cid int64, io rados.IOContext) error {
		for shard := 0; shard < 256; shard++ {
			oid := fmt.Sprintf("gc/%02x", shard)
			st, err := io.Stat(ctx, oid)
			if errors.Is(err, rados.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			version := st.Version
			after := ""
			for {
				kv, more, err := io.OmapList(ctx, oid, "", after, 512)
				if err != nil {
					return err
				}
				for _, entry := range kv {
					done, err := fn(cid, entry.Key)
					if err != nil {
						return err
					}
					if done {
						ver, err := io.Write(ctx, oid, rados.NewWriteTx().AssertVersion(version).OmapRemove(entry.Key))
						if err == nil {
							version = ver
						} else if !errors.Is(err, rados.ErrVersionMismatch) {
							return err
						}
						// On a version conflict leave candidates for the next pass.
					}
					after = entry.Key
				}
				if !more || len(kv) == 0 {
					break
				}
			}
		}
		return nil
	})
}

// GCInfo only asks whether a reference exists, never counts a large omap.
func (s *Store) GCInfo(ctx context.Context, cid int64, hash string) (Info, error) {
	return s.inspect(ctx, s.poolOf(ctx, cid), hash, 1)
}
