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

	"dovetail/rados"
	"dovetail/store/cluster"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("attach: not found")
	ErrHash     = errors.New("attach: content does not match hash")
)

// Store is the attachment store.
type Store struct {
	r   cluster.Resolver
	Now func() time.Time
}

// New returns a store on the attachments pool context (one cluster).
func New(attachmentsPool rados.IOContext) *Store {
	return NewMulti(cluster.FixedPools(attachmentsPool, attachmentsPool, attachmentsPool))
}

// NewMulti returns a store that resolves each mailbox's attachments pool
// through r. Deduplication is per cluster: the same content stored for
// mailboxes on two clusters is two objects.
func NewMulti(r cluster.Resolver) *Store { return &Store{r: r, Now: time.Now} }

func (s *Store) pool(ctx context.Context, mb uuid.UUID) rados.IOContext {
	_, io, err := s.r.Pool(ctx, mb, cluster.RoleAttachments)
	if err != nil {
		return rados.ErrorContext(err)
	}
	return io
}

func (s *Store) poolOf(ctx context.Context, cid int64) rados.IOContext {
	io, err := s.r.PoolOf(ctx, cid, cluster.RoleAttachments)
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
	_, err := s.pool(ctx, mb).Write(ctx, hash, tx)
	if err == nil {
		return nil
	}
	if !errors.Is(err, rados.ErrExists) {
		return err
	}
	return s.AddRef(ctx, hash, mb, msg)
}

// AddRef records that message references the attachment. The object must exist.
func (s *Store) AddRef(ctx context.Context, hash string, mb, msg uuid.UUID) error {
	_, err := s.pool(ctx, mb).Write(ctx, hash, rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{refKey(mb, msg): nil}).SetXattr("touched", []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))))
	return mapErr(err)
}

// RemoveRef drops a message's reference. Missing object or key is not an error.
func (s *Store) RemoveRef(ctx context.Context, hash string, mb, msg uuid.UUID) error {
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
}

// Stat returns metadata and the reference count (counts up to 1<<20 refs).
func (s *Store) Stat(ctx context.Context, mb uuid.UUID, hash string) (Info, error) {
	return s.stat(ctx, s.pool(ctx, mb), hash)
}

// StatOn is Stat on a given cluster (garbage collection).
func (s *Store) StatOn(ctx context.Context, clusterID int64, hash string) (Info, error) {
	return s.stat(ctx, s.poolOf(ctx, clusterID), hash)
}

func (s *Store) stat(ctx context.Context, io rados.IOContext, hash string) (Info, error) {
	st, err := io.Stat(ctx, hash)
	if err != nil {
		return Info{}, mapErr(err)
	}
	info := Info{Hash: hash, Size: st.Size, Created: st.ModTime, Touched: st.ModTime}
	if x, err := io.GetXattrs(ctx, hash); err == nil {
		info.ContentType = string(x["content_type"])
		if c, err := strconv.ParseInt(string(x["created"]), 10, 64); err == nil {
			info.Created = time.Unix(c, 0).UTC()
		}
		if c, err := strconv.ParseInt(string(x["touched"]), 10, 64); err == nil {
			info.Touched = time.Unix(c, 0).UTC()
		}
	}
	kv, _, err := io.OmapList(ctx, hash, "ref/", "", 1<<20)
	if err != nil {
		return Info{}, mapErr(err)
	}
	info.Refs = len(kv)
	return info, nil
}

// Refs lists the referencing messages as "mailbox/message" strings.
func (s *Store) Refs(ctx context.Context, clusterID int64, hash string) ([]string, error) {
	kv, _, err := s.poolOf(ctx, clusterID).OmapList(ctx, hash, "ref/", "", 0)
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
func (s *Store) DeleteIfUnreferenced(ctx context.Context, clusterID int64, hash string, grace time.Duration) (bool, error) {
	info, err := s.StatOn(ctx, clusterID, hash)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if info.Refs > 0 || s.Now().Sub(info.Touched) < grace {
		return false, nil
	}
	if err := s.poolOf(ctx, clusterID).Delete(ctx, hash); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return false, err
	}
	return true, nil
}

// List walks all attachment hashes on every connected cluster.
func (s *Store) List(ctx context.Context, fn func(clusterID int64, hash string) error) error {
	return s.r.Each(ctx, cluster.RoleAttachments, func(cid int64, io rados.IOContext) error {
		return io.List(ctx, func(oid string) error {
			if len(oid) != 64 {
				return nil
			}
			return fn(cid, oid)
		})
	})
}
