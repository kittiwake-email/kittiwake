// Package attach stores immutable mailbox-scoped attachment payloads. Scalar
// reference counts live in the mailbox index journal, not in payload OMAPs.
package attach

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
	"kittiwake/store/index"
	"kittiwake/store/storage"
)

var ErrNotFound = errors.New("attach: not found")
var ErrHash = errors.New("attach: content does not match hash")

type Store struct {
	r          storage.Resolver
	idx        *index.Store
	candidates CandidateQueue
	Now        func() time.Time
}

func New(io rados.IOContext) *Store { return NewMulti(storage.FixedPools(io, io, io)) }
func NewMulti(r storage.Resolver) *Store {
	s := &Store{r: r, idx: index.NewMulti(r), Now: time.Now}
	s.idx.Now = func() time.Time { return s.Now() }
	return s
}

// BindIndex must run during construction, before the store is used. Production
// services share the same index; fixtures can use separate physical pools.
func (s *Store) BindIndex(idx *index.Store) { s.idx = idx }
func Hash(data []byte) string               { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil && hash == strings.ToLower(hash)
}
func payloadID(mb uuid.UUID, ref index.AttachmentRef) string {
	return fmt.Sprintf("v3/%s/%s/%s", mb, ref.Hash, ref.Generation)
}
func parsePayloadID(id string) (uuid.UUID, index.AttachmentRef, error) {
	p := strings.Split(id, "/")
	if len(p) != 4 || p[0] != "v3" || !validHash(p[2]) {
		return uuid.Nil, index.AttachmentRef{}, ErrHash
	}
	mb, err := uuid.Parse(p[1])
	if err != nil {
		return uuid.Nil, index.AttachmentRef{}, ErrHash
	}
	if _, err := uuid.Parse(p[3]); err != nil {
		return uuid.Nil, index.AttachmentRef{}, ErrHash
	}
	return mb, index.AttachmentRef{Hash: p[2], Generation: p[3]}, nil
}
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
func mapErr(err error) error {
	if errors.Is(err, rados.ErrNotFound) || errors.Is(err, index.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

// Upload reserves an immutable generation and writes its payload. It does not
// add a reference: Append must include the returned ref in its root journal.
func (s *Store) Upload(ctx context.Context, hash string, data []byte, contentType string, mb uuid.UUID) (index.AttachmentRef, error) {
	if Hash(data) != hash {
		return index.AttachmentRef{}, ErrHash
	}
	if _, err := s.idx.EnsureRoot(ctx, mb); err != nil {
		return index.AttachmentRef{}, err
	}
	for attempt := 0; attempt < 32; attempt++ {
		a, epoch, err := s.idx.Attachment(ctx, mb, hash)
		if err != nil && !errors.Is(err, index.ErrNotFound) {
			return index.AttachmentRef{}, err
		}
		if err == nil && a.Count > 0 {
			return index.AttachmentRef{Hash: hash, Generation: a.Generation}, nil
		}
		if errors.Is(err, index.ErrNotFound) {
			a = index.Attachment{Generation: uuid.NewString(), Size: uint64(len(data)), ContentType: contentType, Created: s.Now().UTC(), Touched: s.Now().UTC()}
		}
		proposed := index.AttachmentRef{Hash: hash, Generation: a.Generation}
		if err := s.enqueueCandidate(ctx, mb, hash, payloadID(mb, proposed)); err != nil {
			return index.AttachmentRef{}, err
		}
		a, err = s.idx.ReserveAttachment(ctx, mb, hash, epoch, a)
		if errors.Is(err, index.ErrAttachmentRetired) {
			continue
		}
		if err != nil {
			return index.AttachmentRef{}, err
		}
		ref := index.AttachmentRef{Hash: hash, Generation: a.Generation}
		if a.Count == 0 {
			if ref != proposed {
				if err := s.enqueueCandidate(ctx, mb, hash, payloadID(mb, ref)); err != nil {
					return ref, err
				}
			}
			_, err = s.pool(ctx, mb).Write(ctx, payloadID(mb, ref), rados.NewWriteTx().CreateExclusive().WriteFull(data))
			if err != nil && !errors.Is(err, rados.ErrExists) {
				return ref, err
			}
		}
		return ref, nil
	}
	return index.AttachmentRef{}, index.ErrConflict
}

// Put is upload-only. Callers publishing mail use Upload and put its ref in the
// index record, so retries never depend on per-message attachment receipts.
func (s *Store) Put(ctx context.Context, hash string, data []byte, contentType string, mb, msg uuid.UUID) error {
	_, err := s.Upload(ctx, hash, data, contentType, mb)
	return err
}
func (s *Store) MarkGC(ctx context.Context, mb uuid.UUID, refs []index.AttachmentRef) error {
	seen := make(map[index.AttachmentRef]bool)
	for _, ref := range refs {
		if seen[ref] {
			continue
		}
		seen[ref] = true
		if err := s.enqueueCandidate(ctx, mb, ref.Hash, payloadID(mb, ref)); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) Get(ctx context.Context, mb uuid.UUID, hash string) ([]byte, error) {
	if !validHash(hash) {
		return nil, ErrHash
	}
	a, _, err := s.idx.Attachment(ctx, mb, hash)
	if err != nil {
		return nil, mapErr(err)
	}
	data, _, err := s.pool(ctx, mb).Get(ctx, payloadID(mb, index.AttachmentRef{Hash: hash, Generation: a.Generation}))
	return data, mapErr(err)
}

type Info struct {
	Hash             string
	Size             uint64
	ContentType      string
	Created, Touched time.Time
	Refs             int
	Version          uint64
	counterPresent   bool
}

func attachmentInfo(id string, a index.Attachment) Info {
	return Info{Hash: id, Size: a.Size, ContentType: a.ContentType, Created: a.Created, Touched: a.Touched, Refs: int(a.Count), counterPresent: true}
}
func (s *Store) Stat(ctx context.Context, mb uuid.UUID, hash string) (Info, error) {
	if strings.HasPrefix(hash, "v3/") {
		cid, _, err := s.r.Pool(ctx, mb, storage.RoleAttachments)
		if err != nil {
			return Info{}, err
		}
		return s.GCInfo(ctx, cid, hash)
	}
	if !validHash(hash) {
		return Info{}, ErrHash
	}
	a, _, err := s.idx.Attachment(ctx, mb, hash)
	if err != nil {
		return Info{}, mapErr(err)
	}
	return attachmentInfo(payloadID(mb, index.AttachmentRef{Hash: hash, Generation: a.Generation}), a), nil
}
func (s *Store) Metadata(ctx context.Context, mb uuid.UUID, hash string) (Info, error) {
	return s.Stat(ctx, mb, hash)
}
func (s *Store) HasMailboxRef(ctx context.Context, hash string, mb uuid.UUID) (bool, error) {
	a, err := s.Stat(ctx, mb, hash)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil && a.Refs > 0, err
}
func (s *Store) StatOn(ctx context.Context, cid int64, id string) (Info, error) {
	return s.GCInfo(ctx, cid, id)
}

// Observe the payload before the index. A concurrent upload must reserve its
// generation before creating data; collection then fences publication via the
// mailbox epoch even when no payload or counter existed at the first read.
func (s *Store) GCInfo(ctx context.Context, cid int64, id string) (Info, error) {
	mb, ref, err := parsePayloadID(id)
	if err != nil {
		return Info{}, err
	}
	st, err := s.poolOf(ctx, cid).Stat(ctx, id)
	missing := errors.Is(err, rados.ErrNotFound)
	if err != nil && !missing {
		return Info{}, err
	}
	a, epoch, err := s.idx.Attachment(storage.WithID(ctx, cid), mb, ref.Hash)
	if err != nil && !errors.Is(err, index.ErrNotFound) {
		return Info{}, err
	}
	if err == nil && a.Generation == ref.Generation {
		info := attachmentInfo(id, a)
		info.Version = st.Version
		return info, nil
	}
	if missing && epoch == "" {
		return Info{}, ErrNotFound
	}
	return Info{Hash: id, Size: st.Size, Created: st.ModTime, Touched: st.ModTime, Version: st.Version}, nil
}
func (s *Store) DeleteIfUnreferenced(ctx context.Context, cid int64, id string, grace time.Duration) (bool, error) {
	mb, ref, err := parsePayloadID(id)
	if err != nil {
		return false, err
	}
	info, err := s.GCInfo(ctx, cid, id)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Refs > 0 || (!info.Touched.IsZero() && s.Now().Sub(info.Touched) < grace) {
		return false, nil
	}
	retired, err := s.idx.RetireAttachment(storage.WithID(ctx, cid), mb, ref, s.Now(), grace)
	if err != nil || !retired {
		return false, err
	}
	err = s.poolOf(ctx, cid).Delete(ctx, id)
	if errors.Is(err, rados.ErrNotFound) {
		err = nil
	}
	return true, err
}
func (s *Store) List(ctx context.Context, fn func(int64, string) error) error {
	return s.r.Each(ctx, storage.RoleAttachments, func(cid int64, io rados.IOContext) error {
		return io.List(ctx, func(oid string) error {
			if _, _, err := parsePayloadID(oid); err == nil {
				return fn(cid, oid)
			}
			return nil
		})
	})
}
