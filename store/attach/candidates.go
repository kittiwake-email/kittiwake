package attach

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"kittiwake/rados"
	"kittiwake/store/storage"
)

// CandidateQueue keeps the garbage-collection registry outside payload OMAPs.
// A fresh enqueue token fences completion against a concurrent removal.
type CandidateQueue interface {
	EnqueueAttachmentGC(context.Context, int64, string) error
	AttachmentGCCandidates(context.Context, int64, func(string) (bool, error)) error
}

func (s *Store) SetCandidateQueue(q CandidateQueue) { s.candidates = q }
func (s *Store) enqueueCandidate(ctx context.Context, mb uuid.UUID, hash, id string) error {
	cid, io, err := s.r.Pool(ctx, mb, storage.RoleAttachments)
	if err != nil {
		return err
	}
	if s.candidates != nil {
		return s.candidates.EnqueueAttachmentGC(ctx, cid, id)
	}
	_, err = io.Write(ctx, candidateOID(hash), rados.NewWriteTx().OmapSet(map[string][]byte{id: nil}))
	return err
}

func candidateOID(hash string) string { return "gc/" + hash[:2] }

// Candidates visits only hashes affected by a reference removal. The shard
// version prevents cleanup from losing a concurrently enqueued candidate.
func (s *Store) Candidates(ctx context.Context, fn func(int64, string) (bool, error)) error {
	return s.r.Each(ctx, storage.RoleAttachments, func(cid int64, io rados.IOContext) error {
		if s.candidates != nil {
			return s.candidates.AttachmentGCCandidates(ctx, cid, func(id string) (bool, error) { return fn(cid, id) })
		}
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
					fresh, err := io.Stat(ctx, oid)
					if err != nil {
						return err
					}
					version = fresh.Version
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
