package attach

import (
	"context"

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
