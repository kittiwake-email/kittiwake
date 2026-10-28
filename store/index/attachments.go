package index

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrAttachmentRetired = errors.New("index: attachment upload was retired; retry")

type AttachmentRef struct {
	Hash       string `json:"h"`
	Generation string `json:"g"`
}
type Attachment struct {
	Generation  string    `json:"generation"`
	Count       int64     `json:"count"`
	Size        uint64    `json:"size"`
	ContentType string    `json:"content_type"`
	Created     time.Time `json:"created"`
	Touched     time.Time `json:"touched"`
}

func attachmentKey(hash string) string { return "a/" + hash }
func (s *Store) attachment(ctx context.Context, mb uuid.UUID, hash string) (Attachment, error) {
	key := attachmentKey(hash)
	kv, err := s.mapGet(ctx, mb, threadOID(mb, key), []string{key})
	if err != nil {
		return Attachment{}, err
	}
	data, ok := kv[key]
	if !ok {
		return Attachment{}, ErrNotFound
	}
	var a Attachment
	if err := json.Unmarshal(data, &a); err != nil {
		return a, err
	}
	if a.Count < 0 || a.Generation == "" {
		return a, errors.New("index: corrupt attachment counter")
	}
	return a, nil
}

// Attachment returns scalar metadata and the current upload fencing epoch.
func (s *Store) Attachment(ctx context.Context, mb uuid.UUID, hash string) (Attachment, string, error) {
	root, err := s.getRoot(ctx, mb)
	if err != nil {
		return Attachment{}, "", err
	}
	a, err := s.attachment(ctx, mb, hash)
	return a, root.root.AttachmentEpoch, err
}
func (s *Store) putAttachment(ctx context.Context, mb uuid.UUID, hash string, a Attachment) error {
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	key := attachmentKey(hash)
	return s.mapSet(ctx, mb, threadOID(mb, key), key, data)
}

// ReserveAttachment publishes a zero-count generation before payload creation.
// Collection advances a mailbox-wide epoch, including for absent candidates, so
// a delayed upload cannot publish a generation already retired by a collector.
func (s *Store) ReserveAttachment(ctx context.Context, mb uuid.UUID, hash, epoch string, proposed Attachment) (out Attachment, err error) {
	err = s.mutate(ctx, mb, func(ctx context.Context) error {
		vr, err := s.getRoot(ctx, mb)
		if err != nil {
			return err
		}
		if vr.root.AttachmentEpoch != epoch {
			return ErrAttachmentRetired
		}
		a, err := s.attachment(ctx, mb, hash)
		if errors.Is(err, ErrNotFound) {
			a = proposed
		} else if err != nil {
			return err
		}
		if a.Count == 0 {
			a.Touched = s.Now().UTC()
			if err := s.putAttachment(ctx, mb, hash, a); err != nil {
				return err
			}
		}
		out = a
		return nil
	})
	return
}
func (s *Store) changeAttachments(ctx context.Context, mb uuid.UUID, refs []AttachmentRef, delta int64) error {
	seen := map[string]bool{}
	for _, ref := range refs {
		if seen[ref.Hash] {
			return ErrInvalid
		}
		seen[ref.Hash] = true
		a, err := s.attachment(ctx, mb, ref.Hash)
		if errors.Is(err, ErrNotFound) {
			return ErrAttachmentRetired
		}
		if err != nil {
			return err
		}
		if a.Generation != ref.Generation {
			return ErrAttachmentRetired
		}
		if a.Count+delta < 0 {
			return errors.New("index: attachment counter underflow")
		}
		a.Count += delta
		a.Touched = s.Now().UTC()
		if err := s.putAttachment(ctx, mb, ref.Hash, a); err != nil {
			return err
		}
	}
	return nil
}

// RetireAttachment fences a generation before deleting its payload. The index
// journal also fences late uploads when the candidate has no counter yet.
func (s *Store) RetireAttachment(ctx context.Context, mb uuid.UUID, ref AttachmentRef, now time.Time, grace time.Duration) (retired bool, err error) {
	if _, err := s.getRoot(ctx, mb); errors.Is(err, ErrNotFound) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	err = s.mutate(ctx, mb, func(ctx context.Context) error {
		retired = false
		a, err := s.attachment(ctx, mb, ref.Hash)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if err == nil && a.Generation == ref.Generation {
			if a.Count > 0 || now.Sub(a.Touched) < grace {
				return nil
			}
			key := attachmentKey(ref.Hash)
			if err := s.mapRemove(ctx, mb, threadOID(mb, key), key); err != nil {
				return err
			}
		}
		_, err = s.updateRoot(ctx, mb, func(r *Root) error { r.AttachmentEpoch = uuid.NewString(); return nil })
		retired = err == nil
		return err
	})
	return
}
