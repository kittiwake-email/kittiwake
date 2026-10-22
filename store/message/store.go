// Package message persists parsed message documents in the RADOS messages
// pool: one object per message, `<mailbox>/<message-uuid>`, holding the JSON
// document. Xattrs: `mailbox`, `created` (for the orphan sweep) and `loc`
// (the folder/UID the index placed it at, so a message id resolves without a
// per-message omap entry anywhere). The original bytes are not kept; when the
// delivery policy requires them (signed messages) they go to
// `<mailbox>/<message-uuid>.raw`.
package message

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/store/storage"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("message: not found")
	ErrExists   = errors.New("message: already exists")
)

// Store is the message store.
type Store struct {
	r   storage.Resolver
	Now func() time.Time
}

// New returns a store on the messages pool context (one storage).
func New(messagesPool rados.IOContext) *Store {
	return NewMulti(storage.FixedPools(messagesPool, messagesPool, messagesPool))
}

// NewMulti returns a store that resolves each mailbox's messages pool through r.
func NewMulti(r storage.Resolver) *Store { return &Store{r: r, Now: time.Now} }

func (s *Store) pool(ctx context.Context, mb uuid.UUID) rados.IOContext {
	_, io, err := s.r.Pool(ctx, mb, storage.RoleMessages)
	if err != nil {
		return rados.ErrorContext(err)
	}
	return io
}

func docOID(mb, msg uuid.UUID) string { return mb.String() + "/" + msg.String() }
func rawOID(mb, msg uuid.UUID) string { return docOID(mb, msg) + ".raw" }

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, rados.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, rados.ErrExists):
		return ErrExists
	}
	return err
}

// Put stores the document for a new message with its index location
// ("<folder>/<uid>"). Created exclusively; a message uuid is never reused.
func (s *Store) Put(ctx context.Context, mb, msg uuid.UUID, doc []byte, loc string) error {
	tx := rados.NewWriteTx().CreateExclusive().WriteFull(doc).
		SetXattr("mailbox", []byte(mb.String())).
		SetXattr("created", []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))).
		SetXattr("loc", []byte(loc))
	_, err := s.pool(ctx, mb).Write(ctx, docOID(mb, msg), tx)
	return mapErr(err)
}

// PutRaw keeps the original bytes next to the document (signed messages).
func (s *Store) PutRaw(ctx context.Context, mb, msg uuid.UUID, raw []byte) error {
	_, err := s.pool(ctx, mb).Write(ctx, rawOID(mb, msg), rados.NewWriteTx().CreateExclusive().WriteFull(raw).
		SetXattr("mailbox", []byte(mb.String())).SetXattr("created", []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))))
	return mapErr(err)
}

// GetDoc returns the document JSON.
func (s *Store) GetDoc(ctx context.Context, mb, msg uuid.UUID) ([]byte, error) {
	data, _, err := s.pool(ctx, mb).Get(ctx, docOID(mb, msg))
	return data, mapErr(err)
}

// GetRaw returns the kept original bytes, ErrNotFound when none were kept.
func (s *Store) GetRaw(ctx context.Context, mb, msg uuid.UUID) ([]byte, error) {
	data, _, err := s.pool(ctx, mb).Get(ctx, rawOID(mb, msg))
	return data, mapErr(err)
}

// Location returns the "<folder>/<uid>" the message is indexed at.
func (s *Store) Location(ctx context.Context, mb, msg uuid.UUID) (string, error) {
	x, err := s.pool(ctx, mb).GetXattrs(ctx, docOID(mb, msg))
	if err != nil {
		return "", mapErr(err)
	}
	return string(x["loc"]), nil
}

// SetLocation records a new location after a move.
func (s *Store) SetLocation(ctx context.Context, mb, msg uuid.UUID, loc string) error {
	_, err := s.pool(ctx, mb).Write(ctx, docOID(mb, msg), rados.NewWriteTx().AssertExists().SetXattr("loc", []byte(loc)))
	return mapErr(err)
}

// Delete removes the document and any kept raw copy. Missing is not an error.
func (s *Store) Delete(ctx context.Context, mb, msg uuid.UUID) error {
	for _, oid := range []string{docOID(mb, msg), rawOID(mb, msg)} {
		if err := s.pool(ctx, mb).Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	return nil
}

// Info describes a stored message object for sweeps.
type Info struct {
	StorageID int64
	Mailbox   uuid.UUID
	Message   uuid.UUID
	Created   time.Time
	Size      uint64
	Location  string
}

// List walks every message document on every connected storage (not the
// .raw objects), calling fn with its info. Used by the orphan sweep and
// mailbox removal; act on an Info with storage.WithID(ctx, info.StorageID).
func (s *Store) List(ctx context.Context, fn func(Info) error) error {
	return s.r.Each(ctx, storage.RoleMessages, func(cid int64, io rados.IOContext) error {
		return io.List(ctx, func(oid string) error {
			if len(oid) != 73 || oid[36] != '/' { // uuid/uuid
				return nil
			}
			mb, err1 := uuid.Parse(oid[:36])
			msg, err2 := uuid.Parse(oid[37:])
			if err1 != nil || err2 != nil {
				return nil
			}
			st, err := io.Stat(ctx, oid)
			if err != nil {
				if errors.Is(err, rados.ErrNotFound) {
					return nil
				}
				return mapErr(err)
			}
			info := Info{StorageID: cid, Mailbox: mb, Message: msg, Size: st.Size, Created: st.ModTime}
			if x, err := io.GetXattrs(ctx, oid); err == nil {
				if c, err := strconv.ParseInt(string(x["created"]), 10, 64); err == nil {
					info.Created = time.Unix(c, 0).UTC()
				}
				info.Location = string(x["loc"])
			}
			return fn(info)
		})
	})
}
