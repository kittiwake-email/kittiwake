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
	"strings"
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

// ---- JMAP uploads ----
//
// Blobs uploaded through JMAP (drafts to import, attachments for Email/set)
// live in the "uploads" namespace of the mailbox's messages pool, so any
// JMAP node behind the load balancer can use a blob another node received.
// They expire after UploadTTL; the worker's upload sweep deletes expired
// ones and mailbox removal deletes a mailbox's uploads with the rest.

const uploadsNS = "uploads"

// UploadTTL is how long an uploaded blob can be referenced.
const UploadTTL = time.Hour

func uploadOID(mb uuid.UUID, id string) string { return mb.String() + "/" + id }

// PutUpload stores an uploaded blob for the mailbox.
func (s *Store) PutUpload(ctx context.Context, mb uuid.UUID, id string, data []byte, typ string) error {
	expires := s.Now().Add(UploadTTL).Unix()
	tx := rados.NewWriteTx().CreateExclusive().WriteFull(data).
		SetXattr("type", []byte(typ)).SetXattr("expires", []byte(strconv.FormatInt(expires, 10)))
	_, err := s.pool(ctx, mb).Namespace(uploadsNS).Write(ctx, uploadOID(mb, id), tx)
	return mapErr(err)
}

// GetUpload returns an uploaded blob and its media type; ErrNotFound when
// absent or expired.
func (s *Store) GetUpload(ctx context.Context, mb uuid.UUID, id string) ([]byte, string, error) {
	io := s.pool(ctx, mb).Namespace(uploadsNS)
	x, err := io.GetXattrs(ctx, uploadOID(mb, id))
	if err != nil {
		return nil, "", mapErr(err)
	}
	if exp, err := strconv.ParseInt(string(x["expires"]), 10, 64); err == nil && s.Now().Unix() > exp {
		return nil, "", ErrNotFound
	}
	data, _, err := io.Get(ctx, uploadOID(mb, id))
	if err != nil {
		return nil, "", mapErr(err)
	}
	return data, string(x["type"]), nil
}

// DeleteUpload removes an uploaded blob (missing is fine).
func (s *Store) DeleteUpload(ctx context.Context, mb uuid.UUID, id string) error {
	err := s.pool(ctx, mb).Namespace(uploadsNS).Delete(ctx, uploadOID(mb, id))
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return mapErr(err)
}

// DeleteUploads removes every upload of a mailbox (mailbox removal).
func (s *Store) DeleteUploads(ctx context.Context, mb uuid.UUID) (int, error) {
	io := s.pool(ctx, mb).Namespace(uploadsNS)
	var oids []string
	prefix := mb.String() + "/"
	if err := io.List(ctx, func(oid string) error {
		if strings.HasPrefix(oid, prefix) {
			oids = append(oids, oid)
		}
		return nil
	}); err != nil {
		return 0, mapErr(err)
	}
	for _, oid := range oids {
		if err := io.Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return 0, mapErr(err)
		}
	}
	return len(oids), nil
}

// SweepUploads deletes expired uploads on every connected storage and
// returns how many it removed. The uploads namespace only ever holds
// blobs younger than UploadTTL (plus whatever a crash left), so listing
// it is cheap however large the messages pool is.
func (s *Store) SweepUploads(ctx context.Context) (int, error) {
	removed := 0
	now := s.Now().Unix()
	err := s.r.Each(ctx, storage.RoleMessages, func(_ int64, io rados.IOContext) error {
		io = io.Namespace(uploadsNS)
		var expired []string
		if err := io.List(ctx, func(oid string) error {
			x, err := io.GetXattrs(ctx, oid)
			if err != nil {
				if errors.Is(err, rados.ErrNotFound) {
					return nil
				}
				return mapErr(err)
			}
			exp, err := strconv.ParseInt(string(x["expires"]), 10, 64)
			if err != nil || now > exp {
				expired = append(expired, oid) // unreadable expiry: treat as expired
			}
			return nil
		}); err != nil {
			return err
		}
		for _, oid := range expired {
			if err := io.Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
				return mapErr(err)
			}
			removed++
		}
		return nil
	})
	return removed, err
}
