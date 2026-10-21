// Package message persists parsed message documents and raw skeletons in the
// RADOS messages pool: `<mailbox>/<message-uuid>` (JSON) and
// `<mailbox>/<message-uuid>.raw` (skeleton). Each object carries xattrs
// `mailbox` and `created` so an orphan sweep can find objects that never
// reached an index.
package message

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("message: not found")
	ErrExists   = errors.New("message: already exists")
)

// Store is the message store.
type Store struct {
	io  rados.IOContext
	Now func() time.Time
}

// New returns a store on the messages pool context.
func New(messagesPool rados.IOContext) *Store {
	return &Store{io: messagesPool, Now: time.Now}
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

// Put stores the document JSON and the raw skeleton for a new message.
// Both objects are created exclusively; a message uuid is never reused.
func (s *Store) Put(ctx context.Context, mb, msg uuid.UUID, doc, skeleton []byte) error {
	created := []byte(strconv.FormatInt(s.Now().UTC().Unix(), 10))
	mbx := []byte(mb.String())
	// Raw first, document second: the document is what delivery/JMAP read,
	// so a document without raw never exists after a crash.
	if _, err := s.io.Write(ctx, rawOID(mb, msg), rados.NewWriteTx().CreateExclusive().WriteFull(skeleton).SetXattr("mailbox", mbx).SetXattr("created", created)); err != nil {
		return mapErr(err)
	}
	if _, err := s.io.Write(ctx, docOID(mb, msg), rados.NewWriteTx().CreateExclusive().WriteFull(doc).SetXattr("mailbox", mbx).SetXattr("created", created)); err != nil {
		return mapErr(err)
	}
	return nil
}

// GetDoc returns the document JSON.
func (s *Store) GetDoc(ctx context.Context, mb, msg uuid.UUID) ([]byte, error) {
	data, _, err := s.io.Get(ctx, docOID(mb, msg))
	return data, mapErr(err)
}

// GetRaw returns the raw skeleton.
func (s *Store) GetRaw(ctx context.Context, mb, msg uuid.UUID) ([]byte, error) {
	data, _, err := s.io.Get(ctx, rawOID(mb, msg))
	return data, mapErr(err)
}

// Delete removes both objects. Missing objects are not an error.
func (s *Store) Delete(ctx context.Context, mb, msg uuid.UUID) error {
	for _, oid := range []string{docOID(mb, msg), rawOID(mb, msg)} {
		if err := s.io.Delete(ctx, oid); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	return nil
}

// Info describes a stored message object for sweeps.
type Info struct {
	Mailbox uuid.UUID
	Message uuid.UUID
	Created time.Time
	Size    uint64
}

// List walks every message document in the pool (not the .raw objects),
// calling fn with its info. Used by the orphan sweep and mailbox removal.
func (s *Store) List(ctx context.Context, fn func(Info) error) error {
	return s.io.List(ctx, func(oid string) error {
		if len(oid) != 73 || oid[36] != '/' { // uuid/uuid
			return nil
		}
		mb, err1 := uuid.Parse(oid[:36])
		msg, err2 := uuid.Parse(oid[37:])
		if err1 != nil || err2 != nil {
			return nil
		}
		st, err := s.io.Stat(ctx, oid)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				return nil
			}
			return mapErr(err)
		}
		info := Info{Mailbox: mb, Message: msg, Size: st.Size, Created: st.ModTime}
		if x, err := s.io.GetXattrs(ctx, oid); err == nil {
			if c, err := strconv.ParseInt(string(x["created"]), 10, 64); err == nil {
				info.Created = time.Unix(c, 0).UTC()
			}
		}
		return fn(info)
	})
}

// String form of a message object name, for logs and audit.
func ObjectName(mb, msg uuid.UUID) string { return fmt.Sprintf("%s/%s", mb, msg) }
