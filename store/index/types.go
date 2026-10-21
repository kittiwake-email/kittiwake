// Package index persists the per-mailbox index in the RADOS index pool:
// the mailbox root object (collections, counters, quota usage), one folder
// index object per folder (omap keyed by UID), the change log, the retention
// set and the message-id/thread maps. See README.md "Mailbox index".
package index

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors.
var (
	ErrNotFound  = errors.New("index: not found")
	ErrExists    = errors.New("index: already exists")
	ErrConflict  = errors.New("index: concurrent modification")
	ErrInvalid   = errors.New("index: invalid")
	ErrNotEmpty  = errors.New("index: folder not empty")
	ErrNoHorizon = errors.New("index: change log horizon passed; resync required")
)

// Collection types.
const (
	CollectionMailFolder = "mail-folder"
)

// Folder roles (IMAP SPECIAL-USE / JMAP role).
const (
	RoleInbox   = "inbox"
	RoleDrafts  = "drafts"
	RoleSent    = "sent"
	RoleJunk    = "junk"
	RoleTrash   = "trash"
	RoleArchive = "archive"
)

// Delimiter separates folder path components in Name.
const Delimiter = "/"

// System flags, as IMAP spells them.
const (
	FlagSeen     = `\Seen`
	FlagAnswered = `\Answered`
	FlagFlagged  = `\Flagged`
	FlagDeleted  = `\Deleted`
	FlagDraft    = `\Draft`
)

// Folder is one mail folder (a collection of type mail-folder).
type Folder struct {
	ID          string    `json:"id"`   // stable, e.g. "f1"
	Type        string    `json:"type"` // CollectionMailFolder
	Name        string    `json:"name"` // full path, Delimiter-separated, e.g. "Projects/2026"
	Role        string    `json:"role,omitempty"`
	UIDValidity uint32    `json:"uidvalidity"`
	UIDNext     uint32    `json:"uidnext"`
	ModSeq      uint64    `json:"modseq"` // modseq of the last change to this folder or its messages
	Subscribed  bool      `json:"subscribed"`
	Keywords    []string  `json:"keywords,omitempty"` // custom keywords used in this folder
	Total       int64     `json:"total"`
	Unseen      int64     `json:"unseen"`
	Size        int64     `json:"size"`
	Created     time.Time `json:"created"`
}

// Usage is the quota usage of a mailbox.
type Usage struct {
	Bytes    int64 `json:"bytes"`
	Messages int64 `json:"messages"`
}

// Root is the mailbox root object's data.
type Root struct {
	Version      int       `json:"version"`
	MailboxID    uuid.UUID `json:"mailbox_id"`
	Folders      []*Folder `json:"folders"`
	NextFolderID uint32    `json:"next_folder_id"`
	ModSeq       uint64    `json:"modseq"`  // mailbox-wide, monotonically increasing
	Horizon      uint64    `json:"horizon"` // lowest modseq still in the change log
	Usage        Usage     `json:"usage"`
	Created      time.Time `json:"created"`
}

// FolderByID returns the folder with the id, or nil.
func (r *Root) FolderByID(id string) *Folder {
	for _, f := range r.Folders {
		if f.ID == id {
			return f
		}
	}
	return nil
}

// FolderByName returns the folder with the full name, or nil.
func (r *Root) FolderByName(name string) *Folder {
	for _, f := range r.Folders {
		if f.Name == name {
			return f
		}
	}
	return nil
}

// FolderByRole returns the folder with the role, or nil.
func (r *Root) FolderByRole(role string) *Folder {
	for _, f := range r.Folders {
		if f.Role == role {
			return f
		}
	}
	return nil
}

// Record is one message entry in a folder index (the omap value).
type Record struct {
	UID            uint32    `json:"uid"`
	MessageID      uuid.UUID `json:"message_id"` // object name is <mailbox>/<message_id>
	ThreadID       string    `json:"thread_id"`
	Flags          []string  `json:"flags,omitempty"`
	Size           int64     `json:"size"`
	InternalDate   time.Time `json:"internal_date"`
	ModSeq         uint64    `json:"modseq"`
	Subject        string    `json:"subject,omitempty"`
	From           string    `json:"from,omitempty"` // display form "Name <addr>"
	FromEmail      string    `json:"from_email,omitempty"`
	Date           time.Time `json:"date"`
	MsgIDHeader    string    `json:"msgid,omitempty"`
	InReplyTo      []string  `json:"in_reply_to,omitempty"`
	References     []string  `json:"references,omitempty"`
	HasAttachments bool      `json:"has_attachments"`
	Preview        string    `json:"preview,omitempty"`
}

// HasFlag reports whether the record carries the flag (case-insensitive for
// system flags and keywords alike, as IMAP requires).
func (r *Record) HasFlag(flag string) bool {
	for _, f := range r.Flags {
		if equalFold(f, flag) {
			return true
		}
	}
	return false
}

// Location says where a message lives.
type Location struct {
	FolderID string
	UID      uint32
}

// Change is one change log entry.
type Change struct {
	ModSeq    uint64    `json:"modseq"`
	Kind      string    `json:"kind"` // "email" or "folder"
	Op        string    `json:"op"`   // "created", "updated", "destroyed"
	ID        string    `json:"id"`   // message uuid for email, folder id for folder
	FolderID  string    `json:"folder_id,omitempty"`
	UID       uint32    `json:"uid,omitempty"`
	Timestamp time.Time `json:"ts"`
}

// Change ops and kinds.
const (
	KindEmail  = "email"
	KindFolder = "folder"

	OpCreated   = "created"
	OpUpdated   = "updated"
	OpDestroyed = "destroyed"
)

// RetentionEntry is a message waiting for purge.
type RetentionEntry struct {
	MessageID uuid.UUID `json:"message_id"`
	FolderID  string    `json:"folder_id"` // folder it was removed from
	UID       uint32    `json:"uid"`
	Size      int64     `json:"size"`
	DeletedAt time.Time `json:"deleted_at"`
	PurgeAt   time.Time `json:"purge_at"`
	Reason    string    `json:"reason"` // "expunge", "trash", "destroy"
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
