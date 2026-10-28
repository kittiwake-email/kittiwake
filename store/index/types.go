// Package index persists the per-mailbox index in the RADOS index pool:
// the mailbox root object (collections, counters, quota usage), one folder
// index object per folder (omap keyed by UID), the change log, the retention
// set and the message-id/thread maps. See README.md "Mailbox index".
package index

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors.
var (
	ErrNotFound  = errors.New("index: not found")
	ErrExists    = errors.New("index: already exists")
	ErrOverQuota = errors.New("index: hard quota exceeded")
	ErrPending   = errors.New("index: mutation pending recovery")
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

// A folder's Name is a Delimiter-separated path. A component that itself
// contains the delimiter (or a backslash) is stored in escaped form, the
// way Dovecot's listescape plugin does it: "\2f" for "/" and "\5c" for
// "\". IMAP passes the stored form through unchanged, which keeps old
// clients and Dovecot-migrated folder names working; JMAP shows the
// decoded name.

// EscapeComponent encodes a single display name as a path component.
func EscapeComponent(s string) string {
	if !strings.ContainsAny(s, Delimiter+"\\") {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString("\\5c")
		case rune(Delimiter[0]):
			b.WriteString("\\2f")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// UnescapeComponent decodes "\xx" sequences; malformed ones stay as is.
func UnescapeComponent(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+2 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

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
	// Retention uses consecutive 65,536-deletion ranges, independently of live quota.
	RetentionNext  uint64    `json:"retention_next,omitempty"`
	RetentionFirst uint64    `json:"retention_first,omitempty"`
	Mutation       *mutation `json:"mutation,omitempty"`
	Version        int       `json:"version"`
	MailboxID      uuid.UUID `json:"mailbox_id"`
	Folders        []*Folder `json:"folders"`
	NextFolderID   uint32    `json:"next_folder_id"`
	ModSeq         uint64    `json:"modseq"`  // mailbox-wide, monotonically increasing
	Horizon        uint64    `json:"horizon"` // lowest modseq still in the change log
	Usage          Usage     `json:"usage"`
	Created        time.Time `json:"created"`

	// pending change-log entries written with the next root transaction.
	pending []Change
}

// Log queues a change-log entry to be written atomically with the root.
func (r *Root) Log(c Change) { r.pending = append(r.pending, c) }

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

// Record is one message entry in a folder index (the omap value). It is
// kept small on purpose: one folder may hold hundreds of thousands of these
// and listings read them all. Everything else about a message lives in its
// document object. JSON tags are short to save omap bytes.
type Record struct {
	UID            uint32    `json:"u"`
	MessageID      uuid.UUID `json:"m"` // object name is <mailbox>/<m>
	ThreadID       string    `json:"t"`
	Flags          []string  `json:"f,omitempty"`
	Size           int64     `json:"s"`
	InternalDate   time.Time `json:"d"`
	ModSeq         uint64    `json:"q"`
	Date           time.Time `json:"e"` // sent date (Date header)
	Subject        string    `json:"j,omitempty"`
	From           string    `json:"fr,omitempty"` // display form "Name <addr>"
	FromEmail      string    `json:"fe,omitempty"`
	HasAttachments bool      `json:"a,omitempty"`

	// Message-ID is retained to reclaim its mapping when the last live copy leaves.
	// References and InReplyTo are Append input only.
	MsgIDHeader string   `json:"mh,omitempty"`
	InReplyTo   []string `json:"-"`
	References  []string `json:"-"`
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
