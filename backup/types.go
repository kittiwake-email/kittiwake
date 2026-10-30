// Package backup implements mailbox backups to an S3-compatible target:
// the per-mailbox backup run, side-by-side restore, Maildir export from a
// backup, verification and garbage collection, and the `backup` daemon
// mode that executes them from the task queue. See README.md "Backups".
//
// A backup is logical, not a copy of RADOS objects: message documents,
// kept original bytes and attachment payloads are stored as they are, the
// index is stored as the folder list and the per-folder message records.
// Everything is sealed with the backup key ring before it leaves the
// process (see backup/crypto). One run of a changed mailbox writes one or
// more pack objects (new documents and small attachments, the records of
// changed folders, new audit days), large attachments as deduplicated
// blobs, and one manifest. Unchanged folders keep referencing the pack
// entries of earlier runs.
package backup

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"kittiwake/store/identity"
	"kittiwake/store/index"
)

// Sentinel errors.
var (
	ErrNotFound      = errors.New("backup: not found")
	ErrForbidden     = errors.New("backup: forbidden")
	ErrNoBackup      = errors.New("backup: mailbox has no backup")
	ErrDisabled      = errors.New("backup: backups are disabled for the mailbox")
	ErrNotConfigured = errors.New("backup: no backup target configured")
	ErrInvalid       = errors.New("backup: invalid")
)

// ManifestVersion is the manifest format version this code writes.
const ManifestVersion = 1

// Ref locates a sealed object: a whole blob, or an entry inside a pack.
type Ref struct {
	Key    string `json:"k"`           // target key of the pack or blob
	Offset int64  `json:"o,omitempty"` // byte offset inside the pack (0 for a blob)
	Length int64  `json:"l"`           // sealed length
	Size   int64  `json:"s"`           // plaintext size
	SHA256 string `json:"h"`           // plaintext SHA-256, hex
}

// Attachment is one attachment payload of a message.
type Attachment struct {
	Hash        string `json:"hash"` // SHA-256 of the content, as the document names it
	ContentType string `json:"ct,omitempty"`
	Ref         Ref    `json:"ref"`
}

// Message is one message record with its data references. The index
// fields mirror index.Record so a restore can rebuild the record.
type Message struct {
	UID            uint32       `json:"uid"`
	MessageID      uuid.UUID    `json:"id"`
	ThreadID       string       `json:"thread,omitempty"`
	Flags          []string     `json:"flags,omitempty"`
	Size           int64        `json:"size"`
	InternalDate   time.Time    `json:"internal_date"`
	Date           time.Time    `json:"date"`
	Subject        string       `json:"subject,omitempty"`
	From           string       `json:"from,omitempty"`
	FromEmail      string       `json:"from_email,omitempty"`
	HasAttachments bool         `json:"has_attachments,omitempty"`
	MsgIDHeader    string       `json:"message_id,omitempty"`
	Doc            Ref          `json:"doc"`
	Raw            *Ref         `json:"raw,omitempty"`
	Attachments    []Attachment `json:"attachments,omitempty"`
}

// Retained is an expunged message still in the retention set.
type Retained struct {
	Message
	FolderID  string    `json:"folder"`
	DeletedAt time.Time `json:"deleted_at"`
	PurgeAt   time.Time `json:"purge_at"`
	Reason    string    `json:"reason"`
}

// FolderEntry is the per-folder object stored in a pack: the folder as it
// was and every message record in UID order.
type FolderEntry struct {
	Folder   index.Folder `json:"folder"`
	Messages []Message    `json:"messages"`
}

// FolderRef is a folder in the manifest with a reference to its entry.
type FolderRef struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Role        string `json:"role,omitempty"`
	UIDValidity uint32 `json:"uidvalidity"`
	ModSeq      uint64 `json:"modseq"`
	Messages    int    `json:"messages"`
	Bytes       int64  `json:"bytes"`
	Records     Ref    `json:"records"`
	// Packs lists every pack key the entry and its messages reference, so
	// a manifest knows its packs without loading unchanged entries.
	Packs []string `json:"packs"`
}

// AuditDay is one day of the mailbox audit log (JSON lines).
type AuditDay struct {
	Day     string `json:"day"` // YYYY-MM-DD
	Records int    `json:"records"`
	Ref     Ref    `json:"ref"`
}

// RootInfo is what the manifest keeps of the mailbox root.
type RootInfo struct {
	Usage   index.Usage `json:"usage"`
	Created time.Time   `json:"created"`
}

// Manifest describes one backup run of a mailbox.
type Manifest struct {
	Version   int       `json:"version"`
	Mailbox   uuid.UUID `json:"mailbox"`
	Namespace string    `json:"namespace,omitempty"`
	Run       string    `json:"run"`
	Previous  string    `json:"previous,omitempty"`
	Started   time.Time `json:"started"`
	Finished  time.Time `json:"finished"`
	Host      string    `json:"host"`
	ModSeq    uint64    `json:"modseq"`
	// Fuzzy: the mailbox changed while the run copied it (the root modseq
	// moved between the first and last read).
	Fuzzy bool `json:"fuzzy,omitempty"`
	// Full: every folder entry was rewritten (first run, or forced).
	Full      bool                 `json:"full,omitempty"`
	StorageID int64                `json:"storage_id"`
	State     string               `json:"state"`
	Quota     identity.QuotaLimits `json:"quota"`
	Hold      *identity.Hold       `json:"hold,omitempty"`
	Accounts  []string             `json:"accounts"`
	Root      RootInfo             `json:"root"`
	Folders   []FolderRef          `json:"folders"`
	Retention *Ref                 `json:"retention,omitempty"` // entry: []Retained
	Retained  int                  `json:"retained"`
	Audit     []AuditDay           `json:"audit,omitempty"`
	// Packs lists every pack this manifest references (for GC).
	Packs       []string `json:"packs"`
	Messages    int      `json:"messages"`
	NewMessages int      `json:"new_messages"`
	// Bytes written to the target by this run (packs, blobs, manifest).
	Bytes  int64    `json:"bytes"`
	Errors []string `json:"errors,omitempty"`
}

// Run is the catalog row of a completed backup run.
type Run struct {
	ID          string    `json:"id"`
	Mailbox     uuid.UUID `json:"mailbox"`
	Namespace   string    `json:"namespace,omitempty"` // the mailbox's; names the target the run is on
	Started     time.Time `json:"started"`
	Finished    time.Time `json:"finished"`
	ModSeq      uint64    `json:"modseq"`
	Fuzzy       bool      `json:"fuzzy,omitempty"`
	Full        bool      `json:"full,omitempty"`
	ManifestKey string    `json:"manifest_key"`
	Packs       []string  `json:"packs,omitempty"`
	Bytes       int64     `json:"bytes"`
	Messages    int       `json:"messages"`
	NewMessages int       `json:"new_messages"`
	// Expires: when the run leaves retention (zero = kept).
	Expires time.Time `json:"expires,omitempty"`
}

// State is the backup state of a mailbox as the catalog keeps it.
type State struct {
	Mailbox uuid.UUID `json:"mailbox"`
	// Namespace of the mailbox (memory catalog bookkeeping; the database
	// keeps it on the mailbox row).
	Namespace string `json:"namespace,omitempty"`
	// Every is the effective interval; 0 = disabled; unset = not yet scheduled.
	Every       time.Duration `json:"every"`
	Scheduled   bool          `json:"scheduled"`
	Due         time.Time     `json:"due,omitempty"`
	MaintainDue time.Time     `json:"maintain_due,omitempty"`
	LastRun     string        `json:"last_run,omitempty"`
	LastModSeq  uint64        `json:"last_modseq"`
	LastAt      time.Time     `json:"last_at,omitempty"`
	Status      string        `json:"status,omitempty"` // ok | skipped | failed
	Error       string        `json:"error,omitempty"`
	ForceFull   bool          `json:"force_full,omitempty"`
	Verified    time.Time     `json:"verified,omitempty"`
	VerifyError string        `json:"verify_error,omitempty"`
}

// Blob is a deduplicated large attachment on the target.
type Blob struct {
	Name      string    `json:"name"` // keyed name (crypto.Ring.Name)
	Namespace string    `json:"namespace,omitempty"`
	Size      int64     `json:"size"`
	Sealed    int64     `json:"sealed"`
	Created   time.Time `json:"created"`
}

// Job kinds and states. Every job runs on a backup node (the only
// process with the keys and the target); other modes only file them.
const (
	JobRestore        = "restore"         // whole run into a new mailbox
	JobRestoreMessage = "restore-message" // one message into a live mailbox
	JobExport         = "export"          // Maildir archive on the target
	JobVerify         = "verify"          // presence and sampled integrity
	JobForget         = "forget"          // delete every backup of a mailbox

	JobPending = "pending"
	JobRunning = "running"
	JobDone    = "done"
	JobFailed  = "failed"
)

// Job is a restore or export request and its outcome.
type Job struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Mailbox   uuid.UUID `json:"mailbox"`
	Namespace string    `json:"namespace,omitempty"`
	RunID     string    `json:"run_id,omitempty"`
	// Target: the restored mailbox (restore) or the mailbox restored into
	// (restore-message; empty = the source mailbox).
	Target    uuid.UUID `json:"target,omitempty"`
	MessageID uuid.UUID `json:"message_id,omitempty"` // restore-message
	ObjectKey string    `json:"object_key,omitempty"` // export archive
	// URL: a presigned download link for the archive when the target can
	// make one (S3); valid until Expires.
	URL       string          `json:"url,omitempty"`
	Bytes     int64           `json:"bytes,omitempty"`
	Messages  int             `json:"messages,omitempty"`
	Detail    json.RawMessage `json:"detail,omitempty"` // verify report, restore errors
	State     string          `json:"state"`
	Error     string          `json:"error,omitempty"`
	Requested string          `json:"requested_by,omitempty"`
	Created   time.Time       `json:"created"`
	Finished  time.Time       `json:"finished,omitempty"`
	Expires   time.Time       `json:"expires,omitempty"`
}

// Summary is the platform-wide backup picture.
type Summary struct {
	Mailboxes    int       `json:"mailboxes"`
	Scheduled    int       `json:"scheduled"`
	Disabled     int       `json:"disabled"`
	Overdue      int       `json:"overdue"`
	Failed       int       `json:"failed"`
	VerifyFailed int       `json:"verify_failed"`
	OldestDue    time.Time `json:"oldest_due,omitempty"`
	// Rate is the configured aggregate load: the sum over scheduled
	// mailboxes of one run per interval, in runs per hour.
	Rate  float64 `json:"rate_per_hour"`
	Runs  int64   `json:"runs"`
	Bytes int64   `json:"bytes"`
	Blobs int64   `json:"blobs"`
	Jobs  int     `json:"jobs_pending"`
}

type jobJSON struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	Namespace string          `json:"namespace,omitempty"`
	Mailbox   uuid.UUID       `json:"mailbox"`
	RunID     string          `json:"run_id,omitempty"`
	Target    string          `json:"target,omitempty"`
	MessageID string          `json:"message_id,omitempty"`
	ObjectKey string          `json:"object_key,omitempty"`
	URL       string          `json:"url,omitempty"`
	Bytes     int64           `json:"bytes,omitempty"`
	Messages  int             `json:"messages,omitempty"`
	Detail    json.RawMessage `json:"detail,omitempty"`
	State     string          `json:"state"`
	Error     string          `json:"error,omitempty"`
	Requested string          `json:"requested_by,omitempty"`
	Created   time.Time       `json:"created"`
	Finished  *time.Time      `json:"finished,omitempty"`
	Expires   *time.Time      `json:"expires,omitempty"`
}

func optUUID(u uuid.UUID) string {
	if u == uuid.Nil {
		return ""
	}
	return u.String()
}

func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// MarshalJSON leaves unset ids and times out instead of printing zero values.
func (j Job) MarshalJSON() ([]byte, error) {
	return json.Marshal(jobJSON{
		ID: j.ID, Kind: j.Kind, Namespace: j.Namespace, Mailbox: j.Mailbox, RunID: j.RunID, Target: optUUID(j.Target), MessageID: optUUID(j.MessageID),
		ObjectKey: j.ObjectKey, URL: j.URL, Bytes: j.Bytes, Messages: j.Messages, Detail: j.Detail, State: j.State, Error: j.Error,
		Requested: j.Requested, Created: j.Created, Finished: optTime(j.Finished), Expires: optTime(j.Expires),
	})
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (j *Job) UnmarshalJSON(data []byte) error {
	var in jobJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*j = Job{ID: in.ID, Kind: in.Kind, Namespace: in.Namespace, Mailbox: in.Mailbox, RunID: in.RunID, ObjectKey: in.ObjectKey, URL: in.URL, Bytes: in.Bytes, Messages: in.Messages,
		Detail: in.Detail, State: in.State, Error: in.Error, Requested: in.Requested, Created: in.Created}
	if in.Target != "" {
		id, err := uuid.Parse(in.Target)
		if err != nil {
			return err
		}
		j.Target = id
	}
	if in.MessageID != "" {
		id, err := uuid.Parse(in.MessageID)
		if err != nil {
			return err
		}
		j.MessageID = id
	}
	if in.Finished != nil {
		j.Finished = *in.Finished
	}
	if in.Expires != nil {
		j.Expires = *in.Expires
	}
	return nil
}
