// Package admin is a Go client for the Kittiwake admin REST API
// (api/openapi.yaml). It is a plain HTTP client: it imports nothing from
// the server packages so third parties can use it.
package admin

import (
	"encoding/json"
	"time"
)

// Status is GET /v1/admin/status.
type Status struct {
	Version string    `json:"version"`
	Time    time.Time `json:"time"`
	// Schema is absent with the RADOS identity backend (no database).
	Schema *SchemaInfo `json:"schema,omitempty"`
}

// SchemaInfo is the metadata schema in place.
type SchemaInfo struct {
	Version   int       `json:"version"`
	AppliedAt time.Time `json:"applied_at"`
	AppliedBy string    `json:"applied_by"`
}

// Page is a paginated list of names (domains, accounts, aliases).
type Page struct {
	Items []string `json:"items"`
	More  bool     `json:"more"`
	Next  string   `json:"next,omitempty"`
}

// Domain is a mail domain.
type Domain struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Enabled   bool              `json:"enabled"`
	Settings  map[string]string `json:"settings,omitempty"`
	Created   time.Time         `json:"created"`
	Updated   time.Time         `json:"updated"`
}

// DomainRequest creates or patches a domain. Nil fields are left unchanged.
// Namespace applies on create only (default: the token's own, else
// "default").
type DomainRequest struct {
	Name      string            `json:"name,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Enabled   *bool             `json:"enabled,omitempty"`
	Settings  map[string]string `json:"settings,omitempty"`
}

// Namespace is the unit of tenant isolation: it owns storage, domains and
// mailboxes. "default" always exists.
type Namespace struct {
	Name     string            `json:"name"`
	Enabled  bool              `json:"enabled"`
	Settings map[string]string `json:"settings,omitempty"`
	// Ingress is the hostname proxies connect to for this namespace, with
	// its TLS policy (strict, insecure, off; empty = the proxy's default).
	// Nil: proxies pick the namespace's nodes from the node registry.
	Ingress *Ingress  `json:"ingress,omitempty"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
}

// Ingress is a namespace's entry point for proxies.
type Ingress struct {
	Host string `json:"host"`
	TLS  string `json:"tls,omitempty"`
}

// NamespaceRequest creates or patches a namespace. Names are immutable.
// Ingress, when present, replaces the ingress; an empty host removes it.
type NamespaceRequest struct {
	Name     string            `json:"name,omitempty"`
	Enabled  *bool             `json:"enabled,omitempty"`
	Settings map[string]string `json:"settings,omitempty"`
	Ingress  *Ingress          `json:"ingress,omitempty"`
}

// QuotaLimits are per-mailbox limits; zero means the configured default.
type QuotaLimits struct {
	SoftBytes    int64 `json:"soft_bytes"`
	HardBytes    int64 `json:"hard_bytes"`
	SoftMessages int64 `json:"soft_messages"`
	HardMessages int64 `json:"hard_messages"`
}

// Hold is a legal hold record.
type Hold struct {
	Reason         string    `json:"reason"`
	CaseRef        string    `json:"case_ref,omitempty"`
	SetBy          string    `json:"set_by"`
	SetAt          time.Time `json:"set_at"`
	AcceptDelivery bool      `json:"accept_delivery"`
	AllowSend      bool      `json:"allow_send"`
	Sealed         bool      `json:"sealed"`
}

// Mailbox is the identity record of a mailbox.
type Mailbox struct {
	ID          string         `json:"id"`
	Namespace   string         `json:"namespace"`
	StorageID   int64          `json:"storage_id"`
	State       string         `json:"state"` // active | suspended | disabled
	StateReason string         `json:"state_reason,omitempty"`
	StateSince  time.Time      `json:"state_since"`
	Quota       QuotaLimits    `json:"quota"`
	Hold        *Hold          `json:"hold,omitempty"`
	Released    *time.Time     `json:"released,omitempty"`
	Backup      BackupSettings `json:"backup"`
	Created     time.Time      `json:"created"`
	Updated     time.Time      `json:"updated"`
}

// MailboxRequest creates or patches a mailbox.
type MailboxRequest struct {
	// State: active, suspended (soft: LMTP 4xx, MTA queues) or disabled
	// (hard: LMTP 5xx); a reason is required for the latter two.
	State       *string      `json:"state,omitempty"`
	StateReason string       `json:"state_reason,omitempty"`
	Quota       *QuotaLimits `json:"quota,omitempty"`
	// Namespace on create (default: the token's own, else "default");
	// StorageID pins the Ceph storage on create (default: weighted among
	// the namespace's storage entries open for placement).
	Namespace string `json:"namespace,omitempty"`
	StorageID int64  `json:"storage_id,omitempty"`
	// Backup settings (interval, retention, disabled).
	Backup *BackupSettingsRequest `json:"backup,omitempty"`
}

// Pools are the data pools of a storage.
type Pools struct {
	Messages    string `json:"messages"`
	Attachments string `json:"attachments"`
	Index       string `json:"index"`
}

// Storage is a registered Ceph storage (the key is never returned).
type Storage struct {
	ID        int64     `json:"id"`
	FSID      string    `json:"fsid"`
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	MonHost   string    `json:"mon_host"`
	User      string    `json:"user"`
	Pools     Pools     `json:"pools"`
	Placement bool      `json:"placement"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
}

// StorageRequest registers or changes a storage; empty fields are left
// unchanged on PATCH.
type StorageRequest struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"` // create only
	MonHost   string `json:"mon_host,omitempty"`
	User      string `json:"user,omitempty"`
	Key       string `json:"key,omitempty"`
	Pools     *Pools `json:"pools,omitempty"`
	Placement *bool  `json:"placement,omitempty"`
}

// Usage is quota usage.
type Usage struct {
	Bytes    int64 `json:"bytes"`
	Messages int64 `json:"messages"`
}

// Folder is a mail folder as reported by mailbox inspection.
type Folder struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Name        string    `json:"name"`
	Role        string    `json:"role,omitempty"`
	UIDValidity uint32    `json:"uidvalidity"`
	UIDNext     uint32    `json:"uidnext"`
	ModSeq      uint64    `json:"modseq"`
	Subscribed  bool      `json:"subscribed"`
	Keywords    []string  `json:"keywords,omitempty"`
	Total       int64     `json:"total"`
	Unseen      int64     `json:"unseen"`
	Size        int64     `json:"size"`
	Created     time.Time `json:"created"`
}

// MailboxInfo is GET /v1/admin/mailboxes/{id}.
type MailboxInfo struct {
	Mailbox  Mailbox     `json:"mailbox"`
	Accounts []string    `json:"accounts"`
	Indexed  bool        `json:"indexed"`
	Usage    Usage       `json:"usage"`
	ModSeq   uint64      `json:"modseq"`
	Folders  []Folder    `json:"folders,omitempty"`
	Hold     *Hold       `json:"hold,omitempty"`
	Limits   QuotaLimits `json:"limits"`
}

// MailboxListItem is one entry of GET /v1/admin/mailboxes.
type MailboxListItem struct {
	ID          string      `json:"id"`
	Namespace   string      `json:"namespace"`
	StorageID   int64       `json:"storage_id"`
	State       string      `json:"state"`
	StateReason string      `json:"state_reason,omitempty"`
	OnHold      bool        `json:"on_hold"`
	Quota       QuotaLimits `json:"quota"`
	Accounts    []string    `json:"accounts"`
	Created     time.Time   `json:"created"`
}

// HoldRequest places a mailbox on hold. AcceptDelivery defaults to true.
type HoldRequest struct {
	Reason         string `json:"reason"`
	CaseRef        string `json:"case_ref,omitempty"`
	AcceptDelivery *bool  `json:"accept_delivery,omitempty"`
	AllowSend      bool   `json:"allow_send,omitempty"`
	Sealed         bool   `json:"sealed,omitempty"`
}

// Password is a redacted password entry (no hash).
type Password struct {
	ID        string     `json:"id"`
	Kind      string     `json:"kind"`
	Scheme    string     `json:"scheme"`
	Comment   string     `json:"comment"`
	ExpiresAt *time.Time `json:"expires_at"` // nil = never
	Created   time.Time  `json:"created"`
}

// PasswordUpdate changes expiry (nil = never; the field is always sent)
// and, when set, the comment.
type PasswordUpdate struct {
	ExpiresAt *time.Time `json:"expires_at"`
	Comment   string     `json:"comment,omitempty"`
}

// Account is the redacted account view.
type Account struct {
	ID          string     `json:"id"`
	Username    string     `json:"username"`
	Name        string     `json:"name"`
	Domain      string     `json:"domain"`
	Namespace   string     `json:"namespace"`
	MailboxID   string     `json:"mailbox_id"`
	Roles       []string   `json:"roles"`
	Services    []string   `json:"services"`
	Enabled     bool       `json:"enabled"`
	TOTPEnabled bool       `json:"totp_enabled"`
	Passwords   []Password `json:"passwords"`
	Created     time.Time  `json:"created"`
	Updated     time.Time  `json:"updated"`
}

// AccountRequest creates (Username, MailboxID, Roles, Password, Enabled) or
// patches (Roles, Enabled, MailboxID) an account.
type AccountRequest struct {
	Username          string     `json:"username,omitempty"`
	Name              *string    `json:"name,omitempty"`
	MailboxID         string     `json:"mailbox_id,omitempty"`
	Roles             []string   `json:"roles,omitempty"`
	Services          []string   `json:"services,omitempty"`
	Password          string     `json:"password,omitempty"`
	PasswordComment   string     `json:"password_comment,omitempty"`
	PasswordExpiresAt *time.Time `json:"password_expires_at,omitempty"`
	Enabled           *bool      `json:"enabled,omitempty"`
}

// PasswordRequest sets a password. Kind is "primary" (default) or "application".
type PasswordRequest struct {
	Password string `json:"password,omitempty"`
	// Hash takes an existing hash as another platform stores it (exactly one
	// of Password and Hash; primary only).
	Hash      string     `json:"hash,omitempty"`
	Kind      string     `json:"kind,omitempty"`
	Comment   string     `json:"comment,omitempty"`    // required
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // nil = never
}

// PasswordSet is the response of setting a password.
type PasswordSet struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// Session is a login session.
type Session struct {
	ID        string    `json:"id"`
	Client    string    `json:"client"`
	Roles     []string  `json:"roles"`
	Created   time.Time `json:"created"`
	LastUsed  time.Time `json:"last_used"`
	LastIP    string    `json:"last_ip,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Token is an API token (never the secret, except in TokenCreated).
type Token struct {
	ID        string    `json:"id"`
	AccountID string    `json:"account_id"`
	Namespace string    `json:"namespace,omitempty"` // admin tokens: bound namespace, empty = platform
	Roles     []string  `json:"roles"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
	Created   time.Time `json:"created"`
	LastUsed  time.Time `json:"last_used,omitempty"`
}

// TokenRequest creates a token. Comment and ExpiresAt are mandatory.
type TokenRequest struct {
	Roles     []string  `json:"roles,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
}

// AdminTokenRequest mints an admin token. ReadOnly selects role admin-ro.
type AdminTokenRequest struct {
	ReadOnly  bool      `json:"read_only,omitempty"`
	Namespace string    `json:"namespace,omitempty"` // bind to one namespace (a tenant admin)
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
}

// TokenCreated carries the one-time secret.
type TokenCreated struct {
	Token  Token  `json:"token"`
	Secret string `json:"secret"`
}

// Destination is one alias target: a mailbox id or another address.
type Destination struct {
	MailboxID string `json:"mailbox_id,omitempty"`
	Address   string `json:"address,omitempty"`
}

// Alias maps an address (or "@domain" catch-all) to destinations.
type Alias struct {
	Address      string        `json:"address"`
	Domain       string        `json:"domain"`
	Namespace    string        `json:"namespace"`
	Destinations []Destination `json:"destinations"`
	Enabled      bool          `json:"enabled"`
	Created      time.Time     `json:"created"`
	Updated      time.Time     `json:"updated"`
}

// AliasRequest creates or patches an alias.
type AliasRequest struct {
	Address      string        `json:"address,omitempty"`
	Enabled      *bool         `json:"enabled,omitempty"`
	Destinations []Destination `json:"destinations,omitempty"`
}

// AuditRecord is one audit entry.
type AuditRecord struct {
	Time      time.Time `json:"time"`
	Mailbox   string    `json:"mailbox,omitempty"`
	Account   string    `json:"account,omitempty"`
	AccountID string    `json:"account_id,omitempty"`
	TokenID   string    `json:"token_id,omitempty"`
	TokenNote string    `json:"token_comment,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	Protocol  string    `json:"protocol"`
	ClientIP  string    `json:"client_ip,omitempty"`
	Action    string    `json:"action"`
	Target    string    `json:"target,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	Result    string    `json:"result"`
	Admin     string    `json:"admin,omitempty"`
}

// AuditQuery filters GET /v1/admin/audit. Zero values are omitted.
type AuditQuery struct {
	Mailbox  string
	Account  string
	Action   string
	ClientIP string
	From, To time.Time // dates; only the day is used
	After    string    // cursor from a previous page
	Limit    int
}

// AuditPage is a page of audit records.
type AuditPage struct {
	Items []AuditRecord `json:"items"`
	Next  string        `json:"next"`
}

// QueueStats counts tasks per state.
type QueueStats struct {
	Type    string `json:"type"`
	Pending int    `json:"pending"`
	Leased  int    `json:"leased"`
	Dead    int    `json:"dead"`
}

// Task is a background task.
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	RunAfter  time.Time       `json:"run_after"`
	Attempts  int             `json:"attempts"`
	LastError string          `json:"last_error,omitempty"`
	Created   time.Time       `json:"created"`
}

// BackupSettings are a mailbox's explicit backup preferences; empty
// fields inherit (domain settings, then the configured defaults).
type BackupSettings struct {
	Interval  string `json:"interval,omitempty"`  // duration, e.g. "24h"
	Retention string `json:"retention,omitempty"` // duration, e.g. "2160h"
	Disabled  bool   `json:"disabled,omitempty"`
}

// BackupSettingsRequest patches backup settings; nil fields are left
// alone, empty strings clear a field.
type BackupSettingsRequest struct {
	Interval  *string `json:"interval,omitempty"`
	Retention *string `json:"retention,omitempty"`
	Disabled  *bool   `json:"disabled,omitempty"`
}

// BackupState is the schedule state of a mailbox.
type BackupState struct {
	Mailbox     string    `json:"mailbox"`
	Every       int64     `json:"every"` // nanoseconds; 0 = disabled
	Scheduled   bool      `json:"scheduled"`
	Due         time.Time `json:"due,omitempty"`
	MaintainDue time.Time `json:"maintain_due,omitempty"`
	LastRun     string    `json:"last_run,omitempty"`
	LastModSeq  uint64    `json:"last_modseq"`
	LastAt      time.Time `json:"last_at,omitempty"`
	Status      string    `json:"status,omitempty"`
	Error       string    `json:"error,omitempty"`
	ForceFull   bool      `json:"force_full,omitempty"`
	Verified    time.Time `json:"verified,omitempty"`
	VerifyError string    `json:"verify_error,omitempty"`
}

// BackupRun is one completed backup run.
type BackupRun struct {
	ID          string    `json:"id"`
	Mailbox     string    `json:"mailbox"`
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
	Expires     time.Time `json:"expires,omitempty"`
}

// BackupPolicy is the effective policy of a mailbox.
type BackupPolicy struct {
	Interval  string `json:"interval"`
	Retention string `json:"retention"`
	Enabled   bool   `json:"enabled"`
}

// MailboxBackups is GET /v1/admin/mailboxes/{id}/backups.
type MailboxBackups struct {
	State    BackupState    `json:"state"`
	Settings BackupSettings `json:"settings"`
	Policy   BackupPolicy   `json:"policy"`
	Runs     []BackupRun    `json:"runs"`
}

// BackupJob is a restore, export, verify or forget job.
type BackupJob struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	Mailbox   string          `json:"mailbox"`
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
	Finished  time.Time       `json:"finished,omitempty"`
	Expires   time.Time       `json:"expires,omitempty"`
}

// BackupSummary is the platform-wide picture.
type BackupSummary struct {
	Mailboxes    int       `json:"mailboxes"`
	Scheduled    int       `json:"scheduled"`
	Disabled     int       `json:"disabled"`
	Overdue      int       `json:"overdue"`
	Failed       int       `json:"failed"`
	VerifyFailed int       `json:"verify_failed"`
	OldestDue    time.Time `json:"oldest_due,omitempty"`
	Rate         float64   `json:"rate_per_hour"`
	Runs         int64     `json:"runs"`
	Bytes        int64     `json:"bytes"`
	Blobs        int64     `json:"blobs"`
	Jobs         int       `json:"jobs_pending"`
}

// BackupStatus is GET /v1/admin/backups.
type BackupStatus struct {
	Summary BackupSummary     `json:"summary"`
	Policy  map[string]string `json:"policy"`
}

// BackupScheduled is the response of a run-now request.
type BackupScheduled struct {
	TaskID  string `json:"task_id"`
	Mailbox string `json:"mailbox"`
	Full    bool   `json:"full"`
}

// RecalculateResult is the response of a quota recalculation.
// PurgeResult is what a purge request scheduled.
type PurgeResult struct {
	Retained int    `json:"retained"`
	TaskID   string `json:"task_id"`
}

type RecalculateResult struct {
	Usage Usage `json:"usage"`
}

// Node is a registered daemon mode on some host.
type Node struct {
	ID         int64     `json:"id"`
	Mode       string    `json:"mode"`
	Address    string    `json:"address"`
	Host       string    `json:"host"`
	Version    string    `json:"version"`
	Namespaces []string  `json:"namespaces"` // served namespaces; empty = all
	Started    time.Time `json:"started"`
	LastSeen   time.Time `json:"last_seen"`
	Alive      bool      `json:"alive"`
}
