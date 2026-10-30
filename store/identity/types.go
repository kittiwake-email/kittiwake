// Package identity persists domains, mailboxes, accounts, aliases and API
// tokens in the RADOS meta pool. See README.md "Object naming" and "Identity
// store semantics" for the layout.
package identity

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors.
var (
	ErrNotFound      = errors.New("identity: not found")
	ErrExists        = errors.New("identity: already exists")
	ErrConflict      = errors.New("identity: concurrent modification, retry")
	ErrInvalid       = errors.New("identity: invalid")
	ErrDomainUnknown = errors.New("identity: domain unknown")
	ErrInUse         = errors.New("identity: still in use")
	// ErrNamespaceUnknown: the named namespace does not exist.
	ErrNamespaceUnknown = errors.New("identity: namespace unknown")
	// ErrNamespaceMismatch: the operation would link records of different
	// namespaces (an account to a mailbox elsewhere, a mailbox to another
	// namespace's storage). Data never crosses a namespace.
	ErrNamespaceMismatch = errors.New("identity: namespace mismatch")
)

// DefaultNamespace is the namespace that always exists: every domain,
// mailbox and storage created without one lands here.
const DefaultNamespace = "default"

// Namespace is the unit of tenant isolation. It owns storage (one or more
// Ceph pool sets with their own credentials), domains and, through them,
// mailboxes and accounts. Mail data never crosses a namespace: a mailbox is
// placed on its namespace's storage and only accounts of the namespace's
// domains may use it. Daemons serve every namespace unless their config
// restricts them, and admin tokens may be bound to one namespace.
// Settings hold the same keys domains do and sit below them in the policy
// chain (mailbox, domain, namespace, configured defaults).
type Namespace struct {
	Name     string            `json:"name"`
	Enabled  bool              `json:"enabled"`
	Settings map[string]string `json:"settings,omitempty"`
	// Ingress is where proxies reach this namespace's servers when they
	// are not reachable directly: one hostname (an HAProxy terminating
	// TLS in the namespace's datacenter) and a TLS policy. Nil: proxies
	// pick the namespace's nodes from the node registry.
	Ingress *Ingress  `json:"ingress,omitempty"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
}

// Ingress TLS policies: how a proxy connects to an ingress hostname.
const (
	// IngressTLSStrict: TLS, certificate verified against the system roots
	// and the hostname.
	IngressTLSStrict = "strict"
	// IngressTLSInsecure: TLS, any certificate accepted (a lab, a private
	// CA not yet rolled out).
	IngressTLSInsecure = "insecure"
	// IngressTLSOff: plain TCP (a private network that is trusted).
	IngressTLSOff = "off"
)

// Ingress is a namespace's entry point for proxies. TLS is one of the
// IngressTLS policies, or empty for the proxy's configured default.
type Ingress struct {
	Host string `json:"host"`
	TLS  string `json:"tls,omitempty"`
}

// NormalizeIngress lowercases and validates an ingress: the host is a
// DNS name without port or scheme, the policy one of the known ones or
// empty. A nil ingress or one with an empty host means none.
func NormalizeIngress(in *Ingress) (*Ingress, error) {
	if in == nil {
		return nil, nil
	}
	host := strings.TrimSpace(strings.ToLower(in.Host))
	if host == "" {
		return nil, nil
	}
	if len(host) > 253 || strings.ContainsAny(host, " :/\\@\t\r\n\x00") || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return nil, fmt.Errorf("%w: ingress host %q", ErrInvalid, in.Host)
	}
	tls := strings.TrimSpace(strings.ToLower(in.TLS))
	switch tls {
	case "", IngressTLSStrict, IngressTLSInsecure, IngressTLSOff:
	default:
		return nil, fmt.Errorf("%w: ingress tls %q must be strict, insecure or off", ErrInvalid, in.TLS)
	}
	return &Ingress{Host: host, TLS: tls}, nil
}

// Account roles.
const (
	RoleRead  = "read"
	RoleWrite = "write"
	RoleSend  = "send"
)

// Admin token roles. Admins are not accounts: an admin is an API token with
// a comment, an expiry and one of these roles.
const (
	RoleAdmin     = "admin"    // full administration
	RoleAdminRead = "admin-ro" // read-only: queries, inspection, audit
)

// ValidRoles is the set of roles an account may carry.
var ValidRoles = map[string]bool{RoleRead: true, RoleWrite: true, RoleSend: true}

// AdminRoles is the set of roles an admin token may carry (exactly one).
var AdminRoles = map[string]bool{RoleAdmin: true, RoleAdminRead: true}

// Services an account may use. The set grows (CalDAV, CardDAV); an account
// lists the ones it is allowed to log in to, so a phone-only user can be
// restricted to JMAP.
const (
	ServiceIMAP = "imap"
	ServicePOP3 = "pop3"
	ServiceJMAP = "jmap" // the JMAP listener, including its REST self-service endpoints
)

// KnownServices is the set of services an account may list.
var KnownServices = map[string]bool{ServiceIMAP: true, ServicePOP3: true, ServiceJMAP: true}

// AllServices returns every known service, in a stable order.
func AllServices() []string { return []string{ServiceIMAP, ServicePOP3, ServiceJMAP} }

// ServiceFor maps a protocol name (as used in audit records) to the
// service an account must have to use it. Unknown protocols map to "".
func ServiceFor(proto string) string {
	switch proto {
	case "imap":
		return ServiceIMAP
	case "pop3":
		return ServicePOP3
	case "jmap", "rest":
		return ServiceJMAP
	}
	return ""
}

// Password kinds.
const (
	PasswordPrimary     = "primary"
	PasswordApplication = "application"
)

// Domain is a mail domain. It belongs to a namespace, fixed at creation.
type Domain struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Enabled   bool              `json:"enabled"`
	Settings  map[string]string `json:"settings,omitempty"`
	Created   time.Time         `json:"created"`
	Updated   time.Time         `json:"updated"`
}

// QuotaLimits are per-mailbox limits; zero means "use the configured default".
type QuotaLimits struct {
	SoftBytes    int64 `json:"soft_bytes"`
	HardBytes    int64 `json:"hard_bytes"`
	SoftMessages int64 `json:"soft_messages"`
	HardMessages int64 `json:"hard_messages"`
}

// Hold is a legal hold record on a mailbox.
type Hold struct {
	Reason         string    `json:"reason"`
	CaseRef        string    `json:"case_ref,omitempty"`
	SetBy          string    `json:"set_by"`
	SetAt          time.Time `json:"set_at"`
	AcceptDelivery bool      `json:"accept_delivery"`
	AllowSend      bool      `json:"allow_send"`
	Sealed         bool      `json:"sealed"`
}

// Mailbox states.
const (
	// StateActive: delivery and user access work.
	StateActive = "active"
	// StateSuspended: soft. LMTP answers 4xx so the MTA queues and retries;
	// users are locked out. For payment holds, migrations, maintenance.
	StateSuspended = "suspended"
	// StateDisabled: hard. LMTP answers 5xx (and the Postfix map says the
	// recipient is unknown, so mail is rejected at SMTP time); users are
	// locked out.
	StateDisabled = "disabled"
)

// ValidStates is the set of mailbox states.
var ValidStates = map[string]bool{StateActive: true, StateSuspended: true, StateDisabled: true}

// BackupSettings are a mailbox's explicit backup preferences. Zero values
// inherit: the domain's `backup.interval` / `backup.retention` settings
// for mailboxes with accounts, else the configured defaults. Disabled
// switches backups off for the mailbox explicitly.
type BackupSettings struct {
	Interval  time.Duration `json:"interval,omitempty"`
	Retention time.Duration `json:"retention,omitempty"`
	Disabled  bool          `json:"disabled,omitempty"`
}

type backupSettingsJSON struct {
	Interval  string `json:"interval,omitempty"`
	Retention string `json:"retention,omitempty"`
	Disabled  bool   `json:"disabled,omitempty"`
}

// MarshalJSON writes durations as strings ("24h0m0s"), readable in the API
// and in the RADOS identity records alike.
func (b BackupSettings) MarshalJSON() ([]byte, error) {
	out := backupSettingsJSON{Disabled: b.Disabled}
	if b.Interval > 0 {
		out.Interval = b.Interval.String()
	}
	if b.Retention > 0 {
		out.Retention = b.Retention.String()
	}
	return json.Marshal(out)
}

// UnmarshalJSON accepts duration strings.
func (b *BackupSettings) UnmarshalJSON(data []byte) error {
	var in backupSettingsJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*b = BackupSettings{Disabled: in.Disabled}
	var err error
	if in.Interval != "" {
		if b.Interval, err = time.ParseDuration(in.Interval); err != nil {
			return err
		}
	}
	if in.Retention != "" {
		if b.Retention, err = time.ParseDuration(in.Retention); err != nil {
			return err
		}
	}
	return nil
}

// Domain settings keys consulted by the backup policy.
const (
	DomainBackupInterval  = "backup.interval"
	DomainBackupRetention = "backup.retention"
)

// Mailbox is the container for a user's data. It belongs to a namespace,
// fixed at creation, and is placed on one of that namespace's storages.
type Mailbox struct {
	ID        uuid.UUID `json:"id"`
	Namespace string    `json:"namespace"`
	// StorageID is the Ceph storage (registry id) holding the mailbox's
	// data; 0 only in the single-storage memory backend.
	StorageID   int64          `json:"storage_id"`
	State       string         `json:"state"` // active | suspended | disabled
	StateReason string         `json:"state_reason,omitempty"`
	StateSince  time.Time      `json:"state_since"`
	Quota       QuotaLimits    `json:"quota"`
	Hold        *Hold          `json:"hold,omitempty"`
	Released    *time.Time     `json:"released,omitempty"` // when the last hold was released
	Backup      BackupSettings `json:"backup"`
	Created     time.Time      `json:"created"`
	Updated     time.Time      `json:"updated"`
}

// Active reports whether the mailbox is in the active state.
func (m *Mailbox) Active() bool { return m.State == StateActive }

// Password is one credential of an account. Every password carries a
// comment (what it is for) and an optional expiry, so temporary passwords
// can be handed out and stop working on their own.
type Password struct {
	ID        string     `json:"id"`
	Hash      string     `json:"hash"` // scheme-prefixed, e.g. {ARGON2ID}$argon2id$...
	Kind      string     `json:"kind"` // primary or application
	Comment   string     `json:"comment"`
	ExpiresAt *time.Time `json:"expires_at"` // nil = never
	Created   time.Time  `json:"created"`
}

// Expired reports whether the password has expired at t.
func (p *Password) Expired(at time.Time) bool { return p.ExpiresAt != nil && !at.Before(*p.ExpiresAt) }

// TOTP is an account's second-factor configuration.
type TOTP struct {
	Secret    string   `json:"secret"` // base32
	Digits    int      `json:"digits"`
	Period    int      `json:"period"`
	Algorithm string   `json:"algorithm"` // SHA1, SHA256, SHA512
	Recovery  []string `json:"recovery,omitempty"`
	Confirmed bool     `json:"confirmed"`
}

// Account is a set of credentials linked to a mailbox (or an admin).
// Namespace is the domain's, read-only: the backends fill it.
type Account struct {
	ID        uuid.UUID  `json:"id"`
	Username  string     `json:"username"`       // lowercase, usually user@domain
	Name      string     `json:"name,omitempty"` // display name, used as the From: name of every identity
	Domain    string     `json:"domain"`
	Namespace string     `json:"namespace"`
	MailboxID uuid.UUID  `json:"mailbox_id"`
	Roles     []string   `json:"roles"`
	Services  []string   `json:"services"` // which of KnownServices the account may log in to
	Passwords []Password `json:"passwords"`
	TOTP      *TOTP      `json:"totp,omitempty"`
	Enabled   bool       `json:"enabled"`
	Created   time.Time  `json:"created"`
	Updated   time.Time  `json:"updated"`
}

// HasService reports whether the account may use the service.
func (a *Account) HasService(svc string) bool {
	for _, x := range a.Services {
		if x == svc {
			return true
		}
	}
	return false
}

// HasRole reports whether the account has the role.
func (a *Account) HasRole(r string) bool {
	for _, x := range a.Roles {
		if x == r {
			return true
		}
	}
	return false
}

// Destination is one target of an alias: a mailbox or another address.
type Destination struct {
	MailboxID uuid.UUID `json:"mailbox_id,omitempty"`
	Address   string    `json:"address,omitempty"`
}

// Alias maps an address (or "@domain" for catch-all) to destinations.
// Namespace is the domain's, read-only; mailbox destinations must be in it.
type Alias struct {
	Address      string        `json:"address"` // lowercase
	Domain       string        `json:"domain"`
	Namespace    string        `json:"namespace"`
	Destinations []Destination `json:"destinations"`
	Enabled      bool          `json:"enabled"`
	Created      time.Time     `json:"created"`
	Updated      time.Time     `json:"updated"`
}

// IsCatchAll reports whether the alias is a domain catch-all.
func (a *Alias) IsCatchAll() bool { return len(a.Address) > 0 && a.Address[0] == '@' }

// Token is a long-lived API token. The object name is the SHA-256 of the token.
// An admin token has a zero AccountID and an admin role; Namespace binds it
// to one namespace (a tenant admin), empty means the whole platform.
type Token struct {
	ID        string    `json:"id"`
	Hash      string    `json:"hash"`
	AccountID uuid.UUID `json:"account_id"`
	Namespace string    `json:"namespace,omitempty"`
	Roles     []string  `json:"roles"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
	Created   time.Time `json:"created"`
	LastUsed  time.Time `json:"last_used,omitempty"`
}

// Expired reports whether the token has expired at t.
func (t *Token) Expired(at time.Time) bool { return !at.Before(t.ExpiresAt) }

// IsAdmin reports whether this is an admin token (not bound to an account).
func (t *Token) IsAdmin() bool { return t.AccountID == uuid.Nil }

// Storage is one Ceph pool set in the registry: a cluster, credentials and
// three pools, owned by exactly one namespace. Mailboxes reference it by
// ID; the fsid (read from the storage on connect) verifies that the
// connection details still point at the same cluster. Several storages
// may share a cluster (one per namespace, with their own pools and keys),
// never a pool. Key is the Ceph secret of User.
type Storage struct {
	ID        int64     `json:"id"`
	FSID      uuid.UUID `json:"fsid"`
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	MonHost   string    `json:"mon_host"` // comma-separated, as ceph.conf mon_host
	User      string    `json:"user"`
	Key       string    `json:"key"`
	Pools     Pools     `json:"pools"`
	Placement bool      `json:"placement"` // accepts new mailboxes
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
}

// Pools are the data pools of a storage.
type Pools struct {
	Messages    string `json:"messages"`
	Attachments string `json:"attachments"`
	Index       string `json:"index"`
}

// Node is one running daemon mode, as registered by the process itself:
// every process registers each mode it runs at start and refreshes
// LastSeen every heartbeat, so other daemons (and operators) can find it.
// Address is where the mode is reached: the advertised host joined with
// the mode's listener port. A node whose Expires has passed is gone.
type Node struct {
	ID      int64  `json:"id"`
	Mode    string `json:"mode"`
	Address string `json:"address"`
	Host    string `json:"host"` // the machine's hostname
	Version string `json:"version"`
	// Namespaces the process serves; empty means all of them.
	Namespaces []string  `json:"namespaces,omitempty"`
	Started    time.Time `json:"started"`
	LastSeen   time.Time `json:"last_seen"`
	Expires    time.Time `json:"expires"` // LastSeen + 3 heartbeats
}

// Alive reports whether the node's heartbeat is current at t.
func (n *Node) Alive(t time.Time) bool { return t.Before(n.Expires) }

// Serves reports whether the node serves the namespace.
func (n *Node) Serves(namespace string) bool { return ServesNamespace(n.Namespaces, namespace) }

// ServesNamespace reports whether a served list (empty = all) covers the
// namespace.
func ServesNamespace(served []string, namespace string) bool {
	if len(served) == 0 {
		return true
	}
	if namespace == "" {
		namespace = DefaultNamespace
	}
	for _, s := range served {
		if s == namespace {
			return true
		}
	}
	return false
}

// Versioned pairs an object with the version it was read at (RADOS object
// version; 0 on backends without one).
type Versioned[T any] struct {
	Value   T
	Version uint64
}
