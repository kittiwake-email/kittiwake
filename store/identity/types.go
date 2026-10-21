// Package identity persists domains, mailboxes, accounts, aliases and API
// tokens in the RADOS meta pool. See README.md "Object naming" and "Identity
// store semantics" for the layout.
package identity

import (
	"errors"
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
)

// Roles.
const (
	RoleRead  = "read"
	RoleWrite = "write"
	RoleSend  = "send"
	RoleAdmin = "admin"
)

// ValidRoles is the set of known roles.
var ValidRoles = map[string]bool{RoleRead: true, RoleWrite: true, RoleSend: true, RoleAdmin: true}

// Password kinds.
const (
	PasswordPrimary     = "primary"
	PasswordApplication = "application"
)

// Domain is a mail domain.
type Domain struct {
	Name     string            `json:"name"`
	Enabled  bool              `json:"enabled"`
	Settings map[string]string `json:"settings,omitempty"`
	Created  time.Time         `json:"created"`
	Updated  time.Time         `json:"updated"`
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

// Mailbox is the container for a user's data.
type Mailbox struct {
	ID       uuid.UUID   `json:"id"`
	Enabled  bool        `json:"enabled"`
	Quota    QuotaLimits `json:"quota"`
	Hold     *Hold       `json:"hold,omitempty"`
	Released *time.Time  `json:"released,omitempty"` // when the last hold was released
	Created  time.Time   `json:"created"`
	Updated  time.Time   `json:"updated"`
}

// Password is one credential of an account.
type Password struct {
	ID      string    `json:"id"`
	Hash    string    `json:"hash"` // scheme-prefixed, e.g. {ARGON2ID}$argon2id$...
	Kind    string    `json:"kind"` // primary or application
	Comment string    `json:"comment,omitempty"`
	Created time.Time `json:"created"`
}

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
type Account struct {
	ID        uuid.UUID  `json:"id"`
	Username  string     `json:"username"` // lowercase, usually user@domain
	Domain    string     `json:"domain"`
	MailboxID uuid.UUID  `json:"mailbox_id"` // zero for admin accounts
	Roles     []string   `json:"roles"`
	Passwords []Password `json:"passwords"`
	TOTP      *TOTP      `json:"totp,omitempty"`
	Enabled   bool       `json:"enabled"`
	Created   time.Time  `json:"created"`
	Updated   time.Time  `json:"updated"`
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

// IsAdmin reports whether the account carries the admin role.
func (a *Account) IsAdmin() bool { return a.HasRole(RoleAdmin) }

// Destination is one target of an alias: a mailbox or another address.
type Destination struct {
	MailboxID uuid.UUID `json:"mailbox_id,omitempty"`
	Address   string    `json:"address,omitempty"`
}

// Alias maps an address (or "@domain" for catch-all) to destinations.
type Alias struct {
	Address      string        `json:"address"` // lowercase
	Domain       string        `json:"domain"`
	Destinations []Destination `json:"destinations"`
	Enabled      bool          `json:"enabled"`
	Created      time.Time     `json:"created"`
	Updated      time.Time     `json:"updated"`
}

// IsCatchAll reports whether the alias is a domain catch-all.
func (a *Alias) IsCatchAll() bool { return len(a.Address) > 0 && a.Address[0] == '@' }

// Token is a long-lived API token. The object name is the SHA-256 of the token.
type Token struct {
	ID        string    `json:"id"`
	Hash      string    `json:"hash"`
	AccountID uuid.UUID `json:"account_id"`
	Roles     []string  `json:"roles"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
	Created   time.Time `json:"created"`
	LastUsed  time.Time `json:"last_used,omitempty"`
}

// Expired reports whether the token has expired at t.
func (t *Token) Expired(at time.Time) bool { return !at.Before(t.ExpiresAt) }

// Versioned pairs an object with the RADOS version it was read at, for
// optimistic concurrency.
type Versioned[T any] struct {
	Value   T
	Version uint64
}
