// Package identity persists domains, mailboxes, accounts, aliases, API
// tokens and the Ceph storage registry: the metadata of the platform. It
// is small, read-heavy and global, as opposed to mail data, which lives
// per mailbox on a Ceph storage. Store is the API the services use; it
// adds the per-process read cache and delegates to a Backend, of which
// there are two: MariaDB (production, package store/mariadb) and RADOS
// (development and tests, in this package).
package identity

import (
	"context"
	"time"

	"github.com/google/uuid"

	"kittiwake/internal/ttlcache"
	"kittiwake/rados"
)

// Backend is what a metadata database must implement. Every method is
// safe for concurrent use; updates take a mutator and are atomic.
type Backend interface {
	SetNow(func() time.Time)
	CreateDomain(ctx context.Context, d Domain) (Domain, error)
	GetDomain(ctx context.Context, name string) (Versioned[Domain], error)
	UpdateDomain(ctx context.Context, name string, fn func(*Domain) error) (Domain, error)
	DeleteDomain(ctx context.Context, name string) error
	ListDomains(ctx context.Context, after string, max int) (names []string, more bool, err error)
	CreateMailbox(ctx context.Context, m Mailbox) (Mailbox, error)
	GetMailbox(ctx context.Context, id uuid.UUID) (Versioned[Mailbox], error)
	UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*Mailbox) error) (Mailbox, error)
	DeleteMailbox(ctx context.Context, id uuid.UUID) error
	ListMailboxes(ctx context.Context, fn func(id uuid.UUID) error) error
	ListMailboxAccounts(ctx context.Context, id uuid.UUID, after string, max int) (usernames []string, more bool, err error)
	CreateAccount(ctx context.Context, a Account) (Account, error)
	GetAccount(ctx context.Context, id uuid.UUID) (Versioned[Account], error)
	GetAccountByUsername(ctx context.Context, username string) (Versioned[Account], error)
	UpdateAccount(ctx context.Context, id uuid.UUID, fn func(*Account) error) (Account, error)
	RelinkAccount(ctx context.Context, id, mailboxID uuid.UUID) (Account, error)
	DeleteAccount(ctx context.Context, id uuid.UUID) error
	ListDomainAccounts(ctx context.Context, domain, prefix, after string, max int) (usernames []string, more bool, err error)
	CreateAlias(ctx context.Context, al Alias) (Alias, error)
	ListMailboxAliases(ctx context.Context, id uuid.UUID) ([]string, error)
	// ListAliasesTo lists the aliases that have address (normalized) as a
	// destination address: the reverse of address destinations, for
	// sender identities. Defined for addresses in hosted domains; for any
	// other address a backend may return nothing.
	ListAliasesTo(ctx context.Context, address string) ([]string, error)
	GetAlias(ctx context.Context, address string) (Versioned[Alias], error)
	UpdateAlias(ctx context.Context, address string, fn func(*Alias) error) (Alias, error)
	DeleteAlias(ctx context.Context, address string) error
	ListDomainAliases(ctx context.Context, domain, after string, max int) (addresses []string, more bool, err error)
	CreateToken(ctx context.Context, accountID uuid.UUID, roles []string, expiresAt time.Time, comment string) (Token, string, error)
	CreateAdminToken(ctx context.Context, role string, expiresAt time.Time, comment string) (Token, string, error)
	ListAdminTokens(ctx context.Context) ([]Token, error)
	DeleteAdminToken(ctx context.Context, tokenID string) error
	LookupToken(ctx context.Context, secret string) (Versioned[Token], error)
	TouchToken(ctx context.Context, t Versioned[Token])
	ListTokens(ctx context.Context, accountID uuid.UUID) ([]Token, error)
	DeleteToken(ctx context.Context, accountID uuid.UUID, tokenID string) error
	CreateStorage(ctx context.Context, c Storage) (Storage, error)
	GetStorage(ctx context.Context, id int64) (Storage, error)
	ListStorage(ctx context.Context) ([]Storage, error)
	UpdateStorage(ctx context.Context, id int64, fn func(*Storage) error) (Storage, error)
	DeleteStorage(ctx context.Context, id int64) error

	// RegisterNode inserts or refreshes the (mode, address) row.
	RegisterNode(ctx context.Context, n Node) (Node, error)
	// ListNodes lists nodes of one mode ("" = all), alive or not; callers
	// filter with Alive.
	ListNodes(ctx context.Context, mode string) ([]Node, error)
	// DeleteNode removes the (mode, address) row (orderly shutdown).
	DeleteNode(ctx context.Context, mode, address string) error
	// PurgeNodes removes rows that expired before cutoff; returns the count.
	PurgeNodes(ctx context.Context, cutoff time.Time) (int, error)
}

// Store is the identity store the services use.
type Store struct {
	b   Backend
	Now func() time.Time
	// CacheTTL enables the *Cached getters (see cache.go). 0 disables.
	CacheTTL time.Duration
	cache    ttlcache.Cache[any]
}

// New returns a store on the RADOS backend (development and tests).
func New(meta rados.IOContext) *Store { return NewWith(NewRados(meta)) }

// NewWith returns a store on the given backend.
func NewWith(b Backend) *Store {
	s := &Store{b: b, Now: time.Now}
	b.SetNow(func() time.Time { return s.Now() })
	return s
}

// Backend returns the underlying backend (tests, migrations).
func (s *Store) Backend() Backend { return s.b }

// ---- writes: delegate, then drop the cached copy ----

func (s *Store) CreateDomain(ctx context.Context, d Domain) (Domain, error) {
	out, err := s.b.CreateDomain(ctx, d)
	if err == nil {
		s.cache.Delete(domainKey(out.Name))
	}
	return out, err
}

func (s *Store) UpdateDomain(ctx context.Context, name string, fn func(*Domain) error) (Domain, error) {
	out, err := s.b.UpdateDomain(ctx, name, fn)
	if err == nil {
		s.cache.Delete(domainKey(out.Name))
	}
	return out, err
}

func (s *Store) DeleteDomain(ctx context.Context, name string) error {
	err := s.b.DeleteDomain(ctx, name)
	if n, nerr := NormalizeDomain(name); nerr == nil {
		s.cache.Delete(domainKey(n))
	}
	return err
}

func (s *Store) CreateMailbox(ctx context.Context, m Mailbox) (Mailbox, error) {
	out, err := s.b.CreateMailbox(ctx, m)
	if err == nil {
		s.cache.Delete(mailboxKey(out.ID))
	}
	return out, err
}

func (s *Store) UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*Mailbox) error) (Mailbox, error) {
	out, err := s.b.UpdateMailbox(ctx, id, fn)
	s.cache.Delete(mailboxKey(id))
	return out, err
}

func (s *Store) DeleteMailbox(ctx context.Context, id uuid.UUID) error {
	err := s.b.DeleteMailbox(ctx, id)
	s.cache.Delete(mailboxKey(id))
	return err
}

func (s *Store) CreateAccount(ctx context.Context, a Account) (Account, error) {
	out, err := s.b.CreateAccount(ctx, a)
	if err == nil {
		s.cache.Delete(accountKey(out.Username))
	}
	return out, err
}

func (s *Store) UpdateAccount(ctx context.Context, id uuid.UUID, fn func(*Account) error) (Account, error) {
	out, err := s.b.UpdateAccount(ctx, id, fn)
	if err == nil {
		s.cache.Delete(accountKey(out.Username))
	}
	return out, err
}

func (s *Store) RelinkAccount(ctx context.Context, id, mailboxID uuid.UUID) (Account, error) {
	out, err := s.b.RelinkAccount(ctx, id, mailboxID)
	if err == nil {
		s.cache.Delete(accountKey(out.Username))
	}
	return out, err
}

func (s *Store) DeleteAccount(ctx context.Context, id uuid.UUID) error {
	// The username is needed for the cache key; a miss costs one read.
	if v, err := s.b.GetAccount(ctx, id); err == nil {
		defer s.cache.Delete(accountKey(v.Value.Username))
	}
	return s.b.DeleteAccount(ctx, id)
}

func (s *Store) CreateAlias(ctx context.Context, al Alias) (Alias, error) {
	out, err := s.b.CreateAlias(ctx, al)
	if err == nil {
		s.cache.Delete(aliasKey(out.Address))
	}
	return out, err
}

func (s *Store) UpdateAlias(ctx context.Context, address string, fn func(*Alias) error) (Alias, error) {
	out, err := s.b.UpdateAlias(ctx, address, fn)
	if err == nil {
		s.cache.Delete(aliasKey(out.Address))
	}
	return out, err
}

func (s *Store) DeleteAlias(ctx context.Context, address string) error {
	err := s.b.DeleteAlias(ctx, address)
	if a, _, nerr := normalizeAliasAddress(address); nerr == nil {
		s.cache.Delete(aliasKey(a))
	}
	return err
}

// ---- reads and the rest: plain delegation ----

func (s *Store) GetDomain(ctx context.Context, name string) (Versioned[Domain], error) {
	return s.b.GetDomain(ctx, name)
}
func (s *Store) ListDomains(ctx context.Context, after string, max int) (names []string, more bool, err error) {
	return s.b.ListDomains(ctx, after, max)
}
func (s *Store) GetMailbox(ctx context.Context, id uuid.UUID) (Versioned[Mailbox], error) {
	return s.b.GetMailbox(WithPrimaryReads(ctx), id)
}
func (s *Store) ListMailboxes(ctx context.Context, fn func(id uuid.UUID) error) error {
	return s.b.ListMailboxes(ctx, fn)
}
func (s *Store) ListMailboxAccounts(ctx context.Context, id uuid.UUID, after string, max int) (usernames []string, more bool, err error) {
	return s.b.ListMailboxAccounts(ctx, id, after, max)
}
func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (Versioned[Account], error) {
	return s.b.GetAccount(ctx, id)
}
func (s *Store) GetAccountByUsername(ctx context.Context, username string) (Versioned[Account], error) {
	return s.b.GetAccountByUsername(ctx, username)
}
func (s *Store) ListDomainAccounts(ctx context.Context, domain, prefix, after string, max int) (usernames []string, more bool, err error) {
	return s.b.ListDomainAccounts(ctx, domain, prefix, after, max)
}
func (s *Store) ListMailboxAliases(ctx context.Context, id uuid.UUID) ([]string, error) {
	return s.b.ListMailboxAliases(ctx, id)
}
func (s *Store) ListAliasesTo(ctx context.Context, address string) ([]string, error) {
	return s.b.ListAliasesTo(ctx, address)
}
func (s *Store) GetAlias(ctx context.Context, address string) (Versioned[Alias], error) {
	return s.b.GetAlias(ctx, address)
}
func (s *Store) ListDomainAliases(ctx context.Context, domain, after string, max int) (addresses []string, more bool, err error) {
	return s.b.ListDomainAliases(ctx, domain, after, max)
}
func (s *Store) CreateToken(ctx context.Context, accountID uuid.UUID, roles []string, expiresAt time.Time, comment string) (Token, string, error) {
	return s.b.CreateToken(ctx, accountID, roles, expiresAt, comment)
}
func (s *Store) CreateAdminToken(ctx context.Context, role string, expiresAt time.Time, comment string) (Token, string, error) {
	return s.b.CreateAdminToken(ctx, role, expiresAt, comment)
}
func (s *Store) ListAdminTokens(ctx context.Context) ([]Token, error) {
	return s.b.ListAdminTokens(ctx)
}
func (s *Store) DeleteAdminToken(ctx context.Context, tokenID string) error {
	return s.b.DeleteAdminToken(ctx, tokenID)
}
func (s *Store) LookupToken(ctx context.Context, secret string) (Versioned[Token], error) {
	return s.b.LookupToken(ctx, secret)
}
func (s *Store) ListTokens(ctx context.Context, accountID uuid.UUID) ([]Token, error) {
	return s.b.ListTokens(ctx, accountID)
}
func (s *Store) DeleteToken(ctx context.Context, accountID uuid.UUID, tokenID string) error {
	return s.b.DeleteToken(ctx, accountID, tokenID)
}

func (s *Store) TouchToken(ctx context.Context, t Versioned[Token]) { s.b.TouchToken(ctx, t) }

// ---- storage registry ----

func (s *Store) CreateStorage(ctx context.Context, c Storage) (Storage, error) {
	return s.b.CreateStorage(ctx, c)
}
func (s *Store) GetStorage(ctx context.Context, id int64) (Storage, error) {
	return s.b.GetStorage(ctx, id)
}
func (s *Store) ListStorage(ctx context.Context) ([]Storage, error) { return s.b.ListStorage(ctx) }
func (s *Store) UpdateStorage(ctx context.Context, id int64, fn func(*Storage) error) (Storage, error) {
	return s.b.UpdateStorage(ctx, id, fn)
}
func (s *Store) DeleteStorage(ctx context.Context, id int64) error { return s.b.DeleteStorage(ctx, id) }

// ---- node registry ----

func (s *Store) RegisterNode(ctx context.Context, n Node) (Node, error) {
	return s.b.RegisterNode(ctx, n)
}
func (s *Store) ListNodes(ctx context.Context, mode string) ([]Node, error) {
	return s.b.ListNodes(ctx, mode)
}
func (s *Store) DeleteNode(ctx context.Context, mode, address string) error {
	return s.b.DeleteNode(ctx, mode, address)
}
func (s *Store) PurgeNodes(ctx context.Context, cutoff time.Time) (int, error) {
	return s.b.PurgeNodes(ctx, cutoff)
}

// RemovalBackend atomically removes identity and schedules cleanup while pinning storage.
type RemovalBackend interface {
	ScheduleMailboxRemoval(context.Context, uuid.UUID) error
	FinishMailboxRemoval(context.Context, uuid.UUID) error
}

// ScheduleMailboxRemoval returns supported=false on the development backend.
func (s *Store) ScheduleMailboxRemoval(ctx context.Context, id uuid.UUID) (supported bool, err error) {
	b, ok := s.b.(RemovalBackend)
	if !ok {
		return false, nil
	}
	err = b.ScheduleMailboxRemoval(ctx, id)
	s.cache.Delete(mailboxKey(id))
	return true, err
}
func (s *Store) FinishMailboxRemoval(ctx context.Context, id uuid.UUID) error {
	if b, ok := s.b.(RemovalBackend); ok {
		return b.FinishMailboxRemoval(ctx, id)
	}
	return nil
}
