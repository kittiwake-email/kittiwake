package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// Cached lookups. The protocol daemons read the same domain, mailbox,
// account and alias objects for every command (state and hold checks,
// recipient resolution, Postfix socketmap queries). The *Cached getters
// reuse a result for Store.CacheTTL per process; not-found is cached too.
// Writes through this store drop the entry, so the process that makes a
// change sees it at once; other processes see it within the TTL. Admin
// reads and optimistic updates use the plain getters, which are never
// cached.

func (s *Store) cacheGet(key string) (any, error, bool) {
	return s.cache.Get(key, s.Now(), s.CacheTTL)
}

func (s *Store) cachePut(key string, v any, err error) {
	if s.CacheTTL <= 0 || (err != nil && !errors.Is(err, ErrNotFound)) {
		return
	}
	s.cache.Put(key, v, err, s.Now())
}

// InvalidateCache drops every cached lookup in this process.
func (s *Store) InvalidateCache() { s.cache.Clear() }

func namespaceKey(name string) string      { return "n:" + name }
func domainKey(name string) string         { return "d:" + name }
func mailboxKey(id uuid.UUID) string       { return "m:" + id.String() }
func accountKey(username string) string    { return "a:" + username }
func aliasKey(address string) string       { return "l:" + address }
func accountDomainKey(id uuid.UUID) string { return "ad:" + id.String() }

// AccountDomainCached returns the account's domain as last seen within
// CacheTTL (proxies route every request of a token by it).
func (s *Store) AccountDomainCached(ctx context.Context, id uuid.UUID) (string, error) {
	if v, err, ok := s.cacheGet(accountDomainKey(id)); ok {
		return v.(string), err
	}
	v, err := s.b.AccountDomain(ctx, id)
	s.cachePut(accountDomainKey(id), v, err)
	return v, err
}

// GetNamespaceCached returns the namespace as last seen within CacheTTL.
func (s *Store) GetNamespaceCached(ctx context.Context, name string) (Namespace, error) {
	if PrimaryReads(ctx) {
		v, err := s.GetNamespace(ctx, name)
		return v.Value, err
	}
	name, err := NormalizeNamespace(name)
	if err != nil {
		return Namespace{}, err
	}
	if v, err, ok := s.cacheGet(namespaceKey(name)); ok {
		return v.(Namespace), err
	}
	v, err := s.GetNamespace(ctx, name)
	s.cachePut(namespaceKey(name), v.Value, err)
	return v.Value, err
}

// GetDomainCached returns the domain as last seen within CacheTTL.
func (s *Store) GetDomainCached(ctx context.Context, name string) (Domain, error) {
	if PrimaryReads(ctx) {
		v, err := s.GetDomain(ctx, name)
		return v.Value, err
	}
	name, err := NormalizeDomain(name)
	if err != nil {
		return Domain{}, err
	}
	if v, err, ok := s.cacheGet(domainKey(name)); ok {
		return v.(Domain), err
	}
	v, err := s.GetDomain(ctx, name)
	s.cachePut(domainKey(name), v.Value, err)
	return v.Value, err
}

// GetMailboxCached returns the mailbox as last seen within CacheTTL.
func (s *Store) GetMailboxCached(ctx context.Context, id uuid.UUID) (Mailbox, error) {
	if PrimaryReads(ctx) {
		v, err := s.GetMailbox(ctx, id)
		return v.Value, err
	}
	if v, err, ok := s.cacheGet(mailboxKey(id)); ok {
		return v.(Mailbox), err
	}
	v, err := s.b.GetMailbox(ctx, id)
	s.cachePut(mailboxKey(id), v.Value, err)
	return v.Value, err
}

// GetAccountByUsernameCached returns the account as last seen within CacheTTL.
func (s *Store) GetAccountByUsernameCached(ctx context.Context, username string) (Account, error) {
	if PrimaryReads(ctx) {
		v, err := s.GetAccountByUsername(ctx, username)
		return v.Value, err
	}
	username, _, err := NormalizeAddress(username)
	if err != nil {
		return Account{}, err
	}
	if v, err, ok := s.cacheGet(accountKey(username)); ok {
		return v.(Account), err
	}
	v, err := s.GetAccountByUsername(ctx, username)
	s.cachePut(accountKey(username), v.Value, err)
	return v.Value, err
}

// GetAliasCached returns the alias as last seen within CacheTTL.
func (s *Store) GetAliasCached(ctx context.Context, address string) (Alias, error) {
	if PrimaryReads(ctx) {
		v, err := s.GetAlias(ctx, address)
		return v.Value, err
	}
	address, _, err := normalizeAliasAddress(address)
	if err != nil {
		return Alias{}, err
	}
	if v, err, ok := s.cacheGet(aliasKey(address)); ok {
		return v.(Alias), err
	}
	v, err := s.GetAlias(ctx, address)
	s.cachePut(aliasKey(address), v.Value, err)
	return v.Value, err
}
