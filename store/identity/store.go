package identity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

// Namespaces in the meta pool.
const (
	nsDomains   = "domains"
	nsMailboxes = "mailboxes"
	nsAccounts  = "accounts"
	nsNames     = "names"
	nsAliases   = "aliases"
	nsTokens    = "tokens"

	allDomainsObject = "_all"
	schemaVersion    = "1"
	tokenPrefix      = "dvt_"
)

// Store is the identity store.
type Store struct {
	domains   rados.IOContext
	mailboxes rados.IOContext
	accounts  rados.IOContext
	names     rados.IOContext
	aliases   rados.IOContext
	tokens    rados.IOContext
	Now       func() time.Time
}

// New returns a store on the given meta pool context.
func New(meta rados.IOContext) *Store {
	return &Store{
		domains:   meta.Namespace(nsDomains),
		mailboxes: meta.Namespace(nsMailboxes),
		accounts:  meta.Namespace(nsAccounts),
		names:     meta.Namespace(nsNames),
		aliases:   meta.Namespace(nsAliases),
		tokens:    meta.Namespace(nsTokens),
		Now:       time.Now,
	}
}

// ---- helpers ----

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, rados.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, rados.ErrExists):
		return ErrExists
	case errors.Is(err, rados.ErrVersionMismatch):
		return ErrConflict
	}
	return err
}

func getJSON[T any](ctx context.Context, io rados.IOContext, oid string) (Versioned[T], error) {
	var out Versioned[T]
	data, ver, err := io.Get(ctx, oid)
	if err != nil {
		return out, mapErr(err)
	}
	if err := json.Unmarshal(data, &out.Value); err != nil {
		return out, fmt.Errorf("identity: decode %s: %w", oid, err)
	}
	out.Version = ver
	return out, nil
}

func createJSON(ctx context.Context, io rados.IOContext, oid string, v any) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	ver, err := io.Write(ctx, oid, rados.NewWriteTx().CreateExclusive().WriteFull(data).SetXattr("schema", []byte(schemaVersion)))
	return ver, mapErr(err)
}

func updateJSON(ctx context.Context, io rados.IOContext, oid string, version uint64, v any) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	ver, err := io.Write(ctx, oid, rados.NewWriteTx().AssertVersion(version).WriteFull(data))
	return ver, mapErr(err)
}

// listOmap returns omap entries with the prefix stripped from keys.
func listOmap(ctx context.Context, io rados.IOContext, oid, prefix, after string, max int) ([]rados.KV, bool, error) {
	kv, more, err := io.OmapList(ctx, oid, prefix, prefix+after, max)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	for i := range kv {
		kv[i].Key = strings.TrimPrefix(kv[i].Key, prefix)
	}
	return kv, more, nil
}

// NormalizeAddress lowercases and validates an e-mail address, returning the
// address and its domain.
func NormalizeAddress(addr string) (address, domain string, err error) {
	addr = strings.TrimSpace(strings.ToLower(addr))
	if addr == "" || strings.ContainsAny(addr, " \t\r\n<>\x00") {
		return "", "", fmt.Errorf("%w: address %q", ErrInvalid, addr)
	}
	i := strings.LastIndex(addr, "@")
	if i <= 0 || i == len(addr)-1 {
		return "", "", fmt.Errorf("%w: address %q", ErrInvalid, addr)
	}
	if _, err := mail.ParseAddress(addr); err != nil {
		return "", "", fmt.Errorf("%w: address %q", ErrInvalid, addr)
	}
	return addr, addr[i+1:], nil
}

// NormalizeDomain lowercases and validates a domain name.
func NormalizeDomain(d string) (string, error) {
	d = strings.TrimSpace(strings.ToLower(d))
	if d == "" || strings.ContainsAny(d, " @/\\\t\r\n\x00") || strings.HasPrefix(d, ".") || strings.HasSuffix(d, ".") {
		return "", fmt.Errorf("%w: domain %q", ErrInvalid, d)
	}
	return d, nil
}

func validateRoles(roles []string) error {
	seen := map[string]bool{}
	for _, r := range roles {
		if !ValidRoles[r] {
			return fmt.Errorf("%w: role %q", ErrInvalid, r)
		}
		if seen[r] {
			return fmt.Errorf("%w: duplicate role %q", ErrInvalid, r)
		}
		seen[r] = true
	}
	return nil
}

// ---- Domains ----

// CreateDomain creates a domain.
func (s *Store) CreateDomain(ctx context.Context, d Domain) (Domain, error) {
	name, err := NormalizeDomain(d.Name)
	if err != nil {
		return Domain{}, err
	}
	d.Name = name
	now := s.Now().UTC()
	d.Created, d.Updated = now, now
	if _, err := createJSON(ctx, s.domains, name, d); err != nil {
		return Domain{}, err
	}
	// Listing (repairable by reconcile).
	if _, err := s.domains.Write(ctx, allDomainsObject, rados.NewWriteTx().OmapSet(map[string][]byte{name: nil})); err != nil {
		return Domain{}, mapErr(err)
	}
	return d, nil
}

// GetDomain returns a domain.
func (s *Store) GetDomain(ctx context.Context, name string) (Versioned[Domain], error) {
	name, err := NormalizeDomain(name)
	if err != nil {
		return Versioned[Domain]{}, err
	}
	return getJSON[Domain](ctx, s.domains, name)
}

// UpdateDomain applies fn to the domain and saves it, retrying on conflict.
func (s *Store) UpdateDomain(ctx context.Context, name string, fn func(*Domain) error) (Domain, error) {
	name, err := NormalizeDomain(name)
	if err != nil {
		return Domain{}, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := getJSON[Domain](ctx, s.domains, name)
		if err != nil {
			return Domain{}, err
		}
		d := cur.Value
		if err := fn(&d); err != nil {
			return Domain{}, err
		}
		d.Name = name
		d.Updated = s.Now().UTC()
		if _, err := updateJSON(ctx, s.domains, name, cur.Version, d); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Domain{}, err
		}
		return d, nil
	}
	return Domain{}, ErrConflict
}

// DeleteDomain removes an empty domain (no accounts or aliases).
func (s *Store) DeleteDomain(ctx context.Context, name string) error {
	name, err := NormalizeDomain(name)
	if err != nil {
		return err
	}
	if _, err := getJSON[Domain](ctx, s.domains, name); err != nil {
		return err
	}
	kv, _, err := s.domains.OmapList(ctx, name, "", "", 1)
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	if len(kv) > 0 {
		return fmt.Errorf("%w: domain %s has accounts or aliases", ErrInUse, name)
	}
	if _, err := s.domains.Write(ctx, allDomainsObject, rados.NewWriteTx().OmapRemove(name)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	return mapErr(s.domains.Delete(ctx, name))
}

// ListDomains lists domain names after the cursor.
func (s *Store) ListDomains(ctx context.Context, after string, max int) (names []string, more bool, err error) {
	kv, more, err := listOmap(ctx, s.domains, allDomainsObject, "", after, max)
	if err != nil {
		return nil, false, err
	}
	for _, e := range kv {
		names = append(names, e.Key)
	}
	return names, more, nil
}

// ---- Mailboxes ----

// CreateMailbox creates a mailbox. A zero ID is replaced by a new UUID.
func (s *Store) CreateMailbox(ctx context.Context, m Mailbox) (Mailbox, error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	now := s.Now().UTC()
	m.Created, m.Updated = now, now
	m.Hold = nil
	if _, err := createJSON(ctx, s.mailboxes, m.ID.String(), m); err != nil {
		return Mailbox{}, err
	}
	return m, nil
}

// GetMailbox returns a mailbox.
func (s *Store) GetMailbox(ctx context.Context, id uuid.UUID) (Versioned[Mailbox], error) {
	return getJSON[Mailbox](ctx, s.mailboxes, id.String())
}

// UpdateMailbox applies fn and saves, retrying on conflict.
func (s *Store) UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*Mailbox) error) (Mailbox, error) {
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := getJSON[Mailbox](ctx, s.mailboxes, id.String())
		if err != nil {
			return Mailbox{}, err
		}
		m := cur.Value
		if err := fn(&m); err != nil {
			return Mailbox{}, err
		}
		m.ID = id
		m.Updated = s.Now().UTC()
		if _, err := updateJSON(ctx, s.mailboxes, id.String(), cur.Version, m); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Mailbox{}, err
		}
		return m, nil
	}
	return Mailbox{}, ErrConflict
}

// DeleteMailbox removes the mailbox object. It refuses if accounts still link
// to it or a hold is active. Removing the mailbox's mail data is a worker
// task scheduled by the service layer, not done here.
func (s *Store) DeleteMailbox(ctx context.Context, id uuid.UUID) error {
	cur, err := getJSON[Mailbox](ctx, s.mailboxes, id.String())
	if err != nil {
		return err
	}
	if cur.Value.Hold != nil {
		return fmt.Errorf("%w: mailbox %s is on hold", ErrInUse, id)
	}
	accts, _, err := s.ListMailboxAccounts(ctx, id, "", 1)
	if err != nil {
		return err
	}
	if len(accts) > 0 {
		return fmt.Errorf("%w: mailbox %s has accounts", ErrInUse, id)
	}
	return mapErr(s.mailboxes.Delete(ctx, id.String()))
}

// ListMailboxes lists mailbox ids (object listing; unordered, no cursor).
func (s *Store) ListMailboxes(ctx context.Context, fn func(id uuid.UUID) error) error {
	return s.mailboxes.List(ctx, func(oid string) error {
		id, err := uuid.Parse(oid)
		if err != nil {
			return nil // ignore foreign objects
		}
		return fn(id)
	})
}

// ListMailboxAccounts lists the usernames of accounts linked to the mailbox.
func (s *Store) ListMailboxAccounts(ctx context.Context, id uuid.UUID, after string, max int) (usernames []string, more bool, err error) {
	kv, more, err := listOmap(ctx, s.mailboxes, id.String(), "account/", after, max)
	if err != nil {
		return nil, false, err
	}
	for _, e := range kv {
		usernames = append(usernames, string(e.Value))
	}
	return usernames, more, nil
}

// ---- Accounts ----

// CreateAccount creates an account. The username must be unique; the domain
// must exist; a non-admin account must reference an existing mailbox.
func (s *Store) CreateAccount(ctx context.Context, a Account) (Account, error) {
	username, domain, err := NormalizeAddress(a.Username)
	if err != nil {
		return Account{}, err
	}
	if err := validateRoles(a.Roles); err != nil {
		return Account{}, err
	}
	a.Username, a.Domain = username, domain
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if a.MailboxID == uuid.Nil {
		return Account{}, fmt.Errorf("%w: account needs a mailbox", ErrInvalid)
	}
	if _, err := s.GetMailbox(ctx, a.MailboxID); err != nil {
		return Account{}, fmt.Errorf("mailbox %s: %w", a.MailboxID, err)
	}
	if _, err := s.GetDomain(ctx, domain); err != nil {
		if errors.Is(err, ErrNotFound) {
			return Account{}, fmt.Errorf("%w: %s", ErrDomainUnknown, domain)
		}
		return Account{}, err
	}
	now := s.Now().UTC()
	a.Created, a.Updated = now, now
	for i := range a.Passwords {
		if a.Passwords[i].ID == "" {
			a.Passwords[i].ID = uuid.NewString()
		}
		if a.Passwords[i].Kind == "" {
			a.Passwords[i].Kind = PasswordPrimary
		}
		if a.Passwords[i].Created.IsZero() {
			a.Passwords[i].Created = now
		}
	}
	// 1. Name pointer (uniqueness).
	if _, err := s.names.Write(ctx, username, rados.NewWriteTx().CreateExclusive().WriteFull([]byte(a.ID.String()))); err != nil {
		return Account{}, mapErr(err)
	}
	// 2. Primary object.
	if _, err := createJSON(ctx, s.accounts, a.ID.String(), a); err != nil {
		_ = s.names.Delete(ctx, username) // compensate
		return Account{}, err
	}
	// 3. Listings.
	if _, err := s.domains.Write(ctx, domain, rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{"account/" + username: []byte(a.ID.String())})); err != nil {
		return Account{}, mapErr(err)
	}
	if _, err := s.mailboxes.Write(ctx, a.MailboxID.String(), rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{"account/" + a.ID.String(): []byte(username)})); err != nil {
		return Account{}, mapErr(err)
	}
	return a, nil
}

// GetAccount returns an account by id.
func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (Versioned[Account], error) {
	return getJSON[Account](ctx, s.accounts, id.String())
}

// GetAccountByUsername resolves a username to its account.
func (s *Store) GetAccountByUsername(ctx context.Context, username string) (Versioned[Account], error) {
	username, _, err := NormalizeAddress(username)
	if err != nil {
		return Versioned[Account]{}, err
	}
	data, _, err := s.names.Get(ctx, username)
	if err != nil {
		return Versioned[Account]{}, mapErr(err)
	}
	id, err := uuid.Parse(string(data))
	if err != nil {
		return Versioned[Account]{}, fmt.Errorf("identity: corrupt name pointer %s", username)
	}
	acct, err := getJSON[Account](ctx, s.accounts, id.String())
	if err != nil {
		return Versioned[Account]{}, err // dangling pointer reads as not found; reconcile cleans it
	}
	return acct, nil
}

// UpdateAccount applies fn and saves, retrying on conflict. Username and
// domain cannot be changed here; mailbox relinking is handled by RelinkAccount.
func (s *Store) UpdateAccount(ctx context.Context, id uuid.UUID, fn func(*Account) error) (Account, error) {
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := getJSON[Account](ctx, s.accounts, id.String())
		if err != nil {
			return Account{}, err
		}
		a := cur.Value
		if err := fn(&a); err != nil {
			return Account{}, err
		}
		if a.Username != cur.Value.Username || a.Domain != cur.Value.Domain || a.MailboxID != cur.Value.MailboxID {
			return Account{}, fmt.Errorf("%w: username, domain and mailbox cannot be changed via UpdateAccount", ErrInvalid)
		}
		if err := validateRoles(a.Roles); err != nil {
			return Account{}, err
		}
		a.ID = id
		a.Updated = s.Now().UTC()
		for i := range a.Passwords {
			if a.Passwords[i].ID == "" {
				a.Passwords[i].ID = uuid.NewString()
			}
			if a.Passwords[i].Kind == "" {
				a.Passwords[i].Kind = PasswordPrimary
			}
			if a.Passwords[i].Created.IsZero() {
				a.Passwords[i].Created = a.Updated
			}
		}
		if _, err := updateJSON(ctx, s.accounts, id.String(), cur.Version, a); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Account{}, err
		}
		return a, nil
	}
	return Account{}, ErrConflict
}

// RelinkAccount moves an account to another mailbox.
func (s *Store) RelinkAccount(ctx context.Context, id, mailboxID uuid.UUID) (Account, error) {
	if _, err := s.GetMailbox(ctx, mailboxID); err != nil {
		return Account{}, fmt.Errorf("mailbox %s: %w", mailboxID, err)
	}
	cur, err := getJSON[Account](ctx, s.accounts, id.String())
	if err != nil {
		return Account{}, err
	}
	a := cur.Value
	old := a.MailboxID
	a.MailboxID = mailboxID
	a.Updated = s.Now().UTC()
	if _, err := updateJSON(ctx, s.accounts, id.String(), cur.Version, a); err != nil {
		return Account{}, err
	}
	if old != uuid.Nil {
		if _, err := s.mailboxes.Write(ctx, old.String(), rados.NewWriteTx().OmapRemove("account/"+id.String())); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return Account{}, mapErr(err)
		}
	}
	if _, err := s.mailboxes.Write(ctx, mailboxID.String(), rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{"account/" + id.String(): []byte(a.Username)})); err != nil {
		return Account{}, mapErr(err)
	}
	return a, nil
}

// DeleteAccount removes an account, its tokens, listings and name pointer.
func (s *Store) DeleteAccount(ctx context.Context, id uuid.UUID) error {
	cur, err := getJSON[Account](ctx, s.accounts, id.String())
	if err != nil {
		return err
	}
	a := cur.Value
	// Tokens.
	toks, _, err := listOmap(ctx, s.accounts, id.String(), "token/", "", 0)
	if err != nil {
		return err
	}
	for _, t := range toks {
		if err := s.tokens.Delete(ctx, string(t.Value)); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	// Listings.
	if _, err := s.domains.Write(ctx, a.Domain, rados.NewWriteTx().OmapRemove("account/"+a.Username)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	if a.MailboxID != uuid.Nil {
		if _, err := s.mailboxes.Write(ctx, a.MailboxID.String(), rados.NewWriteTx().OmapRemove("account/"+id.String())); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	// Name pointer, then primary.
	if err := s.names.Delete(ctx, a.Username); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	return mapErr(s.accounts.Delete(ctx, id.String()))
}

// ListDomainAccounts lists usernames in a domain with an optional prefix
// (the part after "account/", i.e. the username) after the cursor.
func (s *Store) ListDomainAccounts(ctx context.Context, domain, prefix, after string, max int) (usernames []string, more bool, err error) {
	domain, err = NormalizeDomain(domain)
	if err != nil {
		return nil, false, err
	}
	kv, more, err := listOmap(ctx, s.domains, domain, "account/"+strings.ToLower(prefix), strings.TrimPrefix(after, strings.ToLower(prefix)), max)
	if err != nil {
		return nil, false, err
	}
	for _, e := range kv {
		usernames = append(usernames, strings.ToLower(prefix)+e.Key)
	}
	return usernames, more, nil
}

// ---- Aliases ----

// CreateAlias creates an alias. Address "@domain" creates the catch-all.
func (s *Store) CreateAlias(ctx context.Context, al Alias) (Alias, error) {
	address, domain, err := normalizeAliasAddress(al.Address)
	if err != nil {
		return Alias{}, err
	}
	if len(al.Destinations) == 0 {
		return Alias{}, fmt.Errorf("%w: alias needs at least one destination", ErrInvalid)
	}
	for i, d := range al.Destinations {
		if (d.MailboxID == uuid.Nil) == (d.Address == "") {
			return Alias{}, fmt.Errorf("%w: destination must be exactly one of mailbox or address", ErrInvalid)
		}
		if d.Address != "" {
			na, _, err := NormalizeAddress(d.Address)
			if err != nil {
				return Alias{}, err
			}
			if na == address {
				return Alias{}, fmt.Errorf("%w: alias points at itself", ErrInvalid)
			}
			al.Destinations[i].Address = na
		} else if _, err := s.GetMailbox(ctx, d.MailboxID); err != nil {
			return Alias{}, fmt.Errorf("mailbox %s: %w", d.MailboxID, err)
		}
	}
	if _, err := s.GetDomain(ctx, domain); err != nil {
		if errors.Is(err, ErrNotFound) {
			return Alias{}, fmt.Errorf("%w: %s", ErrDomainUnknown, domain)
		}
		return Alias{}, err
	}
	if !strings.HasPrefix(address, "@") {
		if _, _, err := s.names.Get(ctx, address); err == nil {
			return Alias{}, fmt.Errorf("%w: %s is an account", ErrExists, address)
		}
	}
	al.Address, al.Domain = address, domain
	now := s.Now().UTC()
	al.Created, al.Updated = now, now
	if _, err := createJSON(ctx, s.aliases, address, al); err != nil {
		return Alias{}, err
	}
	if _, err := s.domains.Write(ctx, domain, rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{"alias/" + address: nil})); err != nil {
		return Alias{}, mapErr(err)
	}
	if err := s.setMailboxAliasListings(ctx, address, nil, al.Destinations); err != nil {
		return Alias{}, err
	}
	return al, nil
}

// setMailboxAliasListings maintains the reverse listing `alias/<address>` on
// every destination mailbox (used for sender identities).
func (s *Store) setMailboxAliasListings(ctx context.Context, address string, old, cur []Destination) error {
	was := map[uuid.UUID]bool{}
	for _, d := range old {
		if d.MailboxID != uuid.Nil {
			was[d.MailboxID] = true
		}
	}
	now := map[uuid.UUID]bool{}
	for _, d := range cur {
		if d.MailboxID != uuid.Nil {
			now[d.MailboxID] = true
		}
	}
	for id := range now {
		if _, err := s.mailboxes.Write(ctx, id.String(), rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{"alias/" + address: nil})); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	for id := range was {
		if now[id] {
			continue
		}
		if _, err := s.mailboxes.Write(ctx, id.String(), rados.NewWriteTx().OmapRemove("alias/"+address)); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	return nil
}

// ListMailboxAliases lists alias addresses that deliver to the mailbox.
func (s *Store) ListMailboxAliases(ctx context.Context, id uuid.UUID) ([]string, error) {
	kv, _, err := listOmap(ctx, s.mailboxes, id.String(), "alias/", "", 0)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(kv))
	for _, e := range kv {
		out = append(out, e.Key)
	}
	return out, nil
}

func normalizeAliasAddress(addr string) (string, string, error) {
	addr = strings.TrimSpace(strings.ToLower(addr))
	if strings.HasPrefix(addr, "@") {
		d, err := NormalizeDomain(addr[1:])
		if err != nil {
			return "", "", err
		}
		return "@" + d, d, nil
	}
	return NormalizeAddress(addr)
}

// GetAlias returns an alias.
func (s *Store) GetAlias(ctx context.Context, address string) (Versioned[Alias], error) {
	address, _, err := normalizeAliasAddress(address)
	if err != nil {
		return Versioned[Alias]{}, err
	}
	return getJSON[Alias](ctx, s.aliases, address)
}

// UpdateAlias applies fn and saves, retrying on conflict.
func (s *Store) UpdateAlias(ctx context.Context, address string, fn func(*Alias) error) (Alias, error) {
	address, domain, err := normalizeAliasAddress(address)
	if err != nil {
		return Alias{}, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := getJSON[Alias](ctx, s.aliases, address)
		if err != nil {
			return Alias{}, err
		}
		al := cur.Value
		if err := fn(&al); err != nil {
			return Alias{}, err
		}
		if len(al.Destinations) == 0 {
			return Alias{}, fmt.Errorf("%w: alias needs at least one destination", ErrInvalid)
		}
		al.Address, al.Domain = address, domain
		al.Updated = s.Now().UTC()
		if _, err := updateJSON(ctx, s.aliases, address, cur.Version, al); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Alias{}, err
		}
		if err := s.setMailboxAliasListings(ctx, address, cur.Value.Destinations, al.Destinations); err != nil {
			return Alias{}, err
		}
		return al, nil
	}
	return Alias{}, ErrConflict
}

// DeleteAlias removes an alias.
func (s *Store) DeleteAlias(ctx context.Context, address string) error {
	address, domain, err := normalizeAliasAddress(address)
	if err != nil {
		return err
	}
	cur, err := getJSON[Alias](ctx, s.aliases, address)
	if err != nil {
		return err
	}
	if _, err := s.domains.Write(ctx, domain, rados.NewWriteTx().OmapRemove("alias/"+address)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	if err := s.setMailboxAliasListings(ctx, address, cur.Value.Destinations, nil); err != nil {
		return err
	}
	return mapErr(s.aliases.Delete(ctx, address))
}

// ListDomainAliases lists alias addresses in a domain.
func (s *Store) ListDomainAliases(ctx context.Context, domain, after string, max int) (addresses []string, more bool, err error) {
	domain, err = NormalizeDomain(domain)
	if err != nil {
		return nil, false, err
	}
	kv, more, err := listOmap(ctx, s.domains, domain, "alias/", after, max)
	if err != nil {
		return nil, false, err
	}
	for _, e := range kv {
		addresses = append(addresses, e.Key)
	}
	return addresses, more, nil
}

// ---- Tokens ----

// HashToken returns the object name for a token: hex SHA-256.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// NewTokenSecret generates a fresh token string.
func NewTokenSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// CreateToken creates an API token for an account and returns the token
// record and the plaintext secret (shown once).
func (s *Store) CreateToken(ctx context.Context, accountID uuid.UUID, roles []string, expiresAt time.Time, comment string) (Token, string, error) {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return Token{}, "", fmt.Errorf("%w: token comment is required", ErrInvalid)
	}
	now := s.Now().UTC()
	if !expiresAt.After(now) {
		return Token{}, "", fmt.Errorf("%w: token expiration must be in the future", ErrInvalid)
	}
	if err := validateRoles(roles); err != nil {
		return Token{}, "", err
	}
	acct, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return Token{}, "", err
	}
	if len(roles) == 0 {
		roles = append([]string(nil), acct.Value.Roles...)
	}
	for _, r := range roles {
		if !acct.Value.HasRole(r) {
			return Token{}, "", fmt.Errorf("%w: account lacks role %q", ErrInvalid, r)
		}
	}
	secret, err := NewTokenSecret()
	if err != nil {
		return Token{}, "", err
	}
	t := Token{
		ID:        uuid.NewString(),
		Hash:      HashToken(secret),
		AccountID: accountID,
		Roles:     roles,
		ExpiresAt: expiresAt.UTC(),
		Comment:   comment,
		Created:   now,
	}
	if _, err := createJSON(ctx, s.tokens, t.Hash, t); err != nil {
		return Token{}, "", err
	}
	if _, err := s.accounts.Write(ctx, accountID.String(), rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{"token/" + t.ID: []byte(t.Hash)})); err != nil {
		return Token{}, "", mapErr(err)
	}
	return t, secret, nil
}

const adminTokensObject = "_admin"

// CreateAdminToken creates an admin API token (full or read-only). Admins
// are not accounts: the token is the whole identity.
func (s *Store) CreateAdminToken(ctx context.Context, role string, expiresAt time.Time, comment string) (Token, string, error) {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return Token{}, "", fmt.Errorf("%w: token comment is required", ErrInvalid)
	}
	if !AdminRoles[role] {
		return Token{}, "", fmt.Errorf("%w: admin role %q", ErrInvalid, role)
	}
	now := s.Now().UTC()
	if !expiresAt.After(now) {
		return Token{}, "", fmt.Errorf("%w: token expiration must be in the future", ErrInvalid)
	}
	secret, err := NewTokenSecret()
	if err != nil {
		return Token{}, "", err
	}
	t := Token{ID: uuid.NewString(), Hash: HashToken(secret), Roles: []string{role}, ExpiresAt: expiresAt.UTC(), Comment: comment, Created: now}
	if _, err := createJSON(ctx, s.tokens, t.Hash, t); err != nil {
		return Token{}, "", err
	}
	if _, err := s.tokens.Write(ctx, adminTokensObject, rados.NewWriteTx().OmapSet(map[string][]byte{"token/" + t.ID: []byte(t.Hash)})); err != nil {
		return Token{}, "", mapErr(err)
	}
	return t, secret, nil
}

// ListAdminTokens lists admin tokens (without secrets).
func (s *Store) ListAdminTokens(ctx context.Context) ([]Token, error) {
	kv, _, err := listOmap(ctx, s.tokens, adminTokensObject, "token/", "", 0)
	if err != nil {
		return nil, err
	}
	out := []Token{}
	for _, e := range kv {
		t, err := getJSON[Token](ctx, s.tokens, string(e.Value))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, t.Value)
	}
	return out, nil
}

// DeleteAdminToken revokes an admin token by id.
func (s *Store) DeleteAdminToken(ctx context.Context, tokenID string) error {
	m, err := s.tokens.OmapGet(ctx, adminTokensObject, []string{"token/" + tokenID})
	if err != nil {
		return mapErr(err)
	}
	hash, ok := m["token/"+tokenID]
	if !ok {
		return ErrNotFound
	}
	if err := s.tokens.Delete(ctx, string(hash)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	_, err = s.tokens.Write(ctx, adminTokensObject, rados.NewWriteTx().OmapRemove("token/"+tokenID))
	return mapErr(err)
}

// LookupToken resolves a presented token to its record. It does not check
// expiry; callers do, so they can report it distinctly.
func (s *Store) LookupToken(ctx context.Context, secret string) (Versioned[Token], error) {
	if !strings.HasPrefix(secret, tokenPrefix) {
		return Versioned[Token]{}, ErrNotFound
	}
	return getJSON[Token](ctx, s.tokens, HashToken(secret))
}

// TouchToken records last use. Best effort; conflicts are ignored.
func (s *Store) TouchToken(ctx context.Context, t Versioned[Token]) {
	t.Value.LastUsed = s.Now().UTC()
	_, _ = updateJSON(ctx, s.tokens, t.Value.Hash, t.Version, t.Value)
}

// ListTokens lists an account's tokens.
func (s *Store) ListTokens(ctx context.Context, accountID uuid.UUID) ([]Token, error) {
	kv, _, err := listOmap(ctx, s.accounts, accountID.String(), "token/", "", 0)
	if err != nil {
		return nil, err
	}
	var out []Token
	for _, e := range kv {
		t, err := getJSON[Token](ctx, s.tokens, string(e.Value))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue // dangling listing; reconcile repairs
			}
			return nil, err
		}
		out = append(out, t.Value)
	}
	return out, nil
}

// DeleteToken revokes a token by id.
func (s *Store) DeleteToken(ctx context.Context, accountID uuid.UUID, tokenID string) error {
	m, err := s.accounts.OmapGet(ctx, accountID.String(), []string{"token/" + tokenID})
	if err != nil {
		return mapErr(err)
	}
	hash, ok := m["token/"+tokenID]
	if !ok {
		return ErrNotFound
	}
	if err := s.tokens.Delete(ctx, string(hash)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	_, err = s.accounts.Write(ctx, accountID.String(), rados.NewWriteTx().OmapRemove("token/"+tokenID))
	return mapErr(err)
}
