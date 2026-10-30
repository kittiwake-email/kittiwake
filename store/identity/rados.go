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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
)

// Namespaces in the meta pool.
const (
	nsNamespaces = "namespaces"
	nsDomains    = "domains"
	nsMailboxes  = "mailboxes"
	nsAccounts   = "accounts"
	nsNames      = "names"
	nsAliases    = "aliases"
	nsTokens     = "tokens"
	nsStorage    = "storage"
	nsNodes      = "nodes"

	allDomainsObject = "_all"
	schemaVersion    = "1"
	tokenPrefix      = "kwt_"
)

// Rados is the identity backend on a RADOS meta pool: one object per
// record, exclusive create for uniqueness, omap listings. It is the
// development and test backend; production uses the MariaDB backend.
type Rados struct {
	namespaces rados.IOContext
	domains    rados.IOContext
	mailboxes  rados.IOContext
	accounts   rados.IOContext
	names      rados.IOContext
	aliases    rados.IOContext
	tokens     rados.IOContext
	storage    rados.IOContext
	nodes      rados.IOContext
	Now        func() time.Time
}

// SetNow installs the clock (the Store wrapper forwards its own).
func (s *Rados) SetNow(now func() time.Time) { s.Now = now }

// NewRados returns the RADOS backend on the given meta pool context.
func NewRados(meta rados.IOContext) *Rados {
	return &Rados{
		namespaces: meta.Namespace(nsNamespaces),
		domains:    meta.Namespace(nsDomains),
		mailboxes:  meta.Namespace(nsMailboxes),
		accounts:   meta.Namespace(nsAccounts),
		names:      meta.Namespace(nsNames),
		aliases:    meta.Namespace(nsAliases),
		tokens:     meta.Namespace(nsTokens),
		storage:    meta.Namespace(nsStorage),
		nodes:      meta.Namespace(nsNodes),
		Now:        time.Now,
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

// NormalizeNamespace lowercases and validates a namespace name: a DNS
// label (letters, digits and hyphens, 1 to 63 characters, no hyphen at
// either end). Empty means the default namespace.
func NormalizeNamespace(name string) (string, error) {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return DefaultNamespace, nil
	}
	if len(name) > 63 || name[0] == '-' || name[len(name)-1] == '-' {
		return "", fmt.Errorf("%w: namespace %q", ErrInvalid, name)
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return "", fmt.Errorf("%w: namespace %q", ErrInvalid, name)
		}
	}
	return name, nil
}

// NormalizeDomain lowercases and validates a domain name.
func NormalizeDomain(d string) (string, error) {
	d = strings.TrimSpace(strings.ToLower(d))
	if d == "" || strings.ContainsAny(d, " @/\\\t\r\n\x00") || strings.HasPrefix(d, ".") || strings.HasSuffix(d, ".") {
		return "", fmt.Errorf("%w: domain %q", ErrInvalid, d)
	}
	return d, nil
}

// validateServices checks the service list; nil means "all known" and
// is expanded by the caller at creation.
func validateServices(services []string) error {
	seen := map[string]bool{}
	for _, x := range services {
		if !KnownServices[x] {
			return fmt.Errorf("%w: service %q", ErrInvalid, x)
		}
		if seen[x] {
			return fmt.Errorf("%w: duplicate service %q", ErrInvalid, x)
		}
		seen[x] = true
	}
	return nil
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

// ---- Namespaces ----

const allNamespacesObject = "_all"

// ensureDefault creates the default namespace on first use: the memory
// backend has no migration to seed it.
func (s *Rados) ensureDefault(ctx context.Context) error {
	if _, err := getJSON[Namespace](ctx, s.namespaces, DefaultNamespace); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	now := s.Now().UTC()
	ns := Namespace{Name: DefaultNamespace, Enabled: true, Created: now, Updated: now}
	if _, err := createJSON(ctx, s.namespaces, DefaultNamespace, ns); err != nil && !errors.Is(err, ErrExists) {
		return err
	}
	_, err := s.namespaces.Write(ctx, allNamespacesObject, rados.NewWriteTx().OmapSet(map[string][]byte{DefaultNamespace: nil}))
	return mapErr(err)
}

// requireNamespace normalizes a namespace name and checks that it exists.
func (s *Rados) requireNamespace(ctx context.Context, name string) (string, error) {
	name, err := NormalizeNamespace(name)
	if err != nil {
		return "", err
	}
	if name == DefaultNamespace {
		return name, s.ensureDefault(ctx)
	}
	if _, err := getJSON[Namespace](ctx, s.namespaces, name); err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", fmt.Errorf("%w: %s", ErrNamespaceUnknown, name)
		}
		return "", err
	}
	return name, nil
}

// CreateNamespace creates a namespace.
func (s *Rados) CreateNamespace(ctx context.Context, ns Namespace) (Namespace, error) {
	name, err := NormalizeNamespace(ns.Name)
	if err != nil {
		return Namespace{}, err
	}
	if name == DefaultNamespace {
		return Namespace{}, fmt.Errorf("%w: namespace %s", ErrExists, name)
	}
	ns.Name = name
	now := s.Now().UTC()
	ns.Created, ns.Updated = now, now
	if _, err := createJSON(ctx, s.namespaces, name, ns); err != nil {
		return Namespace{}, err
	}
	if _, err := s.namespaces.Write(ctx, allNamespacesObject, rados.NewWriteTx().OmapSet(map[string][]byte{name: nil})); err != nil {
		return Namespace{}, mapErr(err)
	}
	return ns, nil
}

// GetNamespace returns a namespace.
func (s *Rados) GetNamespace(ctx context.Context, name string) (Versioned[Namespace], error) {
	name, err := NormalizeNamespace(name)
	if err != nil {
		return Versioned[Namespace]{}, err
	}
	if name == DefaultNamespace {
		if err := s.ensureDefault(ctx); err != nil {
			return Versioned[Namespace]{}, err
		}
	}
	return getJSON[Namespace](ctx, s.namespaces, name)
}

// UpdateNamespace applies fn (the name is immutable), retrying on conflict.
func (s *Rados) UpdateNamespace(ctx context.Context, name string, fn func(*Namespace) error) (Namespace, error) {
	name, err := NormalizeNamespace(name)
	if err != nil {
		return Namespace{}, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := s.GetNamespace(ctx, name)
		if err != nil {
			return Namespace{}, err
		}
		ns := cur.Value
		if err := fn(&ns); err != nil {
			return Namespace{}, err
		}
		if ns.Name != name {
			return Namespace{}, fmt.Errorf("%w: namespace names are immutable", ErrInvalid)
		}
		ns.Updated = s.Now().UTC()
		if _, err := updateJSON(ctx, s.namespaces, name, cur.Version, ns); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Namespace{}, err
		}
		return ns, nil
	}
	return Namespace{}, ErrConflict
}

// DeleteNamespace removes an empty namespace: no domains, mailboxes or
// storage. The default namespace cannot be removed.
func (s *Rados) DeleteNamespace(ctx context.Context, name string) error {
	name, err := NormalizeNamespace(name)
	if err != nil {
		return err
	}
	if name == DefaultNamespace {
		return fmt.Errorf("%w: the default namespace cannot be removed", ErrInUse)
	}
	if _, err := getJSON[Namespace](ctx, s.namespaces, name); err != nil {
		return err
	}
	kv, _, err := listOmap(ctx, s.domains, allDomainsObject, "", "", 0)
	if err != nil {
		return err
	}
	for _, e := range kv {
		if string(e.Value) == name {
			return fmt.Errorf("%w: namespace %s has domains", ErrInUse, name)
		}
	}
	stor, err := s.ListStorage(ctx)
	if err != nil {
		return err
	}
	for _, c := range stor {
		if c.Namespace == name {
			return fmt.Errorf("%w: namespace %s has storage", ErrInUse, name)
		}
	}
	inUse := errors.New("in use")
	err = s.ListMailboxes(ctx, name, func(uuid.UUID) error { return inUse })
	if errors.Is(err, inUse) {
		return fmt.Errorf("%w: namespace %s has mailboxes", ErrInUse, name)
	}
	if err != nil {
		return err
	}
	if _, err := s.namespaces.Write(ctx, allNamespacesObject, rados.NewWriteTx().OmapRemove(name)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	return mapErr(s.namespaces.Delete(ctx, name))
}

// ListNamespaces lists namespace names after the cursor.
func (s *Rados) ListNamespaces(ctx context.Context, after string, max int) (names []string, more bool, err error) {
	if err := s.ensureDefault(ctx); err != nil {
		return nil, false, err
	}
	kv, more, err := listOmap(ctx, s.namespaces, allNamespacesObject, "", after, max)
	if err != nil {
		return nil, false, err
	}
	for _, e := range kv {
		names = append(names, e.Key)
	}
	return names, more, nil
}

// ---- Domains ----

// CreateDomain creates a domain in its namespace (default when empty).
func (s *Rados) CreateDomain(ctx context.Context, d Domain) (Domain, error) {
	name, err := NormalizeDomain(d.Name)
	if err != nil {
		return Domain{}, err
	}
	ns, err := s.requireNamespace(ctx, d.Namespace)
	if err != nil {
		return Domain{}, err
	}
	d.Name, d.Namespace = name, ns
	now := s.Now().UTC()
	d.Created, d.Updated = now, now
	if _, err := createJSON(ctx, s.domains, name, d); err != nil {
		return Domain{}, err
	}
	// Listing (repairable by reconcile); the value is the namespace so a
	// filtered listing needs no reads.
	if _, err := s.domains.Write(ctx, allDomainsObject, rados.NewWriteTx().OmapSet(map[string][]byte{name: []byte(ns)})); err != nil {
		return Domain{}, mapErr(err)
	}
	return d, nil
}

// GetDomain returns a domain.
func (s *Rados) GetDomain(ctx context.Context, name string) (Versioned[Domain], error) {
	name, err := NormalizeDomain(name)
	if err != nil {
		return Versioned[Domain]{}, err
	}
	return getJSON[Domain](ctx, s.domains, name)
}

// UpdateDomain applies fn to the domain and saves it, retrying on conflict.
func (s *Rados) UpdateDomain(ctx context.Context, name string, fn func(*Domain) error) (Domain, error) {
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
		d.Name, d.Namespace = name, cur.Value.Namespace
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
func (s *Rados) DeleteDomain(ctx context.Context, name string) error {
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

// ListDomains lists domain names after the cursor, of one namespace or
// ("") of all.
func (s *Rados) ListDomains(ctx context.Context, namespace, after string, max int) (names []string, more bool, err error) {
	if namespace == "" {
		kv, more, err := listOmap(ctx, s.domains, allDomainsObject, "", after, max)
		if err != nil {
			return nil, false, err
		}
		for _, e := range kv {
			names = append(names, e.Key)
		}
		return names, more, nil
	}
	ns, err := NormalizeNamespace(namespace)
	if err != nil {
		return nil, false, err
	}
	if max <= 0 {
		max = 1000
	}
	// Development backend: scan the listing and keep the namespace's rows.
	kv, _, err := listOmap(ctx, s.domains, allDomainsObject, "", after, 0)
	if err != nil {
		return nil, false, err
	}
	for _, e := range kv {
		if string(e.Value) != ns {
			continue
		}
		if len(names) == max {
			return names, true, nil
		}
		names = append(names, e.Key)
	}
	return names, false, nil
}

// ---- Mailboxes ----

// CreateMailbox creates a mailbox. A zero ID is replaced by a new UUID.
func (s *Rados) CreateMailbox(ctx context.Context, m Mailbox) (Mailbox, error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	now := s.Now().UTC()
	m.Created, m.Updated = now, now
	m.Hold = nil
	if m.State == "" {
		m.State = StateActive
	}
	if !ValidStates[m.State] {
		return Mailbox{}, fmt.Errorf("%w: mailbox state %q", ErrInvalid, m.State)
	}
	m.StateSince = now
	ns, err := s.requireNamespace(ctx, m.Namespace)
	if err != nil {
		return Mailbox{}, err
	}
	m.Namespace = ns
	if m.StorageID != 0 {
		c, err := s.GetStorage(ctx, m.StorageID)
		if err != nil {
			return Mailbox{}, fmt.Errorf("storage %d: %w", m.StorageID, err)
		}
		if c.Namespace != ns {
			return Mailbox{}, fmt.Errorf("%w: storage %d belongs to namespace %s, mailbox to %s", ErrNamespaceMismatch, m.StorageID, c.Namespace, ns)
		}
	}
	if _, err := createJSON(ctx, s.mailboxes, m.ID.String(), m); err != nil {
		return Mailbox{}, err
	}
	return m, nil
}

// GetMailbox returns a mailbox.
func (s *Rados) GetMailbox(ctx context.Context, id uuid.UUID) (Versioned[Mailbox], error) {
	return getJSON[Mailbox](ctx, s.mailboxes, id.String())
}

// UpdateMailbox applies fn and saves, retrying on conflict.
func (s *Rados) UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*Mailbox) error) (Mailbox, error) {
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := getJSON[Mailbox](ctx, s.mailboxes, id.String())
		if err != nil {
			return Mailbox{}, err
		}
		m := cur.Value
		if err := fn(&m); err != nil {
			return Mailbox{}, err
		}
		m.ID, m.Namespace = id, cur.Value.Namespace
		m.Updated = s.Now().UTC()
		if m.State == "" {
			m.State = StateActive
		}
		if !ValidStates[m.State] {
			return Mailbox{}, fmt.Errorf("%w: mailbox state %q", ErrInvalid, m.State)
		}
		if m.State != cur.Value.State {
			m.StateSince = m.Updated
		}
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
func (s *Rados) DeleteMailbox(ctx context.Context, id uuid.UUID) error {
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

// ListMailboxes lists mailbox ids (object listing; unordered, no cursor),
// of one namespace or ("") of all. The namespace filter reads every
// record: development backend only.
func (s *Rados) ListMailboxes(ctx context.Context, namespace string, fn func(id uuid.UUID) error) error {
	ns := ""
	if namespace != "" {
		var err error
		if ns, err = NormalizeNamespace(namespace); err != nil {
			return err
		}
	}
	return s.mailboxes.List(ctx, func(oid string) error {
		id, err := uuid.Parse(oid)
		if err != nil {
			return nil // ignore foreign objects
		}
		if ns != "" {
			v, err := getJSON[Mailbox](ctx, s.mailboxes, oid)
			if err != nil || v.Value.Namespace != ns {
				return nil
			}
		}
		return fn(id)
	})
}

// ListMailboxAccounts lists the usernames of accounts linked to the mailbox.
func (s *Rados) ListMailboxAccounts(ctx context.Context, id uuid.UUID, after string, max int) (usernames []string, more bool, err error) {
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
func (s *Rados) CreateAccount(ctx context.Context, a Account) (Account, error) {
	username, domain, err := NormalizeAddress(a.Username)
	if err != nil {
		return Account{}, err
	}
	if err := validateRoles(a.Roles); err != nil {
		return Account{}, err
	}
	if a.Services == nil {
		a.Services = AllServices()
	}
	if err := validateServices(a.Services); err != nil {
		return Account{}, err
	}
	a.Username, a.Domain = username, domain
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if a.MailboxID == uuid.Nil {
		return Account{}, fmt.Errorf("%w: account needs a mailbox", ErrInvalid)
	}
	mb, err := s.GetMailbox(ctx, a.MailboxID)
	if err != nil {
		return Account{}, fmt.Errorf("mailbox %s: %w", a.MailboxID, err)
	}
	dom, err := s.GetDomain(ctx, domain)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Account{}, fmt.Errorf("%w: %s", ErrDomainUnknown, domain)
		}
		return Account{}, err
	}
	if mb.Value.Namespace != dom.Value.Namespace {
		return Account{}, fmt.Errorf("%w: mailbox %s is in namespace %s, domain %s in %s", ErrNamespaceMismatch, a.MailboxID, mb.Value.Namespace, domain, dom.Value.Namespace)
	}
	a.Namespace = dom.Value.Namespace
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
func (s *Rados) GetAccount(ctx context.Context, id uuid.UUID) (Versioned[Account], error) {
	return getJSON[Account](ctx, s.accounts, id.String())
}

// GetAccountByUsername resolves a username to its account.
func (s *Rados) GetAccountByUsername(ctx context.Context, username string) (Versioned[Account], error) {
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
func (s *Rados) UpdateAccount(ctx context.Context, id uuid.UUID, fn func(*Account) error) (Account, error) {
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
		if a.Services == nil {
			a.Services = []string{}
		}
		if err := validateServices(a.Services); err != nil {
			return Account{}, err
		}
		a.ID, a.Namespace = id, cur.Value.Namespace
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
func (s *Rados) RelinkAccount(ctx context.Context, id, mailboxID uuid.UUID) (Account, error) {
	mb, err := s.GetMailbox(ctx, mailboxID)
	if err != nil {
		return Account{}, fmt.Errorf("mailbox %s: %w", mailboxID, err)
	}
	cur, err := getJSON[Account](ctx, s.accounts, id.String())
	if err != nil {
		return Account{}, err
	}
	if mb.Value.Namespace != cur.Value.Namespace {
		return Account{}, fmt.Errorf("%w: mailbox %s is in namespace %s, account in %s", ErrNamespaceMismatch, mailboxID, mb.Value.Namespace, cur.Value.Namespace)
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
func (s *Rados) DeleteAccount(ctx context.Context, id uuid.UUID) error {
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
func (s *Rados) ListDomainAccounts(ctx context.Context, domain, prefix, after string, max int) (usernames []string, more bool, err error) {
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
func (s *Rados) CreateAlias(ctx context.Context, al Alias) (Alias, error) {
	address, domain, err := normalizeAliasAddress(al.Address)
	if err != nil {
		return Alias{}, err
	}
	dom, err := s.GetDomain(ctx, domain)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Alias{}, fmt.Errorf("%w: %s", ErrDomainUnknown, domain)
		}
		return Alias{}, err
	}
	if err := s.checkDestinations(ctx, address, dom.Value.Namespace, al.Destinations); err != nil {
		return Alias{}, err
	}
	if !strings.HasPrefix(address, "@") {
		if _, _, err := s.names.Get(ctx, address); err == nil {
			return Alias{}, fmt.Errorf("%w: %s is an account", ErrExists, address)
		}
	}
	al.Address, al.Domain, al.Namespace = address, domain, dom.Value.Namespace
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
	if err := s.setAddressAliasListings(ctx, address, nil, al.Destinations); err != nil {
		return Alias{}, err
	}
	return al, nil
}

// checkDestinations validates and normalizes alias destinations: exactly
// one of mailbox or address each, no self reference, mailbox destinations
// existing and in the alias domain's namespace.
func (s *Rados) checkDestinations(ctx context.Context, address, namespace string, dsts []Destination) error {
	if len(dsts) == 0 {
		return fmt.Errorf("%w: alias needs at least one destination", ErrInvalid)
	}
	for i, d := range dsts {
		if (d.MailboxID == uuid.Nil) == (d.Address == "") {
			return fmt.Errorf("%w: destination must be exactly one of mailbox or address", ErrInvalid)
		}
		if d.Address != "" {
			na, _, err := NormalizeAddress(d.Address)
			if err != nil {
				return err
			}
			if na == address {
				return fmt.Errorf("%w: alias points at itself", ErrInvalid)
			}
			dsts[i].Address = na
			continue
		}
		mb, err := s.GetMailbox(ctx, d.MailboxID)
		if err != nil {
			return fmt.Errorf("mailbox %s: %w", d.MailboxID, err)
		}
		if mb.Value.Namespace != namespace {
			return fmt.Errorf("%w: mailbox %s is in namespace %s, alias domain in %s", ErrNamespaceMismatch, d.MailboxID, mb.Value.Namespace, namespace)
		}
	}
	return nil
}

// aliasToKey is the reverse listing of an address destination, kept on the
// destination's domain object: `aliasto/<destination>/<alias>`. A domain
// never contains "/", so the destination prefix is unambiguous.
func aliasToKey(dest, alias string) string { return "aliasto/" + dest + "/" + alias }

// setAddressAliasListings maintains `aliasto/` on the domain objects of the
// alias's address destinations. Destinations outside a hosted domain have
// no domain object and are not listed.
func (s *Rados) setAddressAliasListings(ctx context.Context, address string, old, cur []Destination) error {
	addrs := func(ds []Destination) map[string]string {
		out := map[string]string{}
		for _, d := range ds {
			if d.Address == "" {
				continue
			}
			if na, dom, err := NormalizeAddress(d.Address); err == nil {
				out[na] = dom
			}
		}
		return out
	}
	was, now := addrs(old), addrs(cur)
	for dest, dom := range now {
		if _, err := s.domains.Write(ctx, dom, rados.NewWriteTx().AssertExists().OmapSet(map[string][]byte{aliasToKey(dest, address): nil})); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	for dest, dom := range was {
		if _, ok := now[dest]; ok {
			continue
		}
		if _, err := s.domains.Write(ctx, dom, rados.NewWriteTx().OmapRemove(aliasToKey(dest, address))); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return mapErr(err)
		}
	}
	return nil
}

// ListAliasesTo lists the aliases with address as a destination address.
func (s *Rados) ListAliasesTo(ctx context.Context, address string) ([]string, error) {
	na, dom, err := NormalizeAddress(address)
	if err != nil {
		return nil, err
	}
	kv, _, err := listOmap(ctx, s.domains, dom, aliasToKey(na, ""), "", 0)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(kv))
	for _, e := range kv {
		out = append(out, e.Key)
	}
	return out, nil
}

// setMailboxAliasListings maintains the reverse listing `alias/<address>` on
// every destination mailbox (used for sender identities).
func (s *Rados) setMailboxAliasListings(ctx context.Context, address string, old, cur []Destination) error {
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
func (s *Rados) ListMailboxAliases(ctx context.Context, id uuid.UUID) ([]string, error) {
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
func (s *Rados) GetAlias(ctx context.Context, address string) (Versioned[Alias], error) {
	address, _, err := normalizeAliasAddress(address)
	if err != nil {
		return Versioned[Alias]{}, err
	}
	return getJSON[Alias](ctx, s.aliases, address)
}

// UpdateAlias applies fn and saves, retrying on conflict.
func (s *Rados) UpdateAlias(ctx context.Context, address string, fn func(*Alias) error) (Alias, error) {
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
		if err := s.checkDestinations(ctx, address, cur.Value.Namespace, al.Destinations); err != nil {
			return Alias{}, err
		}
		al.Address, al.Domain, al.Namespace = address, domain, cur.Value.Namespace
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
		if err := s.setAddressAliasListings(ctx, address, cur.Value.Destinations, al.Destinations); err != nil {
			return Alias{}, err
		}
		return al, nil
	}
	return Alias{}, ErrConflict
}

// DeleteAlias removes an alias.
func (s *Rados) DeleteAlias(ctx context.Context, address string) error {
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
	if err := s.setAddressAliasListings(ctx, address, cur.Value.Destinations, nil); err != nil {
		return err
	}
	return mapErr(s.aliases.Delete(ctx, address))
}

// ListDomainAliases lists alias addresses in a domain.
func (s *Rados) ListDomainAliases(ctx context.Context, domain, after string, max int) (addresses []string, more bool, err error) {
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
func (s *Rados) CreateToken(ctx context.Context, accountID uuid.UUID, roles []string, expiresAt time.Time, comment string) (Token, string, error) {
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
func (s *Rados) CreateAdminToken(ctx context.Context, role, namespace string, expiresAt time.Time, comment string) (Token, string, error) {
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
	if strings.TrimSpace(namespace) != "" {
		var err error
		if namespace, err = s.requireNamespace(ctx, namespace); err != nil {
			return Token{}, "", err
		}
	} else {
		namespace = ""
	}
	secret, err := NewTokenSecret()
	if err != nil {
		return Token{}, "", err
	}
	t := Token{ID: uuid.NewString(), Hash: HashToken(secret), Namespace: namespace, Roles: []string{role}, ExpiresAt: expiresAt.UTC(), Comment: comment, Created: now}
	if _, err := createJSON(ctx, s.tokens, t.Hash, t); err != nil {
		return Token{}, "", err
	}
	if _, err := s.tokens.Write(ctx, adminTokensObject, rados.NewWriteTx().OmapSet(map[string][]byte{"token/" + t.ID: []byte(t.Hash)})); err != nil {
		return Token{}, "", mapErr(err)
	}
	return t, secret, nil
}

// ListAdminTokens lists admin tokens (without secrets).
func (s *Rados) ListAdminTokens(ctx context.Context) ([]Token, error) {
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
func (s *Rados) DeleteAdminToken(ctx context.Context, tokenID string) error {
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
func (s *Rados) LookupToken(ctx context.Context, secret string) (Versioned[Token], error) {
	if !strings.HasPrefix(secret, tokenPrefix) {
		return Versioned[Token]{}, ErrNotFound
	}
	return getJSON[Token](ctx, s.tokens, HashToken(secret))
}

// TouchToken records last use. Best effort; conflicts are ignored.
func (s *Rados) TouchToken(ctx context.Context, t Versioned[Token]) {
	t.Value.LastUsed = s.Now().UTC()
	_, _ = updateJSON(ctx, s.tokens, t.Value.Hash, t.Version, t.Value)
}

// ListTokens lists an account's tokens.
func (s *Rados) ListTokens(ctx context.Context, accountID uuid.UUID) ([]Token, error) {
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
func (s *Rados) DeleteToken(ctx context.Context, accountID uuid.UUID, tokenID string) error {
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

// ---- storage registry ----
//
// storage/<id> holds the record; storage/_seq holds the last id (OCC);
// storage/_all lists ids in omap so listing is one read.

const (
	storageSeqOID = "_seq"
	storageAllOID = "_all"
)

func storageOID(id int64) string { return strconv.FormatInt(id, 10) }

func (s *Rados) nextStorageID(ctx context.Context) (int64, error) {
	for attempt := 0; attempt < 10; attempt++ {
		data, ver, err := s.storage.Get(ctx, storageSeqOID)
		if errors.Is(err, rados.ErrNotFound) {
			if _, err := s.storage.Write(ctx, storageSeqOID, rados.NewWriteTx().CreateExclusive().WriteFull([]byte("1"))); err == nil {
				return 1, nil
			} else if !errors.Is(err, rados.ErrExists) {
				return 0, mapErr(err)
			}
			continue
		}
		if err != nil {
			return 0, mapErr(err)
		}
		last, _ := strconv.ParseInt(string(data), 10, 64)
		next := last + 1
		if _, err := s.storage.Write(ctx, storageSeqOID, rados.NewWriteTx().AssertVersion(ver).WriteFull([]byte(strconv.FormatInt(next, 10)))); err != nil {
			if errors.Is(err, rados.ErrVersionMismatch) {
				continue
			}
			return 0, mapErr(err)
		}
		return next, nil
	}
	return 0, ErrConflict
}

func validateStorage(c *Storage) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" || len(c.Name) > 64 {
		return fmt.Errorf("%w: storage name", ErrInvalid)
	}
	ns, err := NormalizeNamespace(c.Namespace)
	if err != nil {
		return err
	}
	c.Namespace = ns
	if strings.TrimSpace(c.MonHost) == "" {
		return fmt.Errorf("%w: storage mon_host", ErrInvalid)
	}
	if c.Pools.Messages == "" || c.Pools.Attachments == "" || c.Pools.Index == "" {
		return fmt.Errorf("%w: storage needs messages, attachments and index pools", ErrInvalid)
	}
	return nil
}

func (s *Rados) CreateStorage(ctx context.Context, c Storage) (Storage, error) {
	if err := validateStorage(&c); err != nil {
		return Storage{}, err
	}
	if _, err := s.requireNamespace(ctx, c.Namespace); err != nil {
		return Storage{}, err
	}
	all, err := s.ListStorage(ctx)
	if err != nil {
		return Storage{}, err
	}
	for _, x := range all {
		if x.Name == c.Name {
			return Storage{}, fmt.Errorf("%w: storage %s", ErrExists, c.Name)
		}
		if err := checkPoolsDistinct(x, c); err != nil {
			return Storage{}, err
		}
	}
	id, err := s.nextStorageID(ctx)
	if err != nil {
		return Storage{}, err
	}
	c.ID = id
	now := s.Now().UTC()
	c.Created, c.Updated = now, now
	if _, err := createJSON(ctx, s.storage, storageOID(id), c); err != nil {
		return Storage{}, err
	}
	if _, err := s.storage.Write(ctx, storageAllOID, rados.NewWriteTx().OmapSet(map[string][]byte{storageOID(id): []byte(c.Name)})); err != nil {
		return Storage{}, mapErr(err)
	}
	return c, nil
}

func (s *Rados) GetStorage(ctx context.Context, id int64) (Storage, error) {
	v, err := getJSON[Storage](ctx, s.storage, storageOID(id))
	return v.Value, err
}

func (s *Rados) ListStorage(ctx context.Context) ([]Storage, error) {
	kv, _, err := s.storage.OmapList(ctx, storageAllOID, "", "", 0)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, nil
		}
		return nil, mapErr(err)
	}
	var out []Storage
	for _, e := range kv {
		id, _ := strconv.ParseInt(e.Key, 10, 64)
		c, err := s.GetStorage(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Rados) UpdateStorage(ctx context.Context, id int64, fn func(*Storage) error) (Storage, error) {
	for attempt := 0; attempt < 5; attempt++ {
		cur, err := getJSON[Storage](ctx, s.storage, storageOID(id))
		if err != nil {
			return Storage{}, err
		}
		c := cur.Value
		if err := fn(&c); err != nil {
			return Storage{}, err
		}
		if err := validateStorage(&c); err != nil {
			return Storage{}, err
		}
		if c.Namespace != cur.Value.Namespace {
			return Storage{}, fmt.Errorf("%w: a storage cannot move to another namespace", ErrInvalid)
		}
		all, err := s.ListStorage(ctx)
		if err != nil {
			return Storage{}, err
		}
		for _, x := range all {
			if x.ID == id {
				continue
			}
			if x.Name == c.Name {
				return Storage{}, fmt.Errorf("%w: storage %s", ErrExists, c.Name)
			}
			if err := checkPoolsDistinct(x, c); err != nil {
				return Storage{}, err
			}
		}
		c.ID = id
		c.Updated = s.Now().UTC()
		if _, err := updateJSON(ctx, s.storage, storageOID(id), cur.Version, c); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return Storage{}, err
		}
		_, _ = s.storage.Write(ctx, storageAllOID, rados.NewWriteTx().OmapSet(map[string][]byte{storageOID(id): []byte(c.Name)}))
		return c, nil
	}
	return Storage{}, ErrConflict
}

// checkPoolsDistinct refuses two storages on one cluster (same fsid)
// that share a pool: a pool belongs to exactly one storage, and so to one
// namespace. Two storages may share a cluster.
func checkPoolsDistinct(a, b Storage) error {
	if a.FSID == uuid.Nil || b.FSID == uuid.Nil || a.FSID != b.FSID {
		return nil
	}
	for _, p := range [][2]string{{a.Pools.Messages, b.Pools.Messages}, {a.Pools.Attachments, b.Pools.Attachments}, {a.Pools.Index, b.Pools.Index}} {
		if p[0] == p[1] {
			return fmt.Errorf("%w: storage %s already uses pool %s on cluster %s", ErrExists, a.Name, p[0], a.FSID)
		}
	}
	return nil
}

// DeleteStorage removes a storage no mailbox refers to.
func (s *Rados) DeleteStorage(ctx context.Context, id int64) error {
	if _, err := s.GetStorage(ctx, id); err != nil {
		return err
	}
	inUse := errors.New("in use")
	err := s.ListMailboxes(ctx, "", func(mb uuid.UUID) error {
		v, err := s.GetMailbox(ctx, mb)
		if err == nil && v.Value.StorageID == id {
			return inUse
		}
		return nil
	})
	if errors.Is(err, inUse) {
		return fmt.Errorf("%w: storage %d has mailboxes", ErrInUse, id)
	}
	if err != nil {
		return err
	}
	if _, err := s.storage.Write(ctx, storageAllOID, rados.NewWriteTx().OmapRemove(storageOID(id))); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return mapErr(err)
	}
	return mapErr(s.storage.Delete(ctx, storageOID(id)))
}

// Exported validation helpers shared with other backends.

// TokenPrefix marks API token secrets.
const TokenPrefix = tokenPrefix

// NormalizeAliasAddress normalizes an alias address ("@domain" for a
// catch-all) and returns it with its domain.
func NormalizeAliasAddress(addr string) (address, domain string, err error) {
	return normalizeAliasAddress(addr)
}

// ValidateRoles checks an account role list.
func ValidateRoles(roles []string) error { return validateRoles(roles) }

// ValidateServices checks an account service list.
func ValidateServices(services []string) error { return validateServices(services) }

// ValidateStorage checks and trims a storage record.
func ValidateStorage(c *Storage) error { return validateStorage(c) }

// ---- node registry (one object per mode/address, listing via List) ----

func nodeOID(mode, address string) string { return mode + "/" + address }

func validateNode(n *Node) error {
	n.Mode, n.Address, n.Host = strings.TrimSpace(n.Mode), strings.TrimSpace(n.Address), strings.TrimSpace(n.Host)
	if n.Mode == "" || n.Address == "" {
		return fmt.Errorf("%w: node needs mode and address", ErrInvalid)
	}
	nss, err := NormalizeNamespaces(n.Namespaces)
	if err != nil {
		return err
	}
	n.Namespaces = nss
	return nil
}

// NormalizeNamespaces normalizes a served-namespace list: names
// validated, lowercased, deduplicated and sorted; nil stays nil (all).
func NormalizeNamespaces(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	var out []string
	for _, n := range in {
		ns, err := NormalizeNamespace(n)
		if err != nil {
			return nil, err
		}
		if !seen[ns] {
			seen[ns] = true
			out = append(out, ns)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Rados) RegisterNode(ctx context.Context, n Node) (Node, error) {
	if err := validateNode(&n); err != nil {
		return Node{}, err
	}
	now := s.Now().UTC()
	n.LastSeen = now
	if n.Started.IsZero() {
		n.Started = now
	}
	if cur, err := getJSON[Node](ctx, s.nodes, nodeOID(n.Mode, n.Address)); err == nil {
		n.ID = cur.Value.ID
		if cur.Value.Started.Before(n.Started) && !cur.Value.Started.IsZero() && n.Started.Sub(cur.Value.Started) < time.Second {
			n.Started = cur.Value.Started
		}
	} else {
		id, err := s.nextStorageID(ctx) // shared sequence; ids only need to be unique
		if err != nil {
			return Node{}, err
		}
		n.ID = id
	}
	b, _ := json.Marshal(&n)
	if _, err := s.nodes.Write(ctx, nodeOID(n.Mode, n.Address), rados.NewWriteTx().WriteFull(b)); err != nil {
		return Node{}, mapErr(err)
	}
	return n, nil
}

func (s *Rados) ListNodes(ctx context.Context, mode string) ([]Node, error) {
	var out []Node
	err := s.nodes.List(ctx, func(oid string) error {
		if mode != "" && !strings.HasPrefix(oid, mode+"/") {
			return nil
		}
		v, err := getJSON[Node](ctx, s.nodes, oid)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		out = append(out, v.Value)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Mode != out[j].Mode {
			return out[i].Mode < out[j].Mode
		}
		return out[i].Address < out[j].Address
	})
	return out, err
}

func (s *Rados) DeleteNode(ctx context.Context, mode, address string) error {
	err := s.nodes.Delete(ctx, nodeOID(mode, address))
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return mapErr(err)
}

func (s *Rados) PurgeNodes(ctx context.Context, cutoff time.Time) (int, error) {
	all, err := s.ListNodes(ctx, "")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, x := range all {
		if x.Expires.Before(cutoff) {
			if err := s.DeleteNode(ctx, x.Mode, x.Address); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}
