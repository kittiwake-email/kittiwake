// Package admin is the platform administration service: identity CRUD
// (domains, mailboxes, accounts, aliases, tokens), legal hold, mailbox
// inspection and removal, bootstrap. Every call requires an actor with the
// admin role (or the CLI's system actor) and is audited. The REST admin API
// and the `dovetail admin` CLI are thin adapters over this package.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	cryptoauth "dovetail/auth"
	"dovetail/service/actor"
	"dovetail/service/auditor"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
	"dovetail/store/queue"
)

// Errors.
var (
	ErrForbidden = errors.New("admin: admin role required")
	ErrNotFound  = identity.ErrNotFound
	ErrExists    = identity.ErrExists
	ErrInvalid   = identity.ErrInvalid
	ErrInUse     = identity.ErrInUse
)

// Task types this service enqueues.
const (
	TaskRemoveMailbox = "remove-mailbox"
	TaskRecalcQuota   = "recalc-quota"
	TaskPurgeTrash    = "purge-trash"
)

// Service is the admin service.
type Service struct {
	ids   *identity.Store
	idx   *index.Store
	queue queue.Store
	audit *auditor.Auditor
	Now   func() time.Time
}

// New returns the service. queue may be nil (tasks are then refused).
func New(ids *identity.Store, idx *index.Store, q queue.Store, aud *auditor.Auditor) *Service {
	return &Service{ids: ids, idx: idx, queue: q, audit: aud, Now: time.Now}
}

// authorize guards mutating operations: full admin (or the CLI system actor).
func (s *Service) authorize(ctx context.Context, action, target string) error {
	a := actor.From(ctx)
	if a.IsAdmin() || (a.Protocol == audit.ProtoCLI && a.IsSystem()) {
		return nil
	}
	s.audit.Log(ctx, auditor.Event{Action: action, Target: target, Result: audit.ResultDenied, Detail: "admin role required"})
	return ErrForbidden
}

// authorizeRead guards read-only operations: full or read-only admin.
func (s *Service) authorizeRead(ctx context.Context, action, target string) error {
	a := actor.From(ctx)
	if a.IsAdminReader() || (a.Protocol == audit.ProtoCLI && a.IsSystem()) {
		return nil
	}
	s.audit.Log(ctx, auditor.Event{Action: action, Target: target, Result: audit.ResultDenied, Detail: "admin role required"})
	return ErrForbidden
}

func (s *Service) ok(ctx context.Context, mb uuid.UUID, action, target, detail string) {
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: action, Target: target, Detail: detail})
}

func (s *Service) fail(ctx context.Context, mb uuid.UUID, action, target string, err error) error {
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: action, Target: target, Detail: err.Error(), Result: audit.ResultError})
	return err
}

// ---- domains ----

// CreateDomain creates a domain.
func (s *Service) CreateDomain(ctx context.Context, d identity.Domain) (identity.Domain, error) {
	if err := s.authorize(ctx, "domain-create", d.Name); err != nil {
		return identity.Domain{}, err
	}
	out, err := s.ids.CreateDomain(ctx, d)
	if err != nil {
		return identity.Domain{}, s.fail(ctx, uuid.Nil, "domain-create", d.Name, err)
	}
	s.ok(ctx, uuid.Nil, "domain-create", out.Name, "")
	return out, nil
}

// GetDomain returns a domain.
func (s *Service) GetDomain(ctx context.Context, name string) (identity.Domain, error) {
	if err := s.authorizeRead(ctx, "domain-get", name); err != nil {
		return identity.Domain{}, err
	}
	v, err := s.ids.GetDomain(ctx, name)
	return v.Value, err
}

// UpdateDomain applies fn.
func (s *Service) UpdateDomain(ctx context.Context, name string, fn func(*identity.Domain) error) (identity.Domain, error) {
	if err := s.authorize(ctx, "domain-update", name); err != nil {
		return identity.Domain{}, err
	}
	out, err := s.ids.UpdateDomain(ctx, name, fn)
	if err != nil {
		return identity.Domain{}, s.fail(ctx, uuid.Nil, "domain-update", name, err)
	}
	s.ok(ctx, uuid.Nil, "domain-update", out.Name, fmt.Sprintf("enabled=%v", out.Enabled))
	return out, nil
}

// DeleteDomain deletes an empty domain.
func (s *Service) DeleteDomain(ctx context.Context, name string) error {
	if err := s.authorize(ctx, "domain-delete", name); err != nil {
		return err
	}
	if err := s.ids.DeleteDomain(ctx, name); err != nil {
		return s.fail(ctx, uuid.Nil, "domain-delete", name, err)
	}
	s.ok(ctx, uuid.Nil, "domain-delete", name, "")
	return nil
}

// ListDomains lists domains.
func (s *Service) ListDomains(ctx context.Context, after string, max int) ([]string, bool, error) {
	if err := s.authorizeRead(ctx, "domain-list", ""); err != nil {
		return nil, false, err
	}
	return s.ids.ListDomains(ctx, after, max)
}

// ---- mailboxes ----

// CreateMailbox creates a mailbox.
func (s *Service) CreateMailbox(ctx context.Context, m identity.Mailbox) (identity.Mailbox, error) {
	if err := s.authorize(ctx, "mailbox-create", ""); err != nil {
		return identity.Mailbox{}, err
	}
	out, err := s.ids.CreateMailbox(ctx, m)
	if err != nil {
		return identity.Mailbox{}, s.fail(ctx, uuid.Nil, "mailbox-create", "", err)
	}
	s.ok(ctx, out.ID, "mailbox-create", out.ID.String(), "")
	return out, nil
}

// GetMailbox returns a mailbox.
func (s *Service) GetMailbox(ctx context.Context, id uuid.UUID) (identity.Mailbox, error) {
	if err := s.authorizeRead(ctx, "mailbox-get", id.String()); err != nil {
		return identity.Mailbox{}, err
	}
	v, err := s.ids.GetMailbox(ctx, id)
	return v.Value, err
}

// UpdateMailbox changes the quota. Hold is managed by Hold/Release, the
// state by SetMailboxState.
func (s *Service) UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*identity.Mailbox) error) (identity.Mailbox, error) {
	if err := s.authorize(ctx, "mailbox-update", id.String()); err != nil {
		return identity.Mailbox{}, err
	}
	out, err := s.ids.UpdateMailbox(ctx, id, func(m *identity.Mailbox) error {
		hold, rel, st, reason, since := m.Hold, m.Released, m.State, m.StateReason, m.StateSince
		if err := fn(m); err != nil {
			return err
		}
		m.Hold, m.Released, m.State, m.StateReason, m.StateSince = hold, rel, st, reason, since
		return nil
	})
	if err != nil {
		return identity.Mailbox{}, s.fail(ctx, id, "mailbox-update", id.String(), err)
	}
	s.ok(ctx, id, "mailbox-update", id.String(), fmt.Sprintf("quota=%+v", out.Quota))
	return out, nil
}

// SetMailboxState activates, suspends (soft: LMTP 4xx, MTA queues) or
// disables (hard: LMTP 5xx) a mailbox. Suspended and disabled mailboxes
// refuse user access.
func (s *Service) SetMailboxState(ctx context.Context, id uuid.UUID, state, reason string) (identity.Mailbox, error) {
	if err := s.authorize(ctx, "mailbox-state", id.String()); err != nil {
		return identity.Mailbox{}, err
	}
	if !identity.ValidStates[state] {
		return identity.Mailbox{}, fmt.Errorf("%w: state %q (active, suspended or disabled)", ErrInvalid, state)
	}
	if state != identity.StateActive && strings.TrimSpace(reason) == "" {
		return identity.Mailbox{}, fmt.Errorf("%w: a reason is required to suspend or disable", ErrInvalid)
	}
	out, err := s.ids.UpdateMailbox(ctx, id, func(m *identity.Mailbox) error {
		m.State = state
		m.StateReason = strings.TrimSpace(reason)
		return nil
	})
	if err != nil {
		return identity.Mailbox{}, s.fail(ctx, id, "mailbox-state", id.String(), err)
	}
	s.ok(ctx, id, "mailbox-state", id.String(), fmt.Sprintf("state=%s reason=%q", state, reason))
	return out, nil
}

// MailboxInfo is the inspection view.
type MailboxInfo struct {
	Mailbox  identity.Mailbox     `json:"mailbox"`
	Accounts []string             `json:"accounts"`
	Indexed  bool                 `json:"indexed"`
	Usage    index.Usage          `json:"usage"`
	ModSeq   uint64               `json:"modseq"`
	Folders  []*index.Folder      `json:"folders,omitempty"`
	Hold     *identity.Hold       `json:"hold,omitempty"`
	Limits   identity.QuotaLimits `json:"limits"`
}

// InspectMailbox returns usage, folders and accounts.
func (s *Service) InspectMailbox(ctx context.Context, id uuid.UUID, defaults identity.QuotaLimits) (MailboxInfo, error) {
	if err := s.authorizeRead(ctx, "mailbox-inspect", id.String()); err != nil {
		return MailboxInfo{}, err
	}
	v, err := s.ids.GetMailbox(ctx, id)
	if err != nil {
		return MailboxInfo{}, err
	}
	info := MailboxInfo{Mailbox: v.Value, Hold: v.Value.Hold, Limits: v.Value.Quota}
	if info.Limits.SoftBytes == 0 {
		info.Limits.SoftBytes = defaults.SoftBytes
	}
	if info.Limits.HardBytes == 0 {
		info.Limits.HardBytes = defaults.HardBytes
	}
	if info.Limits.SoftMessages == 0 {
		info.Limits.SoftMessages = defaults.SoftMessages
	}
	if info.Limits.HardMessages == 0 {
		info.Limits.HardMessages = defaults.HardMessages
	}
	info.Accounts, _, err = s.ids.ListMailboxAccounts(ctx, id, "", 1000)
	if err != nil {
		return MailboxInfo{}, err
	}
	root, err := s.idx.GetRoot(ctx, id)
	if err == nil {
		info.Indexed = true
		info.Usage, info.ModSeq, info.Folders = root.Usage, root.ModSeq, root.Folders
	} else if !errors.Is(err, index.ErrNotFound) {
		return MailboxInfo{}, err
	}
	s.ok(ctx, id, "mailbox-inspect", id.String(), "")
	return info, nil
}

// ListMailboxAccounts lists the usernames linked to a mailbox.
func (s *Service) ListMailboxAccounts(ctx context.Context, id uuid.UUID) ([]string, bool, error) {
	if err := s.authorizeRead(ctx, "mailbox-accounts", id.String()); err != nil {
		return nil, false, err
	}
	return s.ids.ListMailboxAccounts(ctx, id, "", 1000)
}

// ListMailboxes walks all mailbox ids.
func (s *Service) ListMailboxes(ctx context.Context, fn func(uuid.UUID) error) error {
	if err := s.authorizeRead(ctx, "mailbox-list", ""); err != nil {
		return err
	}
	return s.ids.ListMailboxes(ctx, fn)
}

// DeleteMailbox removes the mailbox record (no accounts may link to it, no
// hold) and schedules removal of all its data by a worker.
func (s *Service) DeleteMailbox(ctx context.Context, id uuid.UUID) error {
	if err := s.authorize(ctx, "mailbox-delete", id.String()); err != nil {
		return err
	}
	if err := s.ids.DeleteMailbox(ctx, id); err != nil {
		return s.fail(ctx, id, "mailbox-delete", id.String(), err)
	}
	if s.queue != nil {
		if _, err := s.queue.Enqueue(ctx, TaskRemoveMailbox, map[string]any{"mailbox": id}, time.Time{}); err != nil {
			return s.fail(ctx, id, "mailbox-delete", id.String(), err)
		}
	}
	s.ok(ctx, id, "mailbox-delete", id.String(), "data removal scheduled")
	return nil
}

// RecalculateQuota rebuilds counters from the folder indexes, synchronously.
func (s *Service) RecalculateQuota(ctx context.Context, id uuid.UUID) (index.Usage, error) {
	if err := s.authorize(ctx, "quota-recalc", id.String()); err != nil {
		return index.Usage{}, err
	}
	root, err := s.idx.Recalculate(ctx, id)
	if err != nil {
		return index.Usage{}, s.fail(ctx, id, "quota-recalc", id.String(), err)
	}
	s.ok(ctx, id, "quota-recalc", id.String(), fmt.Sprintf("%+v", root.Usage))
	return root.Usage, nil
}

// ---- legal hold ----

// HoldRequest describes a hold.
type HoldRequest struct {
	Reason         string
	CaseRef        string
	AcceptDelivery *bool // default true
	AllowSend      bool
	Sealed         bool
}

// Hold places a mailbox on legal hold.
func (s *Service) Hold(ctx context.Context, id uuid.UUID, req HoldRequest) (identity.Mailbox, error) {
	if err := s.authorize(ctx, "hold", id.String()); err != nil {
		return identity.Mailbox{}, err
	}
	if strings.TrimSpace(req.Reason) == "" {
		return identity.Mailbox{}, fmt.Errorf("%w: hold reason required", ErrInvalid)
	}
	accept := true
	if req.AcceptDelivery != nil {
		accept = *req.AcceptDelivery
	}
	out, err := s.ids.UpdateMailbox(ctx, id, func(m *identity.Mailbox) error {
		if m.Hold != nil {
			return fmt.Errorf("%w: already on hold since %s", ErrExists, m.Hold.SetAt.Format(time.RFC3339))
		}
		m.Hold = &identity.Hold{
			Reason: req.Reason, CaseRef: req.CaseRef, SetBy: actor.From(ctx).Username, SetAt: s.Now().UTC(),
			AcceptDelivery: accept, AllowSend: req.AllowSend, Sealed: req.Sealed,
		}
		return nil
	})
	if err != nil {
		return identity.Mailbox{}, s.fail(ctx, id, "hold", id.String(), err)
	}
	s.ok(ctx, id, "hold", id.String(), fmt.Sprintf("reason=%q case=%q deliver=%v send=%v sealed=%v", req.Reason, req.CaseRef, accept, req.AllowSend, req.Sealed))
	return out, nil
}

// Release lifts a hold. Purges resume after the configured grace period
// (the worker reads Released).
func (s *Service) Release(ctx context.Context, id uuid.UUID, note string) (identity.Mailbox, error) {
	if err := s.authorize(ctx, "release", id.String()); err != nil {
		return identity.Mailbox{}, err
	}
	out, err := s.ids.UpdateMailbox(ctx, id, func(m *identity.Mailbox) error {
		if m.Hold == nil {
			return fmt.Errorf("%w: not on hold", ErrNotFound)
		}
		now := s.Now().UTC()
		m.Hold = nil
		m.Released = &now
		return nil
	})
	if err != nil {
		return identity.Mailbox{}, s.fail(ctx, id, "release", id.String(), err)
	}
	s.ok(ctx, id, "release", id.String(), note)
	return out, nil
}

// ListHolds returns the ids of mailboxes on hold.
func (s *Service) ListHolds(ctx context.Context) ([]identity.Mailbox, error) {
	if err := s.authorizeRead(ctx, "hold-list", ""); err != nil {
		return nil, err
	}
	var out []identity.Mailbox
	err := s.ids.ListMailboxes(ctx, func(id uuid.UUID) error {
		v, err := s.ids.GetMailbox(ctx, id)
		if err != nil {
			return nil
		}
		if v.Value.Hold != nil {
			out = append(out, v.Value)
		}
		return nil
	})
	return out, err
}

// ---- accounts ----

// CreateAccountRequest is the admin's view of a new account.
type CreateAccountRequest struct {
	Username  string
	Name      string // display name
	MailboxID uuid.UUID
	Roles     []string
	Services  []string // nil = every known service
	Password  string   // optional initial primary password (plaintext)
	// PasswordComment and PasswordExpiresAt describe the initial password
	// (comment defaults to "initial password"; expiry nil = never).
	PasswordComment   string
	PasswordExpiresAt *time.Time
	Enabled           *bool // default true
}

// CreateAccount creates an account.
func (s *Service) CreateAccount(ctx context.Context, req CreateAccountRequest) (identity.Account, error) {
	if err := s.authorize(ctx, "account-create", req.Username); err != nil {
		return identity.Account{}, err
	}
	a := identity.Account{Username: req.Username, Name: strings.TrimSpace(req.Name), MailboxID: req.MailboxID, Roles: req.Roles, Services: req.Services, Enabled: true}
	if req.Enabled != nil {
		a.Enabled = *req.Enabled
	}
	if req.Password != "" {
		if len(req.Password) < 8 {
			return identity.Account{}, fmt.Errorf("%w: password too short", ErrInvalid)
		}
		h, err := cryptoauth.HashPassword(req.Password)
		if err != nil {
			return identity.Account{}, err
		}
		comment := strings.TrimSpace(req.PasswordComment)
		if comment == "" {
			comment = "initial password"
		}
		if req.PasswordExpiresAt != nil && !req.PasswordExpiresAt.After(s.Now()) {
			return identity.Account{}, fmt.Errorf("%w: password expiry must be in the future", ErrInvalid)
		}
		a.Passwords = []identity.Password{{Hash: h, Kind: identity.PasswordPrimary, Comment: comment, ExpiresAt: req.PasswordExpiresAt}}
	}
	out, err := s.ids.CreateAccount(ctx, a)
	if err != nil {
		return identity.Account{}, s.fail(ctx, req.MailboxID, "account-create", req.Username, err)
	}
	s.ok(ctx, out.MailboxID, "account-create", out.Username, fmt.Sprintf("roles=%v", out.Roles))
	return out, nil
}

// GetAccount returns an account by id.
func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (identity.Account, error) {
	if err := s.authorizeRead(ctx, "account-get", id.String()); err != nil {
		return identity.Account{}, err
	}
	v, err := s.ids.GetAccount(ctx, id)
	return v.Value, err
}

// GetAccountByUsername returns an account by username.
func (s *Service) GetAccountByUsername(ctx context.Context, username string) (identity.Account, error) {
	if err := s.authorizeRead(ctx, "account-get", username); err != nil {
		return identity.Account{}, err
	}
	v, err := s.ids.GetAccountByUsername(ctx, username)
	return v.Value, err
}

// UpdateAccount changes roles/enabled (not credentials; see service/auth).
func (s *Service) UpdateAccount(ctx context.Context, id uuid.UUID, fn func(*identity.Account) error) (identity.Account, error) {
	if err := s.authorize(ctx, "account-update", id.String()); err != nil {
		return identity.Account{}, err
	}
	out, err := s.ids.UpdateAccount(ctx, id, func(a *identity.Account) error {
		pw, totp := a.Passwords, a.TOTP
		if err := fn(a); err != nil {
			return err
		}
		a.Passwords, a.TOTP = pw, totp
		return nil
	})
	if err != nil {
		return identity.Account{}, s.fail(ctx, uuid.Nil, "account-update", id.String(), err)
	}
	s.ok(ctx, out.MailboxID, "account-update", out.Username, fmt.Sprintf("roles=%v enabled=%v", out.Roles, out.Enabled))
	return out, nil
}

// RelinkAccount moves an account to another mailbox.
func (s *Service) RelinkAccount(ctx context.Context, id, mailboxID uuid.UUID) (identity.Account, error) {
	if err := s.authorize(ctx, "account-relink", id.String()); err != nil {
		return identity.Account{}, err
	}
	out, err := s.ids.RelinkAccount(ctx, id, mailboxID)
	if err != nil {
		return identity.Account{}, s.fail(ctx, mailboxID, "account-relink", id.String(), err)
	}
	s.ok(ctx, mailboxID, "account-relink", out.Username, "")
	return out, nil
}

// DeleteAccount deletes an account.
func (s *Service) DeleteAccount(ctx context.Context, id uuid.UUID) error {
	if err := s.authorize(ctx, "account-delete", id.String()); err != nil {
		return err
	}
	v, err := s.ids.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	if err := s.ids.DeleteAccount(ctx, id); err != nil {
		return s.fail(ctx, v.Value.MailboxID, "account-delete", v.Value.Username, err)
	}
	s.ok(ctx, v.Value.MailboxID, "account-delete", v.Value.Username, "")
	return nil
}

// ListAccounts lists usernames in a domain with an optional prefix.
func (s *Service) ListAccounts(ctx context.Context, domain, prefix, after string, max int) ([]string, bool, error) {
	if err := s.authorizeRead(ctx, "account-list", domain); err != nil {
		return nil, false, err
	}
	return s.ids.ListDomainAccounts(ctx, domain, prefix, after, max)
}

// ---- aliases ----

// CreateAlias creates an alias.
func (s *Service) CreateAlias(ctx context.Context, al identity.Alias) (identity.Alias, error) {
	if err := s.authorize(ctx, "alias-create", al.Address); err != nil {
		return identity.Alias{}, err
	}
	out, err := s.ids.CreateAlias(ctx, al)
	if err != nil {
		return identity.Alias{}, s.fail(ctx, uuid.Nil, "alias-create", al.Address, err)
	}
	s.ok(ctx, uuid.Nil, "alias-create", out.Address, fmt.Sprintf("destinations=%d", len(out.Destinations)))
	return out, nil
}

// GetAlias returns an alias.
func (s *Service) GetAlias(ctx context.Context, address string) (identity.Alias, error) {
	if err := s.authorizeRead(ctx, "alias-get", address); err != nil {
		return identity.Alias{}, err
	}
	v, err := s.ids.GetAlias(ctx, address)
	return v.Value, err
}

// UpdateAlias applies fn.
func (s *Service) UpdateAlias(ctx context.Context, address string, fn func(*identity.Alias) error) (identity.Alias, error) {
	if err := s.authorize(ctx, "alias-update", address); err != nil {
		return identity.Alias{}, err
	}
	out, err := s.ids.UpdateAlias(ctx, address, fn)
	if err != nil {
		return identity.Alias{}, s.fail(ctx, uuid.Nil, "alias-update", address, err)
	}
	s.ok(ctx, uuid.Nil, "alias-update", out.Address, "")
	return out, nil
}

// DeleteAlias deletes an alias.
func (s *Service) DeleteAlias(ctx context.Context, address string) error {
	if err := s.authorize(ctx, "alias-delete", address); err != nil {
		return err
	}
	if err := s.ids.DeleteAlias(ctx, address); err != nil {
		return s.fail(ctx, uuid.Nil, "alias-delete", address, err)
	}
	s.ok(ctx, uuid.Nil, "alias-delete", address, "")
	return nil
}

// ListAliases lists alias addresses in a domain.
func (s *Service) ListAliases(ctx context.Context, domain, after string, max int) ([]string, bool, error) {
	if err := s.authorizeRead(ctx, "alias-list", domain); err != nil {
		return nil, false, err
	}
	return s.ids.ListDomainAliases(ctx, domain, after, max)
}

// ---- tokens ----

// CreateToken creates an API token for an account on its behalf.
func (s *Service) CreateToken(ctx context.Context, accountID uuid.UUID, roles []string, expiresAt time.Time, comment string) (identity.Token, string, error) {
	if err := s.authorize(ctx, "token-create", accountID.String()); err != nil {
		return identity.Token{}, "", err
	}
	t, secret, err := s.ids.CreateToken(ctx, accountID, roles, expiresAt, comment)
	if err != nil {
		return identity.Token{}, "", s.fail(ctx, uuid.Nil, "token-create", accountID.String(), err)
	}
	s.ok(ctx, uuid.Nil, "token-create", t.ID, fmt.Sprintf("account=%s roles=%v expires=%s comment=%q", accountID, t.Roles, t.ExpiresAt.Format(time.RFC3339), comment))
	return t, secret, nil
}

// ListTokens lists an account's tokens.
func (s *Service) ListTokens(ctx context.Context, accountID uuid.UUID) ([]identity.Token, error) {
	if err := s.authorizeRead(ctx, "token-list", accountID.String()); err != nil {
		return nil, err
	}
	return s.ids.ListTokens(ctx, accountID)
}

// DeleteToken revokes a token.
func (s *Service) DeleteToken(ctx context.Context, accountID uuid.UUID, tokenID string) error {
	if err := s.authorize(ctx, "token-delete", tokenID); err != nil {
		return err
	}
	if err := s.ids.DeleteToken(ctx, accountID, tokenID); err != nil {
		return s.fail(ctx, uuid.Nil, "token-delete", tokenID, err)
	}
	s.ok(ctx, uuid.Nil, "token-delete", tokenID, "account="+accountID.String())
	return nil
}

// ---- admin tokens ----

// CreateAdminToken mints an admin API token. readOnly selects the admin-ro
// role. The secret is returned once.
func (s *Service) CreateAdminToken(ctx context.Context, readOnly bool, expiresAt time.Time, comment string) (identity.Token, string, error) {
	if err := s.authorize(ctx, "admin-token-create", comment); err != nil {
		return identity.Token{}, "", err
	}
	role := identity.RoleAdmin
	if readOnly {
		role = identity.RoleAdminRead
	}
	if expiresAt.IsZero() {
		expiresAt = s.Now().Add(24 * time.Hour)
	}
	t, secret, err := s.ids.CreateAdminToken(ctx, role, expiresAt, comment)
	if err != nil {
		return identity.Token{}, "", s.fail(ctx, uuid.Nil, "admin-token-create", comment, err)
	}
	s.ok(ctx, uuid.Nil, "admin-token-create", t.ID, fmt.Sprintf("role=%s expires=%s comment=%q", role, t.ExpiresAt.Format(time.RFC3339), comment))
	return t, secret, nil
}

// ListAdminTokens lists admin tokens.
func (s *Service) ListAdminTokens(ctx context.Context) ([]identity.Token, error) {
	if err := s.authorizeRead(ctx, "admin-token-list", ""); err != nil {
		return nil, err
	}
	return s.ids.ListAdminTokens(ctx)
}

// DeleteAdminToken revokes an admin token.
func (s *Service) DeleteAdminToken(ctx context.Context, id string) error {
	if err := s.authorize(ctx, "admin-token-delete", id); err != nil {
		return err
	}
	if err := s.ids.DeleteAdminToken(ctx, id); err != nil {
		return s.fail(ctx, uuid.Nil, "admin-token-delete", id, err)
	}
	s.ok(ctx, uuid.Nil, "admin-token-delete", id, "")
	return nil
}

// Bootstrap mints the first full admin token. It refuses when admin tokens
// already exist, so it cannot be used to sneak in a token later (use
// `dovetail admin token`, which is also audited, for recovery).
func (s *Service) Bootstrap(ctx context.Context, comment string, lifetime time.Duration) (identity.Token, string, error) {
	if err := s.authorize(ctx, "bootstrap", comment); err != nil {
		return identity.Token{}, "", err
	}
	existing, err := s.ids.ListAdminTokens(ctx)
	if err != nil {
		return identity.Token{}, "", err
	}
	if len(existing) > 0 {
		return identity.Token{}, "", fmt.Errorf("%w: %d admin token(s) already exist; use `dovetail admin token`", ErrExists, len(existing))
	}
	if comment == "" {
		comment = "bootstrap"
	}
	if lifetime <= 0 {
		lifetime = 24 * time.Hour
	}
	t, secret, err := s.ids.CreateAdminToken(ctx, identity.RoleAdmin, s.Now().Add(lifetime), comment)
	if err != nil {
		return identity.Token{}, "", err
	}
	s.ok(ctx, uuid.Nil, "bootstrap", t.ID, "first admin token created")
	return t, secret, nil
}

// ---- export / import of identity data ----

// Export is the JSON dump format.
type Export struct {
	Version   int                `json:"version"`
	Exported  time.Time          `json:"exported"`
	Domains   []identity.Domain  `json:"domains"`
	Mailboxes []identity.Mailbox `json:"mailboxes"`
	Accounts  []identity.Account `json:"accounts"`
	Aliases   []identity.Alias   `json:"aliases"`
	Tokens    []identity.Token   `json:"tokens"`
	Admins    []identity.Token   `json:"admin_tokens"`
}

// ExportIdentity dumps all identity data (hashes included, no plaintext).
func (s *Service) ExportIdentity(ctx context.Context) (Export, error) {
	if err := s.authorizeRead(ctx, "export-identity", ""); err != nil {
		return Export{}, err
	}
	out := Export{Version: 1, Exported: s.Now().UTC()}
	names, _, err := s.ids.ListDomains(ctx, "", 0)
	if err != nil {
		return Export{}, err
	}
	for _, n := range names {
		d, err := s.ids.GetDomain(ctx, n)
		if err != nil {
			return Export{}, err
		}
		out.Domains = append(out.Domains, d.Value)
		accts, _, err := s.ids.ListDomainAccounts(ctx, n, "", "", 0)
		if err != nil {
			return Export{}, err
		}
		for _, u := range accts {
			a, err := s.ids.GetAccountByUsername(ctx, u)
			if err != nil {
				continue
			}
			out.Accounts = append(out.Accounts, a.Value)
			toks, _ := s.ids.ListTokens(ctx, a.Value.ID)
			out.Tokens = append(out.Tokens, toks...)
		}
		als, _, err := s.ids.ListDomainAliases(ctx, n, "", 0)
		if err != nil {
			return Export{}, err
		}
		for _, addr := range als {
			al, err := s.ids.GetAlias(ctx, addr)
			if err != nil {
				continue
			}
			out.Aliases = append(out.Aliases, al.Value)
		}
	}
	err = s.ids.ListMailboxes(ctx, func(id uuid.UUID) error {
		m, err := s.ids.GetMailbox(ctx, id)
		if err != nil {
			return nil
		}
		out.Mailboxes = append(out.Mailboxes, m.Value)
		return nil
	})
	if err != nil {
		return Export{}, err
	}
	if out.Admins, err = s.ids.ListAdminTokens(ctx); err != nil {
		return Export{}, err
	}
	s.ok(ctx, uuid.Nil, "export-identity", "", fmt.Sprintf("domains=%d mailboxes=%d accounts=%d aliases=%d", len(out.Domains), len(out.Mailboxes), len(out.Accounts), len(out.Aliases)))
	return out, nil
}

// ImportIdentity loads a dump. Existing objects are skipped (not merged);
// the counts of created/skipped objects are returned as a string.
func (s *Service) ImportIdentity(ctx context.Context, data []byte) (string, error) {
	if err := s.authorize(ctx, "import-identity", ""); err != nil {
		return "", err
	}
	var in Export
	if err := json.Unmarshal(data, &in); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	created, skipped := 0, 0
	count := func(err error) error {
		if err == nil {
			created++
			return nil
		}
		if errors.Is(err, identity.ErrExists) {
			skipped++
			return nil
		}
		return err
	}
	for _, d := range in.Domains {
		if _, err := s.ids.CreateDomain(ctx, d); count(err) != nil {
			return "", err
		}
	}
	for _, m := range in.Mailboxes {
		if _, err := s.ids.CreateMailbox(ctx, m); count(err) != nil {
			return "", err
		}
	}
	for _, a := range in.Accounts {
		if _, err := s.ids.CreateAccount(ctx, a); count(err) != nil {
			return "", err
		}
	}
	for _, al := range in.Aliases {
		if _, err := s.ids.CreateAlias(ctx, al); count(err) != nil {
			return "", err
		}
	}
	// Tokens carry only hashes; they cannot be recreated through CreateToken
	// (which generates a new secret). They are intentionally not imported.
	summary := fmt.Sprintf("created=%d skipped=%d tokens-ignored=%d", created, skipped, len(in.Tokens)+len(in.Admins))
	s.ok(ctx, uuid.Nil, "import-identity", "", summary)
	return summary, nil
}
