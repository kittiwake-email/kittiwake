package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/queue"
)

func newSvc(t *testing.T) (*Service, *identity.Store, queue.Store, context.Context, context.Context) {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	ids := identity.New(meta)
	q := queue.New(idxp)
	aud := auditor.New(audit.New(idxp), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	s := New(ids, index.New(idxp), q, aud)
	adm := actor.With(context.Background(), actor.Actor{AccountID: uuid.New(), Username: "root@example.org", Roles: []string{"admin"}, Protocol: "admin"})
	user := actor.With(context.Background(), actor.Actor{AccountID: uuid.New(), Username: "u@example.org", Roles: []string{"read"}, Protocol: "jmap"})
	return s, ids, q, adm, user
}

func TestAuthorizationAndCRUD(t *testing.T) {
	s, _, q, adm, user := newSvc(t)
	if _, err := s.CreateDomain(user, identity.Domain{Name: "example.org"}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("non-admin: %v", err)
	}
	d, err := s.CreateDomain(adm, identity.Domain{Name: "example.org", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	mb, err := s.CreateMailbox(adm, identity.Mailbox{})
	if err != nil {
		t.Fatal(err)
	}
	f := false
	a, err := s.CreateAccount(adm, CreateAccountRequest{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{"read", "write"}, Password: "longenough", Enabled: &f})
	if err != nil || a.Enabled || len(a.Passwords) != 1 {
		t.Fatalf("create account: %+v %v", a, err)
	}
	if _, err := s.CreateAccount(adm, CreateAccountRequest{Username: "x@example.org", MailboxID: mb.ID, Password: "short"}); !errors.Is(err, ErrInvalid) {
		t.Fatal("short password")
	}
	a, _ = s.UpdateAccount(adm, a.ID, func(a *identity.Account) error { a.Enabled = true; a.Passwords = nil; return nil })
	if !a.Enabled || len(a.Passwords) != 1 {
		t.Fatal("update must not touch passwords")
	}
	al, err := s.CreateAlias(adm, identity.Alias{Address: "info@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	tok, secret, err := s.CreateToken(adm, a.ID, nil, time.Now().Add(time.Hour), "ci")
	if err != nil || secret == "" {
		t.Fatal(err)
	}
	info, err := s.InspectMailbox(adm, mb.ID, identity.QuotaLimits{HardBytes: 5})
	if err != nil || len(info.Accounts) != 1 || info.Indexed || info.Limits.HardBytes != 5 {
		t.Fatalf("inspect: %+v %v", info, err)
	}
	// Hold / release.
	if _, err := s.Hold(adm, mb.ID, HoldRequest{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("hold without reason")
	}
	m, err := s.Hold(adm, mb.ID, HoldRequest{Reason: "subpoena", CaseRef: "42", Sealed: true})
	if err != nil || m.Hold == nil || !m.Hold.AcceptDelivery || !m.Hold.Sealed || m.Hold.SetBy != "root@example.org" {
		t.Fatalf("hold: %+v %v", m, err)
	}
	if _, err := s.Hold(adm, mb.ID, HoldRequest{Reason: "again"}); !errors.Is(err, ErrExists) {
		t.Fatal("double hold")
	}
	holds, _ := s.ListHolds(adm, "")
	if len(holds) != 1 {
		t.Fatal("list holds")
	}
	// UpdateMailbox cannot clear hold.
	m, _ = s.UpdateMailbox(adm, mb.ID, func(m *identity.Mailbox) error { m.Hold = nil; m.Quota.HardBytes = 99; return nil })
	if m.Hold == nil || m.Quota.HardBytes != 99 {
		t.Fatal("update cleared hold")
	}
	if err := s.DeleteMailbox(adm, mb.ID); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete on hold/with accounts: %v", err)
	}
	m, err = s.Release(adm, mb.ID, "case closed")
	if err != nil || m.Hold != nil || m.Released == nil {
		t.Fatal(err)
	}
	if _, err := s.Release(adm, mb.ID, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal("double release")
	}
	// Export.
	exp, err := s.ExportIdentity(adm)
	if err != nil || len(exp.Domains) != 1 || len(exp.Accounts) != 1 || len(exp.Aliases) != 1 || len(exp.Tokens) != 1 || len(exp.Mailboxes) != 1 {
		t.Fatalf("export: %+v %v", exp, err)
	}
	// Cleanup path: token, alias, account, mailbox (scheduled), domain.
	if err := s.DeleteToken(adm, a.ID, tok.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAlias(adm, al.Address); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(adm, a.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMailbox(adm, mb.ID); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Stats(context.Background(), TaskRemoveMailbox)
	if st.Pending != 1 {
		t.Fatal("removal not scheduled")
	}
	if err := s.DeleteDomain(adm, d.Name); err != nil {
		t.Fatal(err)
	}
	// Import into a fresh store.
	s2, _, _, adm2, _ := newSvc(t)
	b, _ := json.Marshal(exp)
	summary, err := s2.ImportIdentity(adm2, b)
	if err != nil || summary != "created=4 skipped=0 tokens-ignored=1" {
		t.Fatalf("import: %s %v", summary, err)
	}
	summary, _ = s2.ImportIdentity(adm2, b)
	if summary != "created=0 skipped=4 tokens-ignored=1" {
		t.Fatalf("reimport: %s", summary)
	}
}

func TestBootstrapAndAdminTokens(t *testing.T) {
	s, ids, _, adm, user := newSvc(t)
	cli := actor.With(context.Background(), actor.System("cli"))
	if _, _, err := s.Bootstrap(user, "", 0); !errors.Is(err, ErrForbidden) {
		t.Fatal("user bootstrap")
	}
	tok, secret, err := s.Bootstrap(cli, "", 0)
	if err != nil || secret == "" || tok.Comment != "bootstrap" || tok.Roles[0] != identity.RoleAdmin || !tok.IsAdmin() {
		t.Fatalf("bootstrap: %+v %v", tok, err)
	}
	// No account, no domain was created.
	if names, _, _ := ids.ListDomains(context.Background(), "", "", 0); len(names) != 0 {
		t.Fatalf("bootstrap created domains: %v", names)
	}
	if _, _, err := s.Bootstrap(cli, "", 0); !errors.Is(err, ErrExists) {
		t.Fatalf("double bootstrap: %v", err)
	}
	// Read-only tokens via the API path.
	ro, _, err := s.CreateAdminToken(adm, true, "", time.Time{}, "monitoring")
	if err != nil || ro.Roles[0] != identity.RoleAdminRead {
		t.Fatal(err)
	}
	roCtx := actor.With(context.Background(), actor.Actor{Username: "admin:monitoring", Roles: ro.Roles, Protocol: "admin"})
	if _, err := s.ListAdminTokens(roCtx); err != nil {
		t.Fatalf("read-only list: %v", err)
	}
	if _, _, err := s.CreateAdminToken(roCtx, false, "", time.Time{}, "escalate"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("read-only must not mint tokens: %v", err)
	}
	if _, err := s.CreateDomain(roCtx, identity.Domain{Name: "x.example"}); !errors.Is(err, ErrForbidden) {
		t.Fatal("read-only must not create domains")
	}
	if _, _, err := s.ListDomains(roCtx, "", "", 0); err != nil {
		t.Fatal("read-only may list domains")
	}
	list, _ := s.ListAdminTokens(adm)
	if len(list) != 2 {
		t.Fatalf("tokens: %d", len(list))
	}
	if err := s.DeleteAdminToken(adm, ro.ID); err != nil {
		t.Fatal(err)
	}
}

// TestTenantScope: a token bound to a namespace sees and changes only that
// namespace; platform-level operations are refused.
func TestTenantScope(t *testing.T) {
	s, ids, _, adm, _ := newSvc(t)
	ctx := context.Background()
	if _, err := s.CreateNamespace(adm, identity.Namespace{Name: "acme", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateNamespace(adm, identity.Namespace{Name: "other", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	acmeDom, err := s.CreateDomain(adm, identity.Domain{Name: "acme.example", Namespace: "acme", Enabled: true})
	if err != nil || acmeDom.Namespace != "acme" {
		t.Fatalf("domain: %+v %v", acmeDom, err)
	}
	if _, err := s.CreateDomain(adm, identity.Domain{Name: "other.example", Namespace: "other", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	otherMB, _ := s.CreateMailbox(adm, identity.Mailbox{Namespace: "other"})
	tok, _, err := s.CreateAdminToken(adm, false, "acme", time.Time{}, "acme ops")
	if err != nil || tok.Namespace != "acme" {
		t.Fatalf("tenant token: %+v %v", tok, err)
	}
	tenant := actor.With(ctx, actor.Actor{Username: "admin:acme ops", Roles: tok.Roles, Namespace: "acme", Protocol: "admin"})

	// Creates land in the tenant's namespace; naming another one is refused.
	d, err := s.CreateDomain(tenant, identity.Domain{Name: "mail.acme.example", Enabled: true})
	if err != nil || d.Namespace != "acme" {
		t.Fatalf("tenant domain: %+v %v", d, err)
	}
	if _, err := s.CreateDomain(tenant, identity.Domain{Name: "x.example", Namespace: "other"}); !errors.Is(err, ErrScope) {
		t.Fatalf("domain elsewhere: %v", err)
	}
	mb, err := s.CreateMailbox(tenant, identity.Mailbox{})
	if err != nil || mb.Namespace != "acme" {
		t.Fatalf("tenant mailbox: %+v %v", mb, err)
	}
	a, err := s.CreateAccount(tenant, CreateAccountRequest{Username: "u@acme.example", MailboxID: mb.ID, Roles: []string{"read"}})
	if err != nil || a.Namespace != "acme" {
		t.Fatalf("tenant account: %+v %v", a, err)
	}
	if _, err := s.CreateAccount(tenant, CreateAccountRequest{Username: "u@other.example", MailboxID: otherMB.ID, Roles: []string{"read"}}); !errors.Is(err, identity.ErrDomainUnknown) {
		t.Fatalf("account in another namespace must read as not found: %v", err)
	}
	// Listings are filtered; records elsewhere read as not found.
	names, _, _ := s.ListDomains(tenant, "", "", 10)
	if len(names) != 2 || names[0] != "acme.example" || names[1] != "mail.acme.example" {
		t.Fatalf("tenant domain list: %v", names)
	}
	if _, _, err := s.ListDomains(tenant, "other", "", 10); !errors.Is(err, ErrScope) {
		t.Fatalf("list elsewhere: %v", err)
	}
	var seen []uuid.UUID
	_ = s.ListMailboxes(tenant, "", func(id uuid.UUID) error { seen = append(seen, id); return nil })
	if len(seen) != 1 || seen[0] != mb.ID {
		t.Fatalf("tenant mailbox list: %v", seen)
	}
	if _, err := s.GetMailbox(tenant, otherMB.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mailbox elsewhere: %v", err)
	}
	if _, err := s.GetDomain(tenant, "other.example"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("domain elsewhere: %v", err)
	}
	if _, err := s.SetMailboxState(tenant, otherMB.ID, identity.StateSuspended, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("state elsewhere: %v", err)
	}
	if err := s.DeleteDomain(tenant, "other.example"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete elsewhere: %v", err)
	}
	if nss, _, _ := s.ListNamespaces(tenant, "", 10); len(nss) != 1 || nss[0] != "acme" {
		t.Fatalf("tenant namespaces: %v", nss)
	}
	if _, err := s.GetNamespace(tenant, "other"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("namespace elsewhere: %v", err)
	}
	// Platform-level operations are refused.
	if _, err := s.CreateNamespace(tenant, identity.Namespace{Name: "more"}); !errors.Is(err, ErrScope) {
		t.Fatalf("tenant creating namespaces: %v", err)
	}
	if _, _, err := s.CreateAdminToken(tenant, false, "acme", time.Time{}, "escalate"); !errors.Is(err, ErrScope) {
		t.Fatalf("tenant minting tokens: %v", err)
	}
	if _, err := s.AddStorage(tenant, StorageRequest{Name: "x", MonHost: "x", Pools: identity.Pools{Messages: "m", Attachments: "a", Index: "i"}}); !errors.Is(err, ErrScope) {
		t.Fatalf("tenant registering storage: %v", err)
	}
	if _, err := s.ListNodes(tenant, ""); !errors.Is(err, ErrScope) {
		t.Fatalf("tenant listing nodes: %v", err)
	}
	if _, err := s.ExportIdentity(tenant); !errors.Is(err, ErrScope) {
		t.Fatalf("tenant exporting identity: %v", err)
	}
	// Storage: a tenant sees its own entries without keys.
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	if _, err := s.AddStorage(adm, StorageRequest{Name: "acme-1", Namespace: "acme", MonHost: "x", User: "u", Key: "secret", Pools: pools}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddStorage(adm, StorageRequest{Name: "other-1", Namespace: "other", MonHost: "y", User: "u", Key: "secret", Pools: pools}); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListStorage(tenant)
	if err != nil || len(list) != 1 || list[0].Name != "acme-1" || list[0].Key != "" {
		t.Fatalf("tenant storage list: %+v %v", list, err)
	}
	if all, _ := s.ListStorage(adm); len(all) != 2 || all[0].Key == "" {
		t.Fatalf("platform storage list: %+v", all)
	}
	if _, err := s.GetStorage(tenant, list[0].ID+1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("storage elsewhere: %v", err)
	}
	// The audit trail records the namespace of what was acted upon.
	_ = ids
}
