// Package identitytest is the conformance suite every identity backend
// must pass. The RADOS backend runs it in its unit tests; the MariaDB
// backend runs it against a real server when KITTIWAKE_TEST_DSN is set.
package identitytest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/store/identity"
)

// Factory returns a fresh, empty store whose clock is the suite's: the
// store's Now must read the time the suite sets through SetNow.
type Factory func(t *testing.T) *identity.Store

// Run runs every conformance test against stores from f.
func Run(t *testing.T, f Factory) {
	for _, tc := range []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"Namespaces", testNamespaces},
		{"Domains", testDomains},
		{"AccountsAndMailboxes", testAccountsAndMailboxes},
		{"Aliases", testAliases},
		{"Tokens", testTokens},
		{"AdminTokens", testAdminTokens},
		{"ConcurrentUpdates", testConcurrentUpdates},
		{"Services", testServices},
		{"Storage", testStorages},
		{"Nodes", testNodes},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, f) })
	}
}

func testDomains(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	d, err := s.CreateDomain(ctx, identity.Domain{Name: "Example.ORG", Enabled: true})
	if err != nil || d.Name != "example.org" {
		t.Fatalf("%+v %v", d, err)
	}
	if _, err := s.CreateDomain(ctx, identity.Domain{Name: "example.org"}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("expected exists, got %v", err)
	}
	if _, err := s.CreateDomain(ctx, identity.Domain{Name: "bad domain"}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("expected invalid, got %v", err)
	}
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "a.example", Enabled: true})
	names, more, err := s.ListDomains(ctx, "", "", 10)
	if err != nil || more || len(names) != 2 || names[0] != "a.example" || names[1] != "example.org" {
		t.Fatalf("list: %v %v %v", names, more, err)
	}
	if d.Namespace != identity.DefaultNamespace {
		t.Fatalf("default namespace: %+v", d)
	}
	upd, err := s.UpdateDomain(ctx, "example.org", func(d *identity.Domain) error { d.Enabled = false; return nil })
	if err != nil || upd.Enabled {
		t.Fatalf("update: %+v %v", upd, err)
	}
	got, _ := s.GetDomain(ctx, "EXAMPLE.org")
	if got.Value.Enabled {
		t.Fatal("update not persisted")
	}
	if err := s.DeleteDomain(ctx, "a.example"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDomain(ctx, "a.example"); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func testAccountsAndMailboxes(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, err := s.CreateMailbox(ctx, identity.Mailbox{Quota: identity.QuotaLimits{HardBytes: 1 << 30}})
	if err != nil || mb.ID == uuid.Nil {
		t.Fatal(err)
	}

	// identity.Domain must exist.
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "x@nope.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}}); !errors.Is(err, identity.ErrDomainUnknown) {
		t.Fatalf("expected domain unknown, got %v", err)
	}
	// identity.Mailbox must exist.
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "x@example.org", MailboxID: uuid.New(), Roles: []string{identity.RoleRead}}); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	// Bad role.
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "x@example.org", MailboxID: mb.ID, Roles: []string{"god"}}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("expected invalid, got %v", err)
	}

	a, err := s.CreateAccount(ctx, identity.Account{
		Username:  "Wido@Example.org",
		MailboxID: mb.ID,
		Roles:     []string{identity.RoleRead, identity.RoleWrite, identity.RoleSend},
		Passwords: []identity.Password{{Hash: "{PLAIN}x"}},
		Enabled:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "wido@example.org" || a.Domain != "example.org" || a.Passwords[0].Kind != identity.PasswordPrimary || a.Passwords[0].ID == "" {
		t.Fatalf("normalisation: %+v", a)
	}
	// Uniqueness.
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("expected exists, got %v", err)
	}
	// Lookup by name.
	got, err := s.GetAccountByUsername(ctx, "WIDO@example.org")
	if err != nil || got.Value.ID != a.ID {
		t.Fatalf("by username: %v %v", got, err)
	}
	// Listings.
	us, _, _ := s.ListDomainAccounts(ctx, "example.org", "", "", 10)
	if len(us) != 1 || us[0] != "wido@example.org" {
		t.Fatalf("domain accounts: %v", us)
	}
	us, _, _ = s.ListDomainAccounts(ctx, "example.org", "wi", "", 10)
	if len(us) != 1 {
		t.Fatalf("prefix search: %v", us)
	}
	us, _, _ = s.ListDomainAccounts(ctx, "example.org", "zz", "", 10)
	if len(us) != 0 {
		t.Fatalf("prefix search miss: %v", us)
	}
	mus, _, _ := s.ListMailboxAccounts(ctx, mb.ID, "", 10)
	if len(mus) != 1 || mus[0] != "wido@example.org" {
		t.Fatalf("mailbox accounts: %v", mus)
	}
	// identity.Mailbox in use.
	if err := s.DeleteMailbox(ctx, mb.ID); !errors.Is(err, identity.ErrInUse) {
		t.Fatalf("expected in use, got %v", err)
	}
	// identity.Domain in use.
	if err := s.DeleteDomain(ctx, "example.org"); !errors.Is(err, identity.ErrInUse) {
		t.Fatalf("expected in use, got %v", err)
	}
	// Update.
	upd, err := s.UpdateAccount(ctx, a.ID, func(x *identity.Account) error { x.Enabled = false; x.Roles = []string{identity.RoleRead}; return nil })
	if err != nil || upd.Enabled || len(upd.Roles) != 1 {
		t.Fatalf("update: %+v %v", upd, err)
	}
	if _, err := s.UpdateAccount(ctx, a.ID, func(x *identity.Account) error { x.Username = "other@example.org"; return nil }); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("expected invalid rename, got %v", err)
	}
	// Accounts cannot carry admin roles and always need a mailbox.
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "admin@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleAdmin}}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("expected invalid, got %v", err)
	}
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "admin@example.org", Roles: []string{identity.RoleRead}, Enabled: true}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("expected invalid without mailbox, got %v", err)
	}
	// Relink.
	mb2, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	if _, err := s.RelinkAccount(ctx, a.ID, mb2.ID); err != nil {
		t.Fatal(err)
	}
	if mus, _, _ := s.ListMailboxAccounts(ctx, mb.ID, "", 10); len(mus) != 0 {
		t.Fatalf("old mailbox still lists account: %v", mus)
	}
	if mus, _, _ := s.ListMailboxAccounts(ctx, mb2.ID, "", 10); len(mus) != 1 {
		t.Fatalf("new mailbox missing account: %v", mus)
	}
	// Delete.
	if err := s.DeleteAccount(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAccountByUsername(ctx, "wido@example.org"); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if err := s.DeleteMailbox(ctx, mb.ID); err != nil {
		t.Fatal(err)
	}
	var ids []uuid.UUID
	_ = s.ListMailboxes(ctx, "", func(id uuid.UUID) error { ids = append(ids, id); return nil })
	if len(ids) != 1 || ids[0] != mb2.ID {
		t.Fatalf("mailboxes: %v", ids)
	}
}

func testAliases(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = s.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}, Enabled: true})

	if _, err := s.CreateAlias(ctx, identity.Alias{Address: "wido@example.org", Destinations: []identity.Destination{{MailboxID: mb.ID}}}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("alias over account should fail: %v", err)
	}
	if _, err := s.CreateAlias(ctx, identity.Alias{Address: "info@example.org"}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("no destinations: %v", err)
	}
	if _, err := s.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Destinations: []identity.Destination{{MailboxID: mb.ID, Address: "x@example.org"}}}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("both destination kinds: %v", err)
	}
	if _, err := s.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Destinations: []identity.Destination{{Address: "info@example.org"}}}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("self alias: %v", err)
	}
	al, err := s.CreateAlias(ctx, identity.Alias{Address: "Info@Example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb.ID}, {Address: "Wido@example.org"}}})
	if err != nil || al.Address != "info@example.org" || al.Destinations[1].Address != "wido@example.org" {
		t.Fatalf("%+v %v", al, err)
	}
	ca, err := s.CreateAlias(ctx, identity.Alias{Address: "@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb.ID}}})
	if err != nil || !ca.IsCatchAll() || ca.Domain != "example.org" {
		t.Fatalf("catch-all: %+v %v", ca, err)
	}
	addrs, _, _ := s.ListDomainAliases(ctx, "example.org", "", 10)
	if len(addrs) != 2 || addrs[0] != "@example.org" || addrs[1] != "info@example.org" {
		t.Fatalf("list: %v", addrs)
	}
	mal, _ := s.ListMailboxAliases(ctx, mb.ID)
	if len(mal) != 2 || mal[0] != "@example.org" || mal[1] != "info@example.org" {
		t.Fatalf("mailbox aliases: %v", mal)
	}
	// Reverse of address destinations: chains, and destinations outside a hosted domain.
	if _, err := s.CreateAlias(ctx, identity.Alias{Address: "team@example.org", Enabled: true, Destinations: []identity.Destination{{Address: "info@example.org"}, {Address: "Ext@Other.example"}}}); err != nil {
		t.Fatal(err)
	}
	if to, err := s.ListAliasesTo(ctx, "WIDO@example.org"); err != nil || len(to) != 1 || to[0] != "info@example.org" {
		t.Fatalf("aliases to wido: %v %v", to, err)
	}
	if to, _ := s.ListAliasesTo(ctx, "info@example.org"); len(to) != 1 || to[0] != "team@example.org" {
		t.Fatalf("aliases to info: %v", to)
	}
	if _, err := s.ListAliasesTo(ctx, "ext@other.example"); err != nil { // defined only for hosted addresses
		t.Fatalf("aliases to an external address: %v", err)
	}
	if err := s.DeleteAlias(ctx, "team@example.org"); err != nil {
		t.Fatal(err)
	}
	if to, _ := s.ListAliasesTo(ctx, "info@example.org"); len(to) != 0 {
		t.Fatalf("aliases to info after delete: %v", to)
	}
	mb2, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = s.UpdateAlias(ctx, "info@example.org", func(a *identity.Alias) error {
		a.Destinations = []identity.Destination{{MailboxID: mb2.ID}}
		return nil
	})
	if mal, _ = s.ListMailboxAliases(ctx, mb.ID); len(mal) != 1 {
		t.Fatalf("mailbox aliases after retarget: %v", mal)
	}
	if mal, _ = s.ListMailboxAliases(ctx, mb2.ID); len(mal) != 1 || mal[0] != "info@example.org" {
		t.Fatalf("new mailbox aliases: %v", mal)
	}
	if to, _ := s.ListAliasesTo(ctx, "wido@example.org"); len(to) != 0 {
		t.Fatalf("aliases to wido after retarget: %v", to)
	}
	_, _ = s.UpdateAlias(ctx, "info@example.org", func(a *identity.Alias) error {
		a.Destinations = []identity.Destination{{MailboxID: mb.ID}, {Address: "wido@example.org"}}
		return nil
	})
	if _, err := s.UpdateAlias(ctx, "info@example.org", func(a *identity.Alias) error { a.Enabled = false; return nil }); err != nil {
		t.Fatal(err)
	}
	g, _ := s.GetAlias(ctx, "INFO@example.org")
	if g.Value.Enabled {
		t.Fatal("update lost")
	}
	if err := s.DeleteAlias(ctx, "info@example.org"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAlias(ctx, "info@example.org"); !errors.Is(err, identity.ErrNotFound) {
		t.Fatal(err)
	}
	addrs, _, _ = s.ListDomainAliases(ctx, "example.org", "", 10)
	if len(addrs) != 1 {
		t.Fatalf("list after delete: %v", addrs)
	}
	if mal, _ := s.ListMailboxAliases(ctx, mb.ID); len(mal) != 1 || mal[0] != "@example.org" {
		t.Fatalf("mailbox aliases after delete: %v", mal)
	}
	if to, _ := s.ListAliasesTo(ctx, "wido@example.org"); len(to) != 0 {
		t.Fatalf("aliases to wido after delete: %v", to)
	}
}

func testTokens(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	a, _ := s.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead, identity.RoleWrite}, Enabled: true})
	exp := s.Now().Add(24 * time.Hour)

	if _, _, err := s.CreateToken(ctx, a.ID, nil, exp, ""); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("comment required: %v", err)
	}
	if _, _, err := s.CreateToken(ctx, a.ID, nil, s.Now(), "x"); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("expiry required: %v", err)
	}
	if _, _, err := s.CreateToken(ctx, a.ID, []string{identity.RoleSend}, exp, "x"); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("role escalation: %v", err)
	}
	tok, secret, err := s.CreateToken(ctx, a.ID, []string{identity.RoleRead}, exp, "phone app")
	if err != nil {
		t.Fatal(err)
	}
	if secret[:4] != "kwt_" || tok.Hash != identity.HashToken(secret) || len(tok.Roles) != 1 {
		t.Fatalf("token: %+v %s", tok, secret)
	}
	got, err := s.LookupToken(ctx, secret)
	if err != nil || got.Value.ID != tok.ID || got.Value.AccountID != a.ID {
		t.Fatalf("lookup: %+v %v", got, err)
	}
	if _, err := s.LookupToken(ctx, "kwt_nope"); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := s.LookupToken(ctx, "nope"); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("expected not found for bad prefix, got %v", err)
	}
	if got.Value.Expired(exp.Add(-time.Second)) || !got.Value.Expired(exp) {
		t.Fatal("expiry")
	}
	s.TouchToken(ctx, got)
	got2, _ := s.LookupToken(ctx, secret)
	if got2.Value.LastUsed.IsZero() {
		t.Fatal("touch lost")
	}
	// Default roles = account roles.
	tok2, _, _ := s.CreateToken(ctx, a.ID, nil, exp, "ci")
	if len(tok2.Roles) != 2 {
		t.Fatalf("default roles: %v", tok2.Roles)
	}
	list, _ := s.ListTokens(ctx, a.ID)
	if len(list) != 2 {
		t.Fatalf("list: %v", list)
	}
	if err := s.DeleteToken(ctx, a.ID, tok.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupToken(ctx, secret); !errors.Is(err, identity.ErrNotFound) {
		t.Fatal("token not revoked")
	}
	if err := s.DeleteToken(ctx, a.ID, tok.ID); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("double delete: %v", err)
	}
	// Deleting the account removes remaining tokens.
	tok2secret := ""
	_ = tok2secret
	if err := s.DeleteAccount(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	list, err = s.ListTokens(ctx, a.ID)
	if err != nil || len(list) != 0 {
		t.Fatalf("tokens after account delete: %v %v", list, err)
	}
}

func testAdminTokens(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	exp := s.Now().Add(time.Hour)
	if _, _, err := s.CreateAdminToken(ctx, identity.RoleAdmin, "", exp, ""); !errors.Is(err, identity.ErrInvalid) {
		t.Fatal("comment required")
	}
	if _, _, err := s.CreateAdminToken(ctx, "read", "", exp, "x"); !errors.Is(err, identity.ErrInvalid) {
		t.Fatal("account role rejected as admin role")
	}
	full, secret, err := s.CreateAdminToken(ctx, identity.RoleAdmin, "", exp, "ops")
	if err != nil || !full.IsAdmin() || full.Roles[0] != identity.RoleAdmin || full.Namespace != "" {
		t.Fatalf("%+v %v", full, err)
	}
	ro, _, err := s.CreateAdminToken(ctx, identity.RoleAdminRead, "", exp, "monitoring")
	if err != nil || ro.Roles[0] != identity.RoleAdminRead {
		t.Fatal(err)
	}
	got, err := s.LookupToken(ctx, secret)
	if err != nil || got.Value.ID != full.ID || !got.Value.IsAdmin() || got.Value.Namespace != "" {
		t.Fatalf("lookup: %+v %v", got, err)
	}
	// Tenant admin: bound to an existing namespace.
	if _, _, err := s.CreateAdminToken(ctx, identity.RoleAdmin, "nope", exp, "x"); !errors.Is(err, identity.ErrNamespaceUnknown) {
		t.Fatalf("unknown namespace: %v", err)
	}
	_, _ = s.CreateNamespace(ctx, identity.Namespace{Name: "acme", Enabled: true})
	tenant, tsecret, err := s.CreateAdminToken(ctx, identity.RoleAdmin, "ACME", exp, "acme ops")
	if err != nil || tenant.Namespace != "acme" {
		t.Fatalf("tenant token: %+v %v", tenant, err)
	}
	if tg, err := s.LookupToken(ctx, tsecret); err != nil || tg.Value.Namespace != "acme" || !tg.Value.IsAdmin() {
		t.Fatalf("tenant lookup: %+v %v", tg, err)
	}
	if err := s.DeleteAdminToken(ctx, tenant.ID); err != nil {
		t.Fatal(err)
	}
	list, _ := s.ListAdminTokens(ctx)
	if len(list) != 2 {
		t.Fatalf("list: %d", len(list))
	}
	if err := s.DeleteAdminToken(ctx, full.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupToken(ctx, secret); !errors.Is(err, identity.ErrNotFound) {
		t.Fatal("not revoked")
	}
	if err := s.DeleteAdminToken(ctx, full.ID); !errors.Is(err, identity.ErrNotFound) {
		t.Fatal("double delete")
	}
	if list, _ = s.ListAdminTokens(ctx); len(list) != 1 || list[0].ID != ro.ID {
		t.Fatalf("after delete: %+v", list)
	}
}

// testConcurrentUpdates: writers racing on one record never lose an
// update, however the backend achieves it (RADOS retries on version
// mismatch, MariaDB locks the row).
func testConcurrentUpdates(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := s.UpdateDomain(ctx, "example.org", func(d *identity.Domain) error {
				if d.Settings == nil {
					d.Settings = map[string]string{}
				}
				d.Settings[string(rune('a'+i))] = "1"
				return nil
			})
			errs <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.GetDomain(ctx, "example.org")
	if len(got.Value.Settings) != n {
		t.Fatalf("lost update: %d of %d settings survived: %v", len(got.Value.Settings), n, got.Value.Settings)
	}
}

func testServices(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	// nil services -> all known.
	a, err := s.CreateAccount(ctx, identity.Account{Username: "all@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}, Enabled: true})
	if err != nil || len(a.Services) != len(identity.AllServices()) || !a.HasService(identity.ServiceJMAP) {
		t.Fatalf("default services: %+v %v", a.Services, err)
	}
	// Explicit subset.
	b, err := s.CreateAccount(ctx, identity.Account{Username: "phone@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}, Services: []string{identity.ServiceJMAP}, Enabled: true})
	if err != nil || len(b.Services) != 1 || b.HasService(identity.ServiceIMAP) {
		t.Fatalf("subset: %+v %v", b.Services, err)
	}
	got, _ := s.GetAccount(ctx, b.ID)
	if len(got.Value.Services) != 1 || got.Value.Services[0] != identity.ServiceJMAP {
		t.Fatalf("persisted services: %+v", got.Value.Services)
	}
	// Unknown service refused; empty list allowed (no login anywhere).
	if _, err := s.CreateAccount(ctx, identity.Account{Username: "x@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}, Services: []string{"telnet"}}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("unknown service: %v", err)
	}
	upd, err := s.UpdateAccount(ctx, b.ID, func(x *identity.Account) error { x.Services = []string{}; return nil })
	if err != nil || len(upd.Services) != 0 {
		t.Fatalf("empty services: %+v %v", upd.Services, err)
	}
	if _, err := s.UpdateAccount(ctx, b.ID, func(x *identity.Account) error {
		x.Services = []string{identity.ServiceIMAP, identity.ServiceIMAP}
		return nil
	}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("duplicate service: %v", err)
	}
}

func testStorages(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	if _, err := s.CreateStorage(ctx, identity.Storage{Name: "", MonHost: "x", Pools: pools}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("name required: %v", err)
	}
	if _, err := s.CreateStorage(ctx, identity.Storage{Name: "ams1", MonHost: "x", Pools: identity.Pools{Messages: "m"}}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("pools required: %v", err)
	}
	c1, err := s.CreateStorage(ctx, identity.Storage{Name: " ams1 ", MonHost: "[2001:db8::1]:6789", User: "kittiwake", Key: "AQ==", Pools: pools, Placement: true})
	if err != nil || c1.ID == 0 || c1.Name != "ams1" {
		t.Fatalf("create: %+v %v", c1, err)
	}
	if _, err := s.CreateStorage(ctx, identity.Storage{Name: "ams1", MonHost: "y", Pools: pools}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("duplicate name: %v", err)
	}
	fs := uuid.New()
	c2, err := s.CreateStorage(ctx, identity.Storage{Name: "rtd1", FSID: fs, MonHost: "y", Pools: pools, Placement: true})
	if err != nil || c2.ID == c1.ID || c2.FSID != fs {
		t.Fatalf("create 2: %+v %v", c2, err)
	}
	if _, err := s.CreateStorage(ctx, identity.Storage{Name: "rtd2", FSID: fs, MonHost: "y", Pools: pools}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("duplicate fsid: %v", err)
	}
	list, err := s.ListStorage(ctx)
	if err != nil || len(list) != 2 || list[0].ID != c1.ID || list[1].Name != "rtd1" {
		t.Fatalf("list: %+v %v", list, err)
	}
	upd, err := s.UpdateStorage(ctx, c1.ID, func(c *identity.Storage) error { c.Placement = false; c.FSID = uuid.New(); return nil })
	if err != nil || upd.Placement || upd.FSID == uuid.Nil {
		t.Fatalf("update: %+v %v", upd, err)
	}
	got, _ := s.GetStorage(ctx, c1.ID)
	if got.Placement || got.FSID != upd.FSID || got.Key != "AQ==" {
		t.Fatalf("get after update: %+v", got)
	}
	if _, err := s.GetStorage(ctx, 999); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	// A mailbox pins its storage.
	if _, err := s.CreateMailbox(ctx, identity.Mailbox{StorageID: 999}); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("unknown storage on mailbox: %v", err)
	}
	mb, err := s.CreateMailbox(ctx, identity.Mailbox{StorageID: c2.ID})
	if err != nil || mb.StorageID != c2.ID {
		t.Fatalf("mailbox on storage: %+v %v", mb, err)
	}
	if g, _ := s.GetMailbox(ctx, mb.ID); g.Value.StorageID != c2.ID {
		t.Fatalf("storage id not persisted: %+v", g.Value)
	}
	if err := s.DeleteStorage(ctx, c2.ID); !errors.Is(err, identity.ErrInUse) {
		t.Fatalf("delete in use: %v", err)
	}
	if err := s.DeleteStorage(ctx, c1.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteStorage(ctx, c1.ID); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("double delete: %v", err)
	}
	if list, _ := s.ListStorage(ctx); len(list) != 1 {
		t.Fatalf("after delete: %+v", list)
	}
}

func testNodes(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	now := s.Now()
	if _, err := s.RegisterNode(ctx, identity.Node{Mode: "search"}); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("address required: %v", err)
	}
	a, err := s.RegisterNode(ctx, identity.Node{Mode: "search", Address: "[2001:db8::10]:9100", Host: "node1", Version: "v1", Expires: now.Add(30 * time.Second)})
	if err != nil || a.ID == 0 || a.LastSeen.IsZero() || a.Started.IsZero() {
		t.Fatalf("register: %+v %v", a, err)
	}
	b, _ := s.RegisterNode(ctx, identity.Node{Mode: "search", Address: "[2001:db8::11]:9100", Host: "node2", Expires: now.Add(30 * time.Second)})
	_, _ = s.RegisterNode(ctx, identity.Node{Mode: "jmap", Address: "[2001:db8::10]:8080", Host: "node1", Expires: now.Add(30 * time.Second)})
	if b.ID == a.ID {
		t.Fatal("ids must differ")
	}
	// Heartbeat: same row, refreshed.
	a2, err := s.RegisterNode(ctx, identity.Node{Mode: "search", Address: "[2001:db8::10]:9100", Host: "node1", Version: "v2", Started: a.Started, Expires: now.Add(60 * time.Second)})
	if err != nil || a2.ID != a.ID || a2.Version != "v2" || !a2.Expires.After(a.Expires) {
		t.Fatalf("heartbeat: %+v %v", a2, err)
	}
	list, err := s.ListNodes(ctx, "search")
	if err != nil || len(list) != 2 || list[0].Address != "[2001:db8::10]:9100" {
		t.Fatalf("list search: %+v %v", list, err)
	}
	if all, _ := s.ListNodes(ctx, ""); len(all) != 3 || all[0].Mode != "jmap" {
		t.Fatalf("list all: %+v", all)
	}
	if !list[0].Alive(now.Add(59*time.Second)) || list[0].Alive(now.Add(61*time.Second)) {
		t.Fatal("alive window")
	}
	// Purge removes only expired rows; delete is idempotent.
	n, err := s.PurgeNodes(ctx, now.Add(45*time.Second))
	if err != nil || n != 2 {
		t.Fatalf("purge: %d %v", n, err)
	}
	if list, _ := s.ListNodes(ctx, ""); len(list) != 1 || list[0].Mode != "search" {
		t.Fatalf("after purge: %+v", list)
	}
	if err := s.DeleteNode(ctx, "search", "[2001:db8::10]:9100"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNode(ctx, "search", "[2001:db8::10]:9100"); err != nil {
		t.Fatalf("double delete: %v", err)
	}
	if list, _ := s.ListNodes(ctx, ""); len(list) != 0 {
		t.Fatalf("after delete: %+v", list)
	}
}

func testNamespaces(t *testing.T, f Factory) {
	s := f(t)
	ctx := context.Background()
	// The default namespace exists without being created.
	def, err := s.GetNamespace(ctx, "")
	if err != nil || def.Value.Name != identity.DefaultNamespace || !def.Value.Enabled {
		t.Fatalf("default: %+v %v", def.Value, err)
	}
	if _, err := s.CreateNamespace(ctx, identity.Namespace{Name: "default"}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("default exists: %v", err)
	}
	for _, bad := range []string{"Bad Name", "-x", "x-", "a.b", "toolongtoolongtoolongtoolongtoolongtoolongtoolongtoolongtoolongtoolong"} {
		if _, err := s.CreateNamespace(ctx, identity.Namespace{Name: bad}); !errors.Is(err, identity.ErrInvalid) {
			t.Fatalf("%q: expected invalid, got %v", bad, err)
		}
	}
	ns, err := s.CreateNamespace(ctx, identity.Namespace{Name: " ACME ", Enabled: true, Settings: map[string]string{"backup.interval": "12h"}})
	if err != nil || ns.Name != "acme" || !ns.Enabled {
		t.Fatalf("create: %+v %v", ns, err)
	}
	if _, err := s.CreateNamespace(ctx, identity.Namespace{Name: "acme"}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("duplicate: %v", err)
	}
	names, more, err := s.ListNamespaces(ctx, "", 10)
	if err != nil || more || len(names) != 2 || names[0] != "acme" || names[1] != "default" {
		t.Fatalf("list: %v %v %v", names, more, err)
	}
	upd, err := s.UpdateNamespace(ctx, "acme", func(n *identity.Namespace) error { n.Enabled = false; return nil })
	if err != nil || upd.Enabled || upd.Settings["backup.interval"] != "12h" {
		t.Fatalf("update: %+v %v", upd, err)
	}
	if _, err := s.UpdateNamespace(ctx, "acme", func(n *identity.Namespace) error { n.Name = "other"; return nil }); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("rename: %v", err)
	}
	if g, _ := s.GetNamespace(ctx, "ACME"); g.Value.Enabled {
		t.Fatal("update not persisted")
	}
	if err := s.DeleteNamespace(ctx, "default"); !errors.Is(err, identity.ErrInUse) {
		t.Fatalf("delete default: %v", err)
	}

	// Ownership: domains, mailboxes and storage carry their namespace;
	// accounts and aliases never link across namespaces.
	if _, err := s.CreateDomain(ctx, identity.Domain{Name: "nope.example", Namespace: "nope", Enabled: true}); !errors.Is(err, identity.ErrNamespaceUnknown) {
		t.Fatalf("domain in unknown namespace: %v", err)
	}
	d, err := s.CreateDomain(ctx, identity.Domain{Name: "acme.example", Namespace: "acme", Enabled: true})
	if err != nil || d.Namespace != "acme" {
		t.Fatalf("domain: %+v %v", d, err)
	}
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	if names, _, _ := s.ListDomains(ctx, "acme", "", 10); len(names) != 1 || names[0] != "acme.example" {
		t.Fatalf("domains of acme: %v", names)
	}
	if names, _, _ := s.ListDomains(ctx, "default", "", 10); len(names) != 1 || names[0] != "example.org" {
		t.Fatalf("domains of default: %v", names)
	}
	if _, err := s.UpdateDomain(ctx, "acme.example", func(x *identity.Domain) error { x.Namespace = "default"; return nil }); err != nil {
		t.Fatal(err)
	}
	if g, _ := s.GetDomain(ctx, "acme.example"); g.Value.Namespace != "acme" {
		t.Fatalf("domain namespace must be immutable: %+v", g.Value)
	}
	if err := s.DeleteNamespace(ctx, "acme"); !errors.Is(err, identity.ErrInUse) {
		t.Fatalf("delete with domains: %v", err)
	}

	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	fs := uuid.New()
	defStorage, err := s.CreateStorage(ctx, identity.Storage{Name: "shared-default", FSID: fs, MonHost: "x", Pools: pools, Placement: true})
	if err != nil || defStorage.Namespace != identity.DefaultNamespace {
		t.Fatalf("storage: %+v %v", defStorage, err)
	}
	// Same cluster, another namespace: fine with other pools, refused with a shared one.
	if _, err := s.CreateStorage(ctx, identity.Storage{Name: "shared-acme", Namespace: "acme", FSID: fs, MonHost: "x", Pools: identity.Pools{Messages: "m2", Attachments: "a2", Index: "i"}}); !errors.Is(err, identity.ErrExists) {
		t.Fatalf("shared pool on one cluster: %v", err)
	}
	acmeStorage, err := s.CreateStorage(ctx, identity.Storage{Name: "shared-acme", Namespace: "acme", FSID: fs, MonHost: "x", Pools: identity.Pools{Messages: "m2", Attachments: "a2", Index: "i2"}, Placement: true})
	if err != nil || acmeStorage.Namespace != "acme" {
		t.Fatalf("storage in acme: %+v %v", acmeStorage, err)
	}
	if _, err := s.UpdateStorage(ctx, acmeStorage.ID, func(c *identity.Storage) error { c.Namespace = "default"; return nil }); !errors.Is(err, identity.ErrInvalid) {
		t.Fatalf("storage namespace must be immutable: %v", err)
	}

	if _, err := s.CreateMailbox(ctx, identity.Mailbox{Namespace: "acme", StorageID: defStorage.ID}); !errors.Is(err, identity.ErrNamespaceMismatch) {
		t.Fatalf("mailbox on another namespace's storage: %v", err)
	}
	acmeMB, err := s.CreateMailbox(ctx, identity.Mailbox{Namespace: "acme", StorageID: acmeStorage.ID})
	if err != nil || acmeMB.Namespace != "acme" {
		t.Fatalf("mailbox: %+v %v", acmeMB, err)
	}
	defMB, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	if defMB.Namespace != identity.DefaultNamespace {
		t.Fatalf("mailbox default namespace: %+v", defMB)
	}
	if _, err := s.UpdateMailbox(ctx, acmeMB.ID, func(m *identity.Mailbox) error { m.Namespace = "default"; return nil }); err != nil {
		t.Fatal(err)
	}
	if g, _ := s.GetMailbox(ctx, acmeMB.ID); g.Value.Namespace != "acme" {
		t.Fatalf("mailbox namespace must be immutable: %+v", g.Value)
	}
	var ids []uuid.UUID
	_ = s.ListMailboxes(ctx, "acme", func(id uuid.UUID) error { ids = append(ids, id); return nil })
	if len(ids) != 1 || ids[0] != acmeMB.ID {
		t.Fatalf("mailboxes of acme: %v", ids)
	}
	ids = nil
	_ = s.ListMailboxes(ctx, "", func(id uuid.UUID) error { ids = append(ids, id); return nil })
	if len(ids) != 2 {
		t.Fatalf("all mailboxes: %v", ids)
	}

	if _, err := s.CreateAccount(ctx, identity.Account{Username: "u@acme.example", MailboxID: defMB.ID, Roles: []string{identity.RoleRead}, Enabled: true}); !errors.Is(err, identity.ErrNamespaceMismatch) {
		t.Fatalf("account across namespaces: %v", err)
	}
	a, err := s.CreateAccount(ctx, identity.Account{Username: "u@acme.example", MailboxID: acmeMB.ID, Roles: []string{identity.RoleRead}, Enabled: true})
	if err != nil || a.Namespace != "acme" {
		t.Fatalf("account: %+v %v", a, err)
	}
	if g, _ := s.GetAccountByUsername(ctx, "u@acme.example"); g.Value.Namespace != "acme" {
		t.Fatalf("account namespace not read back: %+v", g.Value)
	}
	if _, err := s.RelinkAccount(ctx, a.ID, defMB.ID); !errors.Is(err, identity.ErrNamespaceMismatch) {
		t.Fatalf("relink across namespaces: %v", err)
	}
	if _, err := s.CreateAlias(ctx, identity.Alias{Address: "info@acme.example", Enabled: true, Destinations: []identity.Destination{{MailboxID: defMB.ID}}}); !errors.Is(err, identity.ErrNamespaceMismatch) {
		t.Fatalf("alias across namespaces: %v", err)
	}
	al, err := s.CreateAlias(ctx, identity.Alias{Address: "info@acme.example", Enabled: true, Destinations: []identity.Destination{{MailboxID: acmeMB.ID}, {Address: "x@example.org"}}})
	if err != nil || al.Namespace != "acme" {
		t.Fatalf("alias: %+v %v", al, err)
	}
	if _, err := s.UpdateAlias(ctx, "info@acme.example", func(x *identity.Alias) error {
		x.Destinations = []identity.Destination{{MailboxID: defMB.ID}}
		return nil
	}); !errors.Is(err, identity.ErrNamespaceMismatch) {
		t.Fatalf("alias retarget across namespaces: %v", err)
	}

	// Nodes record what they serve.
	n, err := s.RegisterNode(ctx, identity.Node{Mode: "imap", Address: "[2001:db8::1]:143", Namespaces: []string{"ACME", "acme", "default"}, Expires: s.Now().Add(time.Minute)})
	if err != nil || len(n.Namespaces) != 2 || n.Namespaces[0] != "acme" || !n.Serves("acme") || n.Serves("other") {
		t.Fatalf("node namespaces: %+v %v", n, err)
	}
	all, _ := s.RegisterNode(ctx, identity.Node{Mode: "imap", Address: "[2001:db8::2]:143", Expires: s.Now().Add(time.Minute)})
	if len(all.Namespaces) != 0 || !all.Serves("anything") {
		t.Fatalf("node serving all: %+v", all)
	}
	if list, _ := s.ListNodes(ctx, "imap"); len(list) != 2 || len(list[0].Namespaces) != 2 || len(list[1].Namespaces) != 0 {
		t.Fatalf("listed node namespaces: %+v", list)
	}
}
