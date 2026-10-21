package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

func newStore(t *testing.T) (*Store, *rados.MemoryClient) {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	s := New(meta)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	return s, c
}

func TestDomains(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	d, err := s.CreateDomain(ctx, Domain{Name: "Example.ORG", Enabled: true})
	if err != nil || d.Name != "example.org" {
		t.Fatalf("%+v %v", d, err)
	}
	if _, err := s.CreateDomain(ctx, Domain{Name: "example.org"}); !errors.Is(err, ErrExists) {
		t.Fatalf("expected exists, got %v", err)
	}
	if _, err := s.CreateDomain(ctx, Domain{Name: "bad domain"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid, got %v", err)
	}
	_, _ = s.CreateDomain(ctx, Domain{Name: "a.example", Enabled: true})
	names, more, err := s.ListDomains(ctx, "", 10)
	if err != nil || more || len(names) != 2 || names[0] != "a.example" || names[1] != "example.org" {
		t.Fatalf("list: %v %v %v", names, more, err)
	}
	upd, err := s.UpdateDomain(ctx, "example.org", func(d *Domain) error { d.Enabled = false; return nil })
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
	if _, err := s.GetDomain(ctx, "a.example"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestAccountsAndMailboxes(t *testing.T) {
	s, c := newStore(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, Domain{Name: "example.org", Enabled: true})
	mb, err := s.CreateMailbox(ctx, Mailbox{Enabled: true, Quota: QuotaLimits{HardBytes: 1 << 30}})
	if err != nil || mb.ID == uuid.Nil {
		t.Fatal(err)
	}

	// Domain must exist.
	if _, err := s.CreateAccount(ctx, Account{Username: "x@nope.org", MailboxID: mb.ID, Roles: []string{RoleRead}}); !errors.Is(err, ErrDomainUnknown) {
		t.Fatalf("expected domain unknown, got %v", err)
	}
	// Mailbox must exist.
	if _, err := s.CreateAccount(ctx, Account{Username: "x@example.org", MailboxID: uuid.New(), Roles: []string{RoleRead}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	// Bad role.
	if _, err := s.CreateAccount(ctx, Account{Username: "x@example.org", MailboxID: mb.ID, Roles: []string{"god"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid, got %v", err)
	}

	a, err := s.CreateAccount(ctx, Account{
		Username:  "Wido@Example.org",
		MailboxID: mb.ID,
		Roles:     []string{RoleRead, RoleWrite, RoleSend},
		Passwords: []Password{{Hash: "{PLAIN}x"}},
		Enabled:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "wido@example.org" || a.Domain != "example.org" || a.Passwords[0].Kind != PasswordPrimary || a.Passwords[0].ID == "" {
		t.Fatalf("normalisation: %+v", a)
	}
	// Uniqueness.
	if _, err := s.CreateAccount(ctx, Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{RoleRead}}); !errors.Is(err, ErrExists) {
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
	// Mailbox in use.
	if err := s.DeleteMailbox(ctx, mb.ID); !errors.Is(err, ErrInUse) {
		t.Fatalf("expected in use, got %v", err)
	}
	// Domain in use.
	if err := s.DeleteDomain(ctx, "example.org"); !errors.Is(err, ErrInUse) {
		t.Fatalf("expected in use, got %v", err)
	}
	// Update.
	upd, err := s.UpdateAccount(ctx, a.ID, func(x *Account) error { x.Enabled = false; x.Roles = []string{RoleRead}; return nil })
	if err != nil || upd.Enabled || len(upd.Roles) != 1 {
		t.Fatalf("update: %+v %v", upd, err)
	}
	if _, err := s.UpdateAccount(ctx, a.ID, func(x *Account) error { x.Username = "other@example.org"; return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid rename, got %v", err)
	}
	// Admin accounts cannot have a mailbox; must have no mailbox.
	if _, err := s.CreateAccount(ctx, Account{Username: "admin@example.org", MailboxID: mb.ID, Roles: []string{RoleAdmin}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid, got %v", err)
	}
	adm, err := s.CreateAccount(ctx, Account{Username: "admin@example.org", Roles: []string{RoleAdmin}, Enabled: true})
	if err != nil || !adm.IsAdmin() {
		t.Fatal(err)
	}
	// Relink.
	mb2, _ := s.CreateMailbox(ctx, Mailbox{Enabled: true})
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
	if _, err := s.GetAccountByUsername(ctx, "wido@example.org"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if objs := c.Objects("meta", nsNames); len(objs) != 1 || objs[0] != "admin@example.org" {
		t.Fatalf("name pointers: %v", objs)
	}
	if err := s.DeleteMailbox(ctx, mb.ID); err != nil {
		t.Fatal(err)
	}
	var ids []uuid.UUID
	_ = s.ListMailboxes(ctx, func(id uuid.UUID) error { ids = append(ids, id); return nil })
	if len(ids) != 1 || ids[0] != mb2.ID {
		t.Fatalf("mailboxes: %v", ids)
	}
}

func TestAliases(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, Domain{Name: "example.org", Enabled: true})
	mb, _ := s.CreateMailbox(ctx, Mailbox{Enabled: true})
	_, _ = s.CreateAccount(ctx, Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{RoleRead}, Enabled: true})

	if _, err := s.CreateAlias(ctx, Alias{Address: "wido@example.org", Destinations: []Destination{{MailboxID: mb.ID}}}); !errors.Is(err, ErrExists) {
		t.Fatalf("alias over account should fail: %v", err)
	}
	if _, err := s.CreateAlias(ctx, Alias{Address: "info@example.org"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("no destinations: %v", err)
	}
	if _, err := s.CreateAlias(ctx, Alias{Address: "info@example.org", Destinations: []Destination{{MailboxID: mb.ID, Address: "x@example.org"}}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("both destination kinds: %v", err)
	}
	if _, err := s.CreateAlias(ctx, Alias{Address: "info@example.org", Destinations: []Destination{{Address: "info@example.org"}}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("self alias: %v", err)
	}
	al, err := s.CreateAlias(ctx, Alias{Address: "Info@Example.org", Enabled: true, Destinations: []Destination{{MailboxID: mb.ID}, {Address: "Wido@example.org"}}})
	if err != nil || al.Address != "info@example.org" || al.Destinations[1].Address != "wido@example.org" {
		t.Fatalf("%+v %v", al, err)
	}
	ca, err := s.CreateAlias(ctx, Alias{Address: "@example.org", Enabled: true, Destinations: []Destination{{MailboxID: mb.ID}}})
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
	mb2, _ := s.CreateMailbox(ctx, Mailbox{Enabled: true})
	_, _ = s.UpdateAlias(ctx, "info@example.org", func(a *Alias) error { a.Destinations = []Destination{{MailboxID: mb2.ID}}; return nil })
	if mal, _ = s.ListMailboxAliases(ctx, mb.ID); len(mal) != 1 {
		t.Fatalf("mailbox aliases after retarget: %v", mal)
	}
	if mal, _ = s.ListMailboxAliases(ctx, mb2.ID); len(mal) != 1 || mal[0] != "info@example.org" {
		t.Fatalf("new mailbox aliases: %v", mal)
	}
	_, _ = s.UpdateAlias(ctx, "info@example.org", func(a *Alias) error {
		a.Destinations = []Destination{{MailboxID: mb.ID}, {Address: "wido@example.org"}}
		return nil
	})
	if _, err := s.UpdateAlias(ctx, "info@example.org", func(a *Alias) error { a.Enabled = false; return nil }); err != nil {
		t.Fatal(err)
	}
	g, _ := s.GetAlias(ctx, "INFO@example.org")
	if g.Value.Enabled {
		t.Fatal("update lost")
	}
	if err := s.DeleteAlias(ctx, "info@example.org"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAlias(ctx, "info@example.org"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	addrs, _, _ = s.ListDomainAliases(ctx, "example.org", "", 10)
	if len(addrs) != 1 {
		t.Fatalf("list after delete: %v", addrs)
	}
	if mal, _ := s.ListMailboxAliases(ctx, mb.ID); len(mal) != 1 || mal[0] != "@example.org" {
		t.Fatalf("mailbox aliases after delete: %v", mal)
	}
}

func TestTokens(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, Domain{Name: "example.org", Enabled: true})
	mb, _ := s.CreateMailbox(ctx, Mailbox{Enabled: true})
	a, _ := s.CreateAccount(ctx, Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{RoleRead, RoleWrite}, Enabled: true})
	exp := s.Now().Add(24 * time.Hour)

	if _, _, err := s.CreateToken(ctx, a.ID, nil, exp, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("comment required: %v", err)
	}
	if _, _, err := s.CreateToken(ctx, a.ID, nil, s.Now(), "x"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expiry required: %v", err)
	}
	if _, _, err := s.CreateToken(ctx, a.ID, []string{RoleSend}, exp, "x"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("role escalation: %v", err)
	}
	tok, secret, err := s.CreateToken(ctx, a.ID, []string{RoleRead}, exp, "phone app")
	if err != nil {
		t.Fatal(err)
	}
	if secret[:4] != "dvt_" || tok.Hash != HashToken(secret) || len(tok.Roles) != 1 {
		t.Fatalf("token: %+v %s", tok, secret)
	}
	got, err := s.LookupToken(ctx, secret)
	if err != nil || got.Value.ID != tok.ID || got.Value.AccountID != a.ID {
		t.Fatalf("lookup: %+v %v", got, err)
	}
	if _, err := s.LookupToken(ctx, "dvt_nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := s.LookupToken(ctx, "nope"); !errors.Is(err, ErrNotFound) {
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
	if _, err := s.LookupToken(ctx, secret); !errors.Is(err, ErrNotFound) {
		t.Fatal("token not revoked")
	}
	if err := s.DeleteToken(ctx, a.ID, tok.ID); !errors.Is(err, ErrNotFound) {
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

func TestConcurrentUpdateRetries(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, Domain{Name: "example.org", Enabled: true})
	calls := 0
	_, err := s.UpdateDomain(ctx, "example.org", func(d *Domain) error {
		calls++
		if calls == 1 {
			// Simulate a concurrent writer between read and write.
			_, _ = s.UpdateDomain(ctx, "example.org", func(d *Domain) error { d.Settings = map[string]string{"x": "1"}; return nil })
		}
		d.Enabled = false
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("retry: calls=%d err=%v", calls, err)
	}
	got, _ := s.GetDomain(ctx, "example.org")
	if got.Value.Enabled || got.Value.Settings["x"] != "1" {
		t.Fatalf("lost update: %+v", got.Value)
	}
}

func TestNormalizeAddress(t *testing.T) {
	for _, bad := range []string{"", "nouser", "@x.org", "a@", "a b@x.org", "a@x\norg"} {
		if _, _, err := NormalizeAddress(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	a, d, err := NormalizeAddress("  Wido.Den.Hollander@Example.ORG ")
	if err != nil || a != "wido.den.hollander@example.org" || d != "example.org" {
		t.Fatal(a, d, err)
	}
}
