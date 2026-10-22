package identity_test

import (
	"context"
	"testing"
	"time"

	"dovetail/rados"
	"dovetail/store/identity"
	"dovetail/store/identity/identitytest"
)

func memoryStore(t *testing.T) *identity.Store {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	s := identity.New(meta)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	return s
}

func TestRadosBackend(t *testing.T) { identitytest.Run(t, memoryStore) }

// The RADOS backend keeps a name pointer per account; deleting the account
// must remove it (the conformance suite cannot see objects).
func TestRadosNamePointerRemoved(t *testing.T) {
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	s := identity.New(meta)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := s.CreateMailbox(ctx, identity.Mailbox{})
	a, err := s.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{identity.RoleRead}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if objs := c.Objects("meta", "names"); len(objs) != 0 {
		t.Fatalf("name pointers: %v", objs)
	}
}

// The RADOS backend uses optimistic concurrency: a writer that slips in
// between read and write makes the mutator run again.
func TestRadosUpdateRetries(t *testing.T) {
	s := memoryStore(t)
	ctx := context.Background()
	_, _ = s.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	calls := 0
	_, err := s.UpdateDomain(ctx, "example.org", func(d *identity.Domain) error {
		calls++
		if calls == 1 {
			_, _ = s.UpdateDomain(ctx, "example.org", func(d *identity.Domain) error { d.Settings = map[string]string{"x": "1"}; return nil })
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
		if _, _, err := identity.NormalizeAddress(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	a, d, err := identity.NormalizeAddress("  Wido.Den.Hollander@Example.ORG ")
	if err != nil || a != "wido.den.hollander@example.org" || d != "example.org" {
		t.Fatal(a, d, err)
	}
}
