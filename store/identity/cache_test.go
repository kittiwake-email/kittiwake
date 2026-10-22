package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"kittiwake/rados"
)

func TestCachedLookups(t *testing.T) {
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	s := New(meta)
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	s.CacheTTL = 5 * time.Second
	ctx := context.Background()
	reads := func() int { return meta.(interface{ Reads() int }).Reads() }

	// Not-found is cached, and creating the object drops that entry.
	if _, err := s.GetDomainCached(ctx, "example.org"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	n := reads()
	if _, err := s.GetDomainCached(ctx, "example.org"); !errors.Is(err, ErrNotFound) || reads() != n {
		t.Fatalf("negative lookup not cached (err %v, reads %d -> %d)", err, n, reads())
	}
	if _, err := s.CreateDomain(ctx, Domain{Name: "example.org", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if d, err := s.GetDomainCached(ctx, "example.org"); err != nil || !d.Enabled {
		t.Fatalf("created domain not visible: %+v %v", d, err)
	}

	// Mailbox: hits cost no reads; an update through this store is visible at once.
	mb, _ := s.CreateMailbox(ctx, Mailbox{})
	if _, err := s.GetMailboxCached(ctx, mb.ID); err != nil {
		t.Fatal(err)
	}
	n = reads()
	for i := 0; i < 10; i++ {
		if _, err := s.GetMailboxCached(ctx, mb.ID); err != nil {
			t.Fatal(err)
		}
	}
	if reads() != n {
		t.Fatalf("cached mailbox reads hit RADOS: %d -> %d", n, reads())
	}
	if _, err := s.UpdateMailbox(ctx, mb.ID, func(m *Mailbox) error { m.State = StateSuspended; return nil }); err != nil {
		t.Fatal(err)
	}
	if m, _ := s.GetMailboxCached(ctx, mb.ID); m.State != StateSuspended {
		t.Fatalf("update not visible through the cache: %s", m.State)
	}

	// Account by username and alias behave the same; the TTL expires entries.
	acct, err := s.CreateAccount(ctx, Account{Username: "wido@example.org", MailboxID: mb.ID, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if a, err := s.GetAccountByUsernameCached(ctx, "WIDO@example.org"); err != nil || a.ID != acct.ID {
		t.Fatalf("account: %+v %v", a, err)
	}
	if _, err := s.UpdateAccount(ctx, acct.ID, func(a *Account) error { a.Enabled = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if a, _ := s.GetAccountByUsernameCached(ctx, "wido@example.org"); a.Enabled {
		t.Fatal("account update not visible through the cache")
	}
	if _, err := s.CreateAlias(ctx, Alias{Address: "all@example.org", Enabled: true, Destinations: []Destination{{MailboxID: mb.ID}}}); err != nil {
		t.Fatal(err)
	}
	if al, err := s.GetAliasCached(ctx, "all@example.org"); err != nil || !al.Enabled {
		t.Fatalf("alias: %+v %v", al, err)
	}
	if err := s.DeleteAlias(ctx, "all@example.org"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAliasCached(ctx, "all@example.org"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted alias still cached: %v", err)
	}

	// Another writer (a second store on the same pool) is seen after the TTL.
	other := New(meta)
	if _, err := other.UpdateMailbox(ctx, mb.ID, func(m *Mailbox) error { m.State = StateActive; return nil }); err != nil {
		t.Fatal(err)
	}
	if m, _ := s.GetMailboxCached(ctx, mb.ID); m.State != StateSuspended {
		t.Fatalf("expected the stale copy within the TTL, got %s", m.State)
	}
	now = now.Add(5 * time.Second)
	if m, _ := s.GetMailboxCached(ctx, mb.ID); m.State != StateActive {
		t.Fatalf("expected a fresh read after the TTL, got %s", m.State)
	}

	// TTL 0: every call reads.
	s.CacheTTL = 0
	n = reads()
	_, _ = s.GetMailboxCached(ctx, mb.ID)
	_, _ = s.GetMailboxCached(ctx, mb.ID)
	if reads() != n+2 {
		t.Fatalf("cache off should read every time: %d -> %d", n, reads())
	}
}
