package audit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
)

func TestWriteReadPurge(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	s := New(io)
	now := time.Date(2026, 10, 21, 23, 59, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	ctx := context.Background()
	mb := uuid.New()
	for i := 0; i < 5; i++ {
		if err := s.Write(ctx, Record{Mailbox: mb, Account: "wido@example.org", Protocol: ProtoIMAP, ClientIP: "2001:db8::1", Action: "fetch", Result: ResultOK}); err != nil {
			t.Fatal(err)
		}
	}
	// Next day, admin action on the mailbox.
	now = now.Add(2 * time.Minute)
	_ = s.Write(ctx, Record{Mailbox: mb, Account: "wido@example.org", Admin: "admin@example.org", Protocol: ProtoAdmin, Action: "hold", Result: ResultOK})
	_ = s.Write(ctx, Record{Account: "admin@example.org", Protocol: ProtoAdmin, Action: "domain-create", Target: "example.org", Result: ResultOK})

	recs, next, err := s.Read(ctx, Query{Mailbox: mb, From: now.Add(-24 * time.Hour), To: now, Max: 3})
	if err != nil || len(recs) != 3 || next == "" {
		t.Fatalf("page1: %d %q %v", len(recs), next, err)
	}
	recs2, next2, _ := s.Read(ctx, Query{Mailbox: mb, From: now.Add(-24 * time.Hour), To: now, Max: 10, After: next})
	if len(recs2) != 3 || next2 != "" || recs2[2].Action != "hold" {
		t.Fatalf("page2: %d %q %+v", len(recs2), next2, recs2)
	}
	recs, _, _ = s.Read(ctx, Query{Mailbox: mb, From: now.Add(-24 * time.Hour), To: now, Action: "hold"})
	if len(recs) != 1 {
		t.Fatalf("filter action: %d", len(recs))
	}
	recs, _, _ = s.Read(ctx, Query{Mailbox: mb, From: now.Add(-24 * time.Hour), To: now, ClientIP: "::1"})
	if len(recs) != 0 {
		t.Fatalf("filter ip: %d", len(recs))
	}
	adm, _, _ := s.Read(ctx, Query{From: now, Account: "admin@example.org"})
	if len(adm) != 2 {
		t.Fatalf("admin log: %d", len(adm))
	}
	n, err := s.PurgeBefore(ctx, mb, now, 3)
	if err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	recs, _, _ = s.Read(ctx, Query{Mailbox: mb, From: now.Add(-24 * time.Hour), To: now})
	if len(recs) != 1 {
		t.Fatalf("after purge: %d", len(recs))
	}
}
