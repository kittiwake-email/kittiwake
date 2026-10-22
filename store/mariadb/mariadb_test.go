package mariadb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/identity/identitytest"
	"kittiwake/store/mariadb"
	"kittiwake/store/queue"
	"kittiwake/store/queue/queuetest"
	"kittiwake/store/session"
	"kittiwake/store/session/sessiontest"
)

// The MariaDB backend is tested against a real server:
//
//	KITTIWAKE_TEST_DSN='kittiwake:kittiwake@tcp(127.0.0.1:3307)/kittiwake' go test ./store/mariadb/
//
// Every table is truncated before each conformance test.
func open(t *testing.T) *mariadb.DB {
	t.Helper()
	dsn := os.Getenv("KITTIWAKE_TEST_DSN")
	if dsn == "" {
		t.Skip("KITTIWAKE_TEST_DSN not set")
	}
	db, err := mariadb.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"TRUNCATE alias_destinations", "TRUNCATE aliases", "TRUNCATE account_passwords", "TRUNCATE tokens", "TRUNCATE accounts",
		"TRUNCATE mailboxes", "TRUNCATE domains", "TRUNCATE clusters", "TRUNCATE signing_keys", "TRUNCATE sessions",
		"TRUNCATE totp_steps", "TRUNCATE tasks", "TRUNCATE scheduler", "TRUNCATE scheduler_lease", "TRUNCATE audit_admin",
		"SET FOREIGN_KEY_CHECKS = 1",
	} {
		if _, err := db.SQL().ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return db
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	v1, err := db.Migrate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := db.Migrate(ctx)
	if err != nil || v2 != v1 || v1 != mariadb.LatestVersion() {
		t.Fatalf("migrate twice: %d %d %v", v1, v2, err)
	}
	if got, _ := db.SchemaVersion(ctx); got != v1 {
		t.Fatalf("schema version %d, want %d", got, v1)
	}
}

func TestIdentityConformance(t *testing.T) {
	identitytest.Run(t, func(t *testing.T) *identity.Store {
		s := identity.NewWith(open(t))
		now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
		s.Now = func() time.Time { return now }
		return s
	})
}

func TestSessionConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) session.Store { return open(t) })
}

func TestQueueConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, now *time.Time) queue.Store { return open(t) })
}

func TestAdminAuditLog(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	db.Now = func() time.Time { return now }
	recs := []audit.Record{
		{Time: now, Account: "admin:ops", Admin: "admin:ops", Protocol: audit.ProtoAdmin, Action: "domain-create", Target: "example.org", Result: audit.ResultOK, ClientIP: "::1"},
		{Time: now.Add(time.Minute), Protocol: audit.ProtoCLI, Action: "token-create", Result: audit.ResultOK},
		{Time: now.Add(-48 * time.Hour), Protocol: audit.ProtoAdmin, Action: "domain-create", Result: audit.ResultOK},
	}
	if err := db.WriteAdmin(ctx, recs); err != nil {
		t.Fatal(err)
	}
	got, next, err := db.ReadAdmin(ctx, audit.Query{From: now, Max: 10})
	if err != nil || len(got) != 2 || next != "" || got[0].Action != "domain-create" || got[1].Action != "token-create" {
		t.Fatalf("read today: %+v %q %v", got, next, err)
	}
	got, next, _ = db.ReadAdmin(ctx, audit.Query{From: now, Max: 1})
	if len(got) != 1 || next == "" {
		t.Fatalf("page 1: %+v %q", got, next)
	}
	got, next, _ = db.ReadAdmin(ctx, audit.Query{From: now, Max: 1, After: next})
	if len(got) != 1 || got[0].Action != "token-create" || next != "" {
		t.Fatalf("page 2: %+v %q", got, next)
	}
	got, _, _ = db.ReadAdmin(ctx, audit.Query{From: now, Action: "domain-create", Account: "ADMIN:ops", Max: 10})
	if len(got) != 1 {
		t.Fatalf("filters: %+v", got)
	}
	got, _, _ = db.ReadAdmin(ctx, audit.Query{From: now.Add(-72 * time.Hour), To: now, Max: 10})
	if len(got) != 3 {
		t.Fatalf("range: %+v", got)
	}
	n, err := db.PurgeAdminBefore(ctx, now.Add(-24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
}
