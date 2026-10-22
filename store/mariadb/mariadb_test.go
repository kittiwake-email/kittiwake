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

// Read/write split: with a read DSN, SELECT-only operations use the
// replica except within the stickiness window after a write. The test
// opens the same server as both roles (KITTIWAKE_TEST_READ_DSN overrides
// the replica, e.g. a real one) and watches the replica query counter.
func TestReadWriteSplit(t *testing.T) {
	dsn := os.Getenv("KITTIWAKE_TEST_DSN")
	if dsn == "" {
		t.Skip("KITTIWAKE_TEST_DSN not set")
	}
	readDSN := os.Getenv("KITTIWAKE_TEST_READ_DSN")
	if readDSN == "" {
		readDSN = dsn
	}
	open(t) // migrated and truncated
	db, err := mariadb.OpenSplit(dsn, readDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.ReadStickiness = 300 * time.Millisecond
	ctx := context.Background()
	if !db.HasReplica() {
		t.Fatal("replica not configured")
	}
	ids := identity.NewWith(db)
	// Nothing written yet: a lookup goes to the replica.
	_, _, _ = ids.ListDomains(ctx, "", 10)
	if db.ReaderQueries() != 1 {
		t.Fatalf("first read not on the replica: %d", db.ReaderQueries())
	}
	// A write pins reads to the primary for the stickiness window...
	if _, err := ids.CreateDomain(ctx, identity.Domain{Name: "split.example", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := ids.GetDomain(ctx, "split.example"); err != nil {
		t.Fatalf("read-your-write: %v", err)
	}
	if db.ReaderQueries() != 1 {
		t.Fatalf("read after write went to the replica: %d", db.ReaderQueries())
	}
	// ...and then they return to the replica, which (when it is a real
	// replica) has caught up by now.
	time.Sleep(400 * time.Millisecond)
	d, err := ids.GetDomain(ctx, "split.example")
	if err != nil || !d.Value.Enabled {
		t.Fatalf("replica read: %+v %v", d, err)
	}
	if db.ReaderQueries() != 2 {
		t.Fatalf("read after the window not on the replica: %d", db.ReaderQueries())
	}
	// Transactions and writes never touch the replica.
	if _, err := ids.UpdateDomain(ctx, "split.example", func(x *identity.Domain) error { x.Enabled = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if db.ReaderQueries() != 2 {
		t.Fatalf("transaction used the replica: %d", db.ReaderQueries())
	}
}
