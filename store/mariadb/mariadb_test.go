package mariadb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"dovetail/store/identity"
	"dovetail/store/identity/identitytest"
	"dovetail/store/mariadb"
	"dovetail/store/queue"
	"dovetail/store/queue/queuetest"
	"dovetail/store/session"
	"dovetail/store/session/sessiontest"
)

// The MariaDB backend is tested against a real server:
//
//	DOVETAIL_TEST_DSN='dovetail:dovetail@tcp(127.0.0.1:3307)/dovetail' go test ./store/mariadb/
//
// Every table is truncated before each conformance test.
func open(t *testing.T) *mariadb.DB {
	t.Helper()
	dsn := os.Getenv("DOVETAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("DOVETAIL_TEST_DSN not set")
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
		"TRUNCATE totp_steps", "TRUNCATE tasks", "TRUNCATE scheduler", "TRUNCATE scheduler_lease",
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
