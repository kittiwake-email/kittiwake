// Package mariadb is the production metadata backend: domains, mailboxes,
// accounts, aliases, tokens, signing keys, sessions, the worker queue and
// the Ceph cluster registry in MariaDB (10.6 or newer). It implements
// identity.Backend, session.Store and queue.Store on one connection pool.
//
// Schema migrations are numbered SQL files embedded in the binary and
// applied by Migrate (the `kittiwake migrate` command), under a server-side
// lock. MariaDB commits every DDL statement on its own, so each statement
// is written to be idempotent and a migration that failed half-way is
// simply re-run; the version row is written after the file succeeded.
package mariadb

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"kittiwake/store/identity"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// DB is an open metadata database.
type DB struct {
	sql *sql.DB
	Now func() time.Time
	// Queue tuning (see queue.Rados for the defaults).
	MaxAttempts int
	Backoff     func(attempt int) time.Duration
}

// Open connects to the DSN (go-sql-driver/mysql format, e.g.
// "kittiwake:secret@tcp(db.example:3306)/kittiwake"). Times are always
// exchanged in UTC with microsecond precision whatever the DSN says.
func Open(dsn string) (*DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mariadb: dsn: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Params["sql_mode"] = "'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'"
	cfg.Collation = "utf8mb4_unicode_ci"
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mariadb: %w", err)
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &DB{sql: db, Now: time.Now, MaxAttempts: 8, Backoff: defaultBackoff}, nil
}

func defaultBackoff(n int) time.Duration {
	d := time.Duration(1<<uint(min(n, 10))) * 30 * time.Second
	if d > 6*time.Hour {
		d = 6 * time.Hour
	}
	return d
}

// SetNow installs the clock (identity.Backend, session.Store, queue.Store).
func (d *DB) SetNow(now func() time.Time) { d.Now = now }

// SetRetryPolicy sets attempts before dead-lettering and the backoff (queue.Store).
func (d *DB) SetRetryPolicy(maxAttempts int, backoff func(int) time.Duration) {
	d.MaxAttempts, d.Backoff = maxAttempts, backoff
}

// Close closes the pool.
func (d *DB) Close() error { return d.sql.Close() }

// Ping checks the connection.
func (d *DB) Ping(ctx context.Context) error { return d.sql.PingContext(ctx) }

// SQL exposes the pool (tests, status).
func (d *DB) SQL() *sql.DB { return d.sql }

// ---- migrations ----

// LatestVersion is the schema version this binary expects.
func LatestVersion() int {
	files, _ := migrationFiles()
	if len(files) == 0 {
		return 0
	}
	return files[len(files)-1].version
}

type migration struct {
	version int
	name    string
	body    string
}

func migrationFiles() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		v, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			return nil, fmt.Errorf("mariadb: migration %s: bad version prefix", name)
		}
		b, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: name, body: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// SchemaVersion returns the applied schema version (0 = empty database).
func (d *DB) SchemaVersion(ctx context.Context) (int, error) {
	var v sql.NullInt64
	err := d.sql.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_version").Scan(&v)
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1146 { // table does not exist
			return 0, nil
		}
		return 0, err
	}
	return int(v.Int64), nil
}

// Migrate applies every migration above the current version and returns
// the version now in place. Safe to run concurrently (server-side lock)
// and to re-run after a failure.
func (d *DB) Migrate(ctx context.Context) (int, error) {
	conn, err := d.sql.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK('kittiwake_migrate', 60)").Scan(&got); err != nil {
		return 0, err
	}
	if !got.Valid || got.Int64 != 1 {
		return 0, errors.New("mariadb: another migration is running")
	}
	defer func() { _, _ = conn.ExecContext(ctx, "SELECT RELEASE_LOCK('kittiwake_migrate')") }()

	cur, err := d.SchemaVersion(ctx)
	if err != nil {
		return 0, err
	}
	files, err := migrationFiles()
	if err != nil {
		return 0, err
	}
	for _, m := range files {
		if m.version <= cur {
			continue
		}
		for _, stmt := range splitStatements(m.body) {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return cur, fmt.Errorf("mariadb: migration %s: %w\n%s", m.name, err, stmt)
			}
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)", m.version, d.Now().UTC()); err != nil {
			return cur, err
		}
		cur = m.version
	}
	return cur, nil
}

// splitStatements splits a migration file on ";" at line ends, dropping
// "--" comment lines. Statement bodies never contain ";\n" themselves.
func splitStatements(body string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(body, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteString("\n")
		if strings.HasSuffix(t, ";") {
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// ---- helpers ----

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return identity.ErrNotFound
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1062:
			return fmt.Errorf("%w: %s", identity.ErrExists, me.Message)
		case 1451:
			return fmt.Errorf("%w: %s", identity.ErrInUse, me.Message)
		case 1452:
			return fmt.Errorf("%w: %s", identity.ErrNotFound, me.Message)
		case 1213, 1205: // deadlock, lock wait timeout
			return fmt.Errorf("%w: %s", identity.ErrConflict, me.Message)
		}
	}
	return err
}

func bin(u uuid.UUID) []byte { return u[:] }

func nullBin(u uuid.UUID) any {
	if u == uuid.Nil {
		return nil
	}
	return u[:]
}

func toUUID(b []byte) uuid.UUID {
	u, _ := uuid.FromBytes(b)
	return u
}

func joinList(ss []string) string { return strings.Join(ss, ",") }

func splitList(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

func nullTimeValue(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

// tx runs fn in a transaction, retrying on deadlock.
func (d *DB) tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		tx, err := d.sql.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			last = mapErr(err)
			if errors.Is(last, identity.ErrConflict) {
				continue
			}
			return last
		}
		if err := tx.Commit(); err != nil {
			last = mapErr(err)
			if errors.Is(last, identity.ErrConflict) {
				continue
			}
			return last
		}
		return nil
	}
	return last
}

// likePrefix escapes a prefix for LIKE.
func likePrefix(p string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(p) + "%"
}
