package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/store/identity"
)

// querier is *sql.DB or *sql.Tx.
type querier interface {
	ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

// ---- namespaces ----

const namespaceCols = "id, name, enabled, settings, created, updated"

func scanNamespace(row interface{ Scan(...any) error }) (int64, identity.Namespace, error) {
	var id int64
	var ns identity.Namespace
	var settings sql.NullString
	if err := row.Scan(&id, &ns.Name, &ns.Enabled, &settings, &ns.Created, &ns.Updated); err != nil {
		return 0, identity.Namespace{}, mapErr(err)
	}
	if settings.Valid && settings.String != "" {
		_ = json.Unmarshal([]byte(settings.String), &ns.Settings)
	}
	return id, ns, nil
}

// namespaceID resolves a namespace name ("" = default) to its row id.
func (d *DB) namespaceID(ctx context.Context, q querier, name string) (int64, error) {
	name, err := identity.NormalizeNamespace(name)
	if err != nil {
		return 0, err
	}
	var id int64
	err = q.QueryRowContext(ctx, "SELECT id FROM namespaces WHERE name = ?", name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: %s", identity.ErrNamespaceUnknown, name)
	}
	return id, mapErr(err)
}

func (d *DB) CreateNamespace(ctx context.Context, ns identity.Namespace) (identity.Namespace, error) {
	name, err := identity.NormalizeNamespace(ns.Name)
	if err != nil {
		return identity.Namespace{}, err
	}
	ns.Name = name
	now := d.Now().UTC()
	ns.Created, ns.Updated = now, now
	_, err = d.exec(ctx, "INSERT INTO namespaces (name, enabled, settings, created, updated) VALUES (?, ?, ?, ?, ?)",
		ns.Name, ns.Enabled, settingsJSON(ns.Settings), now, now)
	if err != nil {
		return identity.Namespace{}, mapErr(err)
	}
	return ns, nil
}

func (d *DB) GetNamespace(ctx context.Context, name string) (identity.Versioned[identity.Namespace], error) {
	name, err := identity.NormalizeNamespace(name)
	if err != nil {
		return identity.Versioned[identity.Namespace]{}, err
	}
	_, ns, err := scanNamespace(d.r(ctx).QueryRowContext(ctx, "SELECT "+namespaceCols+" FROM namespaces WHERE name = ?", name))
	return identity.Versioned[identity.Namespace]{Value: ns}, err
}

func (d *DB) UpdateNamespace(ctx context.Context, name string, fn func(*identity.Namespace) error) (identity.Namespace, error) {
	name, err := identity.NormalizeNamespace(name)
	if err != nil {
		return identity.Namespace{}, err
	}
	var out identity.Namespace
	err = d.tx(ctx, func(tx *sql.Tx) error {
		id, ns, err := scanNamespace(tx.QueryRowContext(ctx, "SELECT "+namespaceCols+" FROM namespaces WHERE name = ? FOR UPDATE", name))
		if err != nil {
			return err
		}
		if err := fn(&ns); err != nil {
			return err
		}
		if ns.Name != name {
			return fmt.Errorf("%w: namespace names are immutable", identity.ErrInvalid)
		}
		ns.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE namespaces SET enabled = ?, settings = ?, updated = ? WHERE id = ?", ns.Enabled, settingsJSON(ns.Settings), ns.Updated, id); err != nil {
			return err
		}
		out = ns
		return nil
	})
	return out, err
}

func (d *DB) DeleteNamespace(ctx context.Context, name string) error {
	name, err := identity.NormalizeNamespace(name)
	if err != nil {
		return err
	}
	if name == identity.DefaultNamespace {
		return fmt.Errorf("%w: the default namespace cannot be removed", identity.ErrInUse)
	}
	return d.tx(ctx, func(tx *sql.Tx) error {
		var id int64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM namespaces WHERE name = ? FOR UPDATE", name).Scan(&id); err != nil {
			return err
		}
		for _, c := range []struct{ table, what string }{{"domains", "domains"}, {"mailboxes", "mailboxes"}, {"storage", "storage"}} {
			var n int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table+" WHERE namespace_id = ?", id).Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				return fmt.Errorf("%w: namespace %s has %s", identity.ErrInUse, name, c.what)
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM tokens WHERE namespace_id = ?", id); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "DELETE FROM namespaces WHERE id = ?", id)
		return err
	})
}

func (d *DB) ListNamespaces(ctx context.Context, after string, max int) (names []string, more bool, err error) {
	return d.listStrings(ctx, "SELECT name FROM namespaces WHERE name > ? ORDER BY name LIMIT ?", after, max)
}

// ---- domains ----

const domainCols = "d.id, d.name, n.name, d.enabled, d.settings, d.created, d.updated"
const domainFrom = " FROM domains d JOIN namespaces n ON n.id = d.namespace_id "

func scanDomain(row interface{ Scan(...any) error }) (int64, identity.Domain, error) {
	var id int64
	var d identity.Domain
	var settings sql.NullString
	if err := row.Scan(&id, &d.Name, &d.Namespace, &d.Enabled, &settings, &d.Created, &d.Updated); err != nil {
		return 0, identity.Domain{}, mapErr(err)
	}
	if settings.Valid && settings.String != "" {
		_ = json.Unmarshal([]byte(settings.String), &d.Settings)
	}
	return id, d, nil
}

// domainRow resolves a domain name to its row id and namespace name.
func (d *DB) domainRow(ctx context.Context, q querier, name string) (id int64, namespace string, err error) {
	err = q.QueryRowContext(ctx, "SELECT d.id, n.name"+domainFrom+"WHERE d.name = ?", name).Scan(&id, &namespace)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", fmt.Errorf("%w: %s", identity.ErrDomainUnknown, name)
	}
	return id, namespace, mapErr(err)
}

func (d *DB) domainID(ctx context.Context, q querier, name string) (int64, error) {
	id, _, err := d.domainRow(ctx, q, name)
	return id, err
}

func settingsJSON(m map[string]string) any {
	if len(m) == 0 {
		return nil
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func (d *DB) CreateDomain(ctx context.Context, dom identity.Domain) (identity.Domain, error) {
	name, err := identity.NormalizeDomain(dom.Name)
	if err != nil {
		return identity.Domain{}, err
	}
	dom.Name = name
	if dom.Namespace, err = identity.NormalizeNamespace(dom.Namespace); err != nil {
		return identity.Domain{}, err
	}
	now := d.Now().UTC()
	dom.Created, dom.Updated = now, now
	err = d.tx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.namespaceID(ctx, tx, dom.Namespace)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO domains (name, namespace_id, enabled, settings, created, updated) VALUES (?, ?, ?, ?, ?, ?)",
			dom.Name, nsID, dom.Enabled, settingsJSON(dom.Settings), now, now)
		return err
	})
	if err != nil {
		return identity.Domain{}, err
	}
	return dom, nil
}

func (d *DB) GetDomain(ctx context.Context, name string) (identity.Versioned[identity.Domain], error) {
	name, err := identity.NormalizeDomain(name)
	if err != nil {
		return identity.Versioned[identity.Domain]{}, err
	}
	_, dom, err := scanDomain(d.r(ctx).QueryRowContext(ctx, "SELECT "+domainCols+domainFrom+"WHERE d.name = ?", name))
	return identity.Versioned[identity.Domain]{Value: dom}, err
}

func (d *DB) UpdateDomain(ctx context.Context, name string, fn func(*identity.Domain) error) (identity.Domain, error) {
	name, err := identity.NormalizeDomain(name)
	if err != nil {
		return identity.Domain{}, err
	}
	var out identity.Domain
	err = d.tx(ctx, func(tx *sql.Tx) error {
		id, dom, err := scanDomain(tx.QueryRowContext(ctx, "SELECT "+domainCols+domainFrom+"WHERE d.name = ? FOR UPDATE", name))
		if err != nil {
			return err
		}
		ns := dom.Namespace
		if err := fn(&dom); err != nil {
			return err
		}
		dom.Name, dom.Namespace = name, ns
		dom.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE domains SET enabled = ?, settings = ?, updated = ? WHERE id = ?", dom.Enabled, settingsJSON(dom.Settings), dom.Updated, id); err != nil {
			return err
		}
		out = dom
		return nil
	})
	return out, err
}

func (d *DB) DeleteDomain(ctx context.Context, name string) error {
	name, err := identity.NormalizeDomain(name)
	if err != nil {
		return err
	}
	return d.tx(ctx, func(tx *sql.Tx) error {
		var id int64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM domains WHERE name = ? FOR UPDATE", name).Scan(&id); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM accounts WHERE domain_id = ?) + (SELECT COUNT(*) FROM aliases WHERE domain_id = ?)", id, id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: domain %s has accounts or aliases", identity.ErrInUse, name)
		}
		_, err := tx.ExecContext(ctx, "DELETE FROM domains WHERE id = ?", id)
		return err
	})
}

func (d *DB) ListDomains(ctx context.Context, namespace, after string, max int) (names []string, more bool, err error) {
	if namespace == "" {
		return d.listStrings(ctx, "SELECT name FROM domains WHERE name > ? ORDER BY name LIMIT ?", after, max)
	}
	ns, err := identity.NormalizeNamespace(namespace)
	if err != nil {
		return nil, false, err
	}
	return d.listStrings(ctx, "SELECT d.name"+domainFrom+"WHERE n.name = ? AND d.name > ? ORDER BY d.name LIMIT ?", ns, after, max)
}

// listStrings runs a "WHERE x > after ORDER BY x LIMIT max+1" query and
// reports whether more rows follow. Query must end with "LIMIT ?" and
// take `after` as its last argument before the limit.
func (d *DB) listStrings(ctx context.Context, q string, args ...any) ([]string, bool, error) {
	max, _ := args[len(args)-1].(int)
	if max <= 0 {
		max = 1000
	}
	args[len(args)-1] = max + 1
	rows, err := d.r(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, false, mapErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, false, err
		}
		out = append(out, s)
	}
	if len(out) > max {
		return out[:max], true, rows.Err()
	}
	return out, false, rows.Err()
}

// ---- mailboxes ----

const mailboxCols = "m.id, m.uuid, n.name, m.storage_id, m.state, m.state_reason, m.state_since, m.soft_bytes, m.hard_bytes, m.soft_messages, m.hard_messages, m.hold, m.released, m.backup_interval, m.backup_retention, m.backup_disabled, m.created, m.updated"
const mailboxFrom = " FROM mailboxes m JOIN namespaces n ON n.id = m.namespace_id "

func scanMailbox(row interface{ Scan(...any) error }) (int64, identity.Mailbox, error) {
	var id int64
	var m identity.Mailbox
	var u []byte
	var storage sql.NullInt64
	var hold sql.NullString
	var released sql.NullTime
	var bInterval, bRetention sql.NullInt64
	if err := row.Scan(&id, &u, &m.Namespace, &storage, &m.State, &m.StateReason, &m.StateSince, &m.Quota.SoftBytes, &m.Quota.HardBytes, &m.Quota.SoftMessages, &m.Quota.HardMessages, &hold, &released, &bInterval, &bRetention, &m.Backup.Disabled, &m.Created, &m.Updated); err != nil {
		return 0, identity.Mailbox{}, mapErr(err)
	}
	m.ID = toUUID(u)
	m.StorageID = storage.Int64
	m.Backup.Interval = time.Duration(bInterval.Int64) * time.Second
	m.Backup.Retention = time.Duration(bRetention.Int64) * time.Second
	if hold.Valid && hold.String != "" {
		var h identity.Hold
		if json.Unmarshal([]byte(hold.String), &h) == nil {
			m.Hold = &h
		}
	}
	if released.Valid {
		t := released.Time
		m.Released = &t
	}
	return id, m, nil
}

func holdJSON(h *identity.Hold) any {
	if h == nil {
		return nil
	}
	b, _ := json.Marshal(h)
	return string(b)
}

func nullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

// nullSeconds stores a duration as whole seconds, NULL when zero.
func nullSeconds(d time.Duration) any {
	if d <= 0 {
		return nil
	}
	return int64(d / time.Second)
}

func (d *DB) CreateMailbox(ctx context.Context, m identity.Mailbox) (identity.Mailbox, error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	now := d.Now().UTC()
	m.Created, m.Updated = now, now
	m.Hold = nil
	m.Released = nil
	if m.State == "" {
		m.State = identity.StateActive
	}
	if !identity.ValidStates[m.State] {
		return identity.Mailbox{}, fmt.Errorf("%w: mailbox state %q", identity.ErrInvalid, m.State)
	}
	m.StateSince = now
	ns, err := identity.NormalizeNamespace(m.Namespace)
	if err != nil {
		return identity.Mailbox{}, err
	}
	m.Namespace = ns
	err = d.tx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.namespaceID(ctx, tx, ns)
		if err != nil {
			return err
		}
		if m.StorageID != 0 {
			var sns string
			err := tx.QueryRowContext(ctx, "SELECT n.name FROM storage s JOIN namespaces n ON n.id = s.namespace_id WHERE s.id = ?", m.StorageID).Scan(&sns)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("storage %d: %w", m.StorageID, identity.ErrNotFound)
			}
			if err != nil {
				return err
			}
			if sns != ns {
				return fmt.Errorf("%w: storage %d belongs to namespace %s, mailbox to %s", identity.ErrNamespaceMismatch, m.StorageID, sns, ns)
			}
		}
		var removed []byte
		err = tx.QueryRowContext(ctx, "SELECT uuid FROM mailbox_removals WHERE uuid = ? FOR UPDATE", bin(m.ID)).Scan(&removed)
		if err == nil {
			return identity.ErrInUse
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO mailboxes (uuid, namespace_id, storage_id, state, state_reason, state_since, soft_bytes, hard_bytes, soft_messages, hard_messages, hold, released, backup_interval, backup_retention, backup_disabled, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)",
			bin(m.ID), nsID, nullID(m.StorageID), m.State, m.StateReason, m.StateSince, m.Quota.SoftBytes, m.Quota.HardBytes, m.Quota.SoftMessages, m.Quota.HardMessages, nullSeconds(m.Backup.Interval), nullSeconds(m.Backup.Retention), m.Backup.Disabled, now, now)
		return err
	})
	if err != nil {
		return identity.Mailbox{}, err
	}
	return m, nil
}

func (d *DB) GetMailbox(ctx context.Context, id uuid.UUID) (identity.Versioned[identity.Mailbox], error) {
	_, m, err := scanMailbox(d.r(ctx).QueryRowContext(ctx, "SELECT "+mailboxCols+mailboxFrom+"WHERE m.uuid = ?", bin(id)))
	return identity.Versioned[identity.Mailbox]{Value: m}, err
}

func (d *DB) mailboxRowID(ctx context.Context, q querier, id uuid.UUID) (int64, error) {
	rid, _, err := d.mailboxRow(ctx, q, id)
	return rid, err
}

// mailboxRow resolves a mailbox uuid to its row id and namespace name.
func (d *DB) mailboxRow(ctx context.Context, q querier, id uuid.UUID) (rid int64, namespace string, err error) {
	err = q.QueryRowContext(ctx, "SELECT m.id, n.name"+mailboxFrom+"WHERE m.uuid = ?", bin(id)).Scan(&rid, &namespace)
	return rid, namespace, mapErr(err)
}

func (d *DB) UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*identity.Mailbox) error) (identity.Mailbox, error) {
	var out identity.Mailbox
	err := d.tx(ctx, func(tx *sql.Tx) error {
		rid, m, err := scanMailbox(tx.QueryRowContext(ctx, "SELECT "+mailboxCols+mailboxFrom+"WHERE m.uuid = ? FOR UPDATE", bin(id)))
		if err != nil {
			return err
		}
		prev := m
		if err := fn(&m); err != nil {
			return err
		}
		m.ID, m.Namespace = id, prev.Namespace
		m.Updated = d.Now().UTC()
		if m.State == "" {
			m.State = identity.StateActive
		}
		if !identity.ValidStates[m.State] {
			return fmt.Errorf("%w: mailbox state %q", identity.ErrInvalid, m.State)
		}
		if m.State != prev.State {
			m.StateSince = m.Updated
		}
		_, err = tx.ExecContext(ctx, "UPDATE mailboxes SET storage_id = ?, state = ?, state_reason = ?, state_since = ?, soft_bytes = ?, hard_bytes = ?, soft_messages = ?, hard_messages = ?, hold = ?, released = ?, backup_interval = ?, backup_retention = ?, backup_disabled = ?, updated = ? WHERE id = ?",
			nullID(m.StorageID), m.State, m.StateReason, m.StateSince, m.Quota.SoftBytes, m.Quota.HardBytes, m.Quota.SoftMessages, m.Quota.HardMessages, holdJSON(m.Hold), nullTime(m.Released), nullSeconds(m.Backup.Interval), nullSeconds(m.Backup.Retention), m.Backup.Disabled, m.Updated, rid)
		if err != nil {
			return err
		}
		out = m
		return nil
	})
	return out, err
}

func (d *DB) DeleteMailbox(ctx context.Context, id uuid.UUID) error {
	return d.deleteMailbox(ctx, id, false)
}

func (d *DB) ScheduleMailboxRemoval(ctx context.Context, id uuid.UUID) error {
	return d.deleteMailbox(ctx, id, true)
}

func (d *DB) deleteMailbox(ctx context.Context, id uuid.UUID, schedule bool) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		rid, m, err := scanMailbox(tx.QueryRowContext(ctx, "SELECT "+mailboxCols+mailboxFrom+"WHERE m.uuid = ? FOR UPDATE", bin(id)))
		if err != nil {
			return err
		}
		if m.Hold != nil {
			return fmt.Errorf("%w: mailbox %s is on hold", identity.ErrInUse, id)
		}
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE mailbox_id = ?", rid).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: mailbox %s has accounts", identity.ErrInUse, id)
		}
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM alias_destinations WHERE mailbox_id = ?", rid).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: mailbox %s is an alias destination", identity.ErrInUse, id)
		}
		if schedule {
			payload, err := json.Marshal(map[string]any{"mailbox": id, "storage_id": m.StorageID})
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO mailbox_removals (uuid, storage_id) VALUES (?, ?)", bin(id), nullID(m.StorageID)); err != nil {
				return err
			}
			now := d.Now().UTC()
			nsID, err := d.namespaceID(ctx, tx, m.Namespace)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO tasks (id, type, namespace_id, state, payload, run_after, attempts, created) VALUES (?, 'remove-mailbox', ?, 'pending', ?, ?, 0, ?)", uuid.NewString(), nsID, string(payload), now, now); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM mailboxes WHERE id = ?", rid)
		return err
	})
}

// ListMailboxes releases each SQL result before invoking callbacks. The high
// water mark prevents concurrent provisioning from extending a sweep forever.
func (d *DB) ListMailboxes(ctx context.Context, namespace string, fn func(uuid.UUID) error) error {
	var ceiling int64
	if err := d.r(ctx).QueryRowContext(ctx, "SELECT COALESCE(MAX(id),0) FROM mailboxes").Scan(&ceiling); err != nil {
		return err
	}
	q, filter := "SELECT id, uuid FROM mailboxes WHERE id > ? AND id <= ? ORDER BY id LIMIT 512", []any(nil)
	if namespace != "" {
		nsID, err := d.namespaceID(ctx, d.r(ctx), namespace)
		if err != nil {
			if errors.Is(err, identity.ErrNamespaceUnknown) {
				return nil
			}
			return err
		}
		q, filter = "SELECT id, uuid FROM mailboxes WHERE namespace_id = ? AND id > ? AND id <= ? ORDER BY id LIMIT 512", []any{nsID}
	}
	var after int64
	for {
		rows, err := d.r(ctx).QueryContext(ctx, q, append(append([]any{}, filter...), after, ceiling)...)
		if err != nil {
			return mapErr(err)
		}
		var ids []uuid.UUID
		for rows.Next() {
			var id int64
			var raw []byte
			if err := rows.Scan(&id, &raw); err != nil {
				rows.Close()
				return err
			}
			after = id
			ids = append(ids, toUUID(raw))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := fn(id); err != nil {
				return err
			}
		}
		if len(ids) < 512 {
			return nil
		}
	}
}

func (d *DB) ListMailboxAccounts(ctx context.Context, id uuid.UUID, after string, max int) (usernames []string, more bool, err error) {
	rid, err := d.mailboxRowID(ctx, d.r(ctx), id)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return d.listStrings(ctx, "SELECT username FROM accounts WHERE mailbox_id = ? AND username > ? ORDER BY username LIMIT ?", rid, after, max)
}

// ---- accounts ----

const accountCols = "a.id, a.uuid, a.username, d.name, n.name, m.uuid, a.name, a.roles, a.services, a.enabled, a.totp, a.created, a.updated"
const accountFrom = " FROM accounts a JOIN domains d ON d.id = a.domain_id JOIN namespaces n ON n.id = d.namespace_id JOIN mailboxes m ON m.id = a.mailbox_id "

func (d *DB) scanAccount(ctx context.Context, q querier, row interface{ Scan(...any) error }) (int64, identity.Account, error) {
	var id int64
	var a identity.Account
	var u, mb []byte
	var roles, services string
	var totp sql.NullString
	if err := row.Scan(&id, &u, &a.Username, &a.Domain, &a.Namespace, &mb, &a.Name, &roles, &services, &a.Enabled, &totp, &a.Created, &a.Updated); err != nil {
		return 0, identity.Account{}, mapErr(err)
	}
	a.ID = toUUID(u)
	a.MailboxID = toUUID(mb)
	a.Roles = splitList(roles)
	a.Services = splitList(services)
	if totp.Valid && totp.String != "" {
		var t identity.TOTP
		if json.Unmarshal([]byte(totp.String), &t) == nil {
			a.TOTP = &t
		}
	}
	rows, err := q.QueryContext(ctx, "SELECT id, hash, kind, comment, expires_at, created FROM account_passwords WHERE account_id = ? ORDER BY created, id", id)
	if err != nil {
		return 0, identity.Account{}, mapErr(err)
	}
	defer rows.Close()
	a.Passwords = []identity.Password{}
	for rows.Next() {
		var p identity.Password
		var exp sql.NullTime
		if err := rows.Scan(&p.ID, &p.Hash, &p.Kind, &p.Comment, &exp, &p.Created); err != nil {
			return 0, identity.Account{}, err
		}
		if exp.Valid {
			t := exp.Time
			p.ExpiresAt = &t
		}
		a.Passwords = append(a.Passwords, p)
	}
	return id, a, rows.Err()
}

func totpJSON(t *identity.TOTP) any {
	if t == nil {
		return nil
	}
	b, _ := json.Marshal(t)
	return string(b)
}

func (d *DB) writePasswords(ctx context.Context, tx *sql.Tx, accountID int64, pws []identity.Password, now time.Time) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM account_passwords WHERE account_id = ?", accountID); err != nil {
		return err
	}
	for i, p := range pws {
		if p.ID == "" {
			p.ID = uuid.NewString()
		}
		if p.Kind == "" {
			p.Kind = identity.PasswordPrimary
		}
		if p.Created.IsZero() {
			p.Created = now
		}
		pws[i] = p
		if _, err := tx.ExecContext(ctx, "INSERT INTO account_passwords (id, account_id, hash, kind, comment, expires_at, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
			p.ID, accountID, p.Hash, p.Kind, p.Comment, nullTime(p.ExpiresAt), p.Created.UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) CreateAccount(ctx context.Context, a identity.Account) (identity.Account, error) {
	username, domain, err := identity.NormalizeAddress(a.Username)
	if err != nil {
		return identity.Account{}, err
	}
	if err := identity.ValidateRoles(a.Roles); err != nil {
		return identity.Account{}, err
	}
	if a.Services == nil {
		a.Services = identity.AllServices()
	}
	if err := identity.ValidateServices(a.Services); err != nil {
		return identity.Account{}, err
	}
	a.Username, a.Domain = username, domain
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if a.MailboxID == uuid.Nil {
		return identity.Account{}, fmt.Errorf("%w: account needs a mailbox", identity.ErrInvalid)
	}
	a.Name = strings.TrimSpace(a.Name)
	now := d.Now().UTC()
	a.Created, a.Updated = now, now
	err = d.tx(ctx, func(tx *sql.Tx) error {
		mbID, mbNS, err := d.mailboxRow(ctx, tx, a.MailboxID)
		if err != nil {
			return fmt.Errorf("mailbox %s: %w", a.MailboxID, err)
		}
		domID, domNS, err := d.domainRow(ctx, tx, domain)
		if err != nil {
			return err
		}
		if mbNS != domNS {
			return fmt.Errorf("%w: mailbox %s is in namespace %s, domain %s in %s", identity.ErrNamespaceMismatch, a.MailboxID, mbNS, domain, domNS)
		}
		a.Namespace = domNS
		res, err := tx.ExecContext(ctx, "INSERT INTO accounts (uuid, username, domain_id, mailbox_id, name, roles, services, enabled, totp, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			bin(a.ID), username, domID, mbID, a.Name, joinList(a.Roles), joinList(a.Services), a.Enabled, totpJSON(a.TOTP), now, now)
		if err != nil {
			return err
		}
		rid, _ := res.LastInsertId()
		return d.writePasswords(ctx, tx, rid, a.Passwords, now)
	})
	if err != nil {
		return identity.Account{}, err
	}
	return a, nil
}

func (d *DB) GetAccount(ctx context.Context, id uuid.UUID) (identity.Versioned[identity.Account], error) {
	_, a, err := d.scanAccount(ctx, d.r(ctx), d.r(ctx).QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.uuid = ?", bin(id)))
	return identity.Versioned[identity.Account]{Value: a}, err
}

func (d *DB) GetAccountByUsername(ctx context.Context, username string) (identity.Versioned[identity.Account], error) {
	username, _, err := identity.NormalizeAddress(username)
	if err != nil {
		return identity.Versioned[identity.Account]{}, err
	}
	_, a, err := d.scanAccount(ctx, d.r(ctx), d.r(ctx).QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.username = ?", username))
	return identity.Versioned[identity.Account]{Value: a}, err
}

func (d *DB) UpdateAccount(ctx context.Context, id uuid.UUID, fn func(*identity.Account) error) (identity.Account, error) {
	var out identity.Account
	err := d.tx(ctx, func(tx *sql.Tx) error {
		rid, a, err := d.scanAccount(ctx, tx, tx.QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.uuid = ? FOR UPDATE", bin(id)))
		if err != nil {
			return err
		}
		cur := a
		if err := fn(&a); err != nil {
			return err
		}
		if a.Username != cur.Username || a.Domain != cur.Domain || a.MailboxID != cur.MailboxID {
			return fmt.Errorf("%w: username, domain and mailbox cannot be changed via UpdateAccount", identity.ErrInvalid)
		}
		if err := identity.ValidateRoles(a.Roles); err != nil {
			return err
		}
		if a.Services == nil {
			a.Services = []string{}
		}
		if err := identity.ValidateServices(a.Services); err != nil {
			return err
		}
		a.ID, a.Namespace = id, cur.Namespace
		a.Name = strings.TrimSpace(a.Name)
		a.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE accounts SET name = ?, roles = ?, services = ?, enabled = ?, totp = ?, updated = ? WHERE id = ?",
			a.Name, joinList(a.Roles), joinList(a.Services), a.Enabled, totpJSON(a.TOTP), a.Updated, rid); err != nil {
			return err
		}
		if err := d.writePasswords(ctx, tx, rid, a.Passwords, a.Updated); err != nil {
			return err
		}
		out = a
		return nil
	})
	return out, err
}

func (d *DB) RelinkAccount(ctx context.Context, id, mailboxID uuid.UUID) (identity.Account, error) {
	var out identity.Account
	err := d.tx(ctx, func(tx *sql.Tx) error {
		mbID, mbNS, err := d.mailboxRow(ctx, tx, mailboxID)
		if err != nil {
			return fmt.Errorf("mailbox %s: %w", mailboxID, err)
		}
		rid, a, err := d.scanAccount(ctx, tx, tx.QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.uuid = ? FOR UPDATE", bin(id)))
		if err != nil {
			return err
		}
		if mbNS != a.Namespace {
			return fmt.Errorf("%w: mailbox %s is in namespace %s, account in %s", identity.ErrNamespaceMismatch, mailboxID, mbNS, a.Namespace)
		}
		a.MailboxID = mailboxID
		a.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE accounts SET mailbox_id = ?, updated = ? WHERE id = ?", mbID, a.Updated, rid); err != nil {
			return err
		}
		out = a
		return nil
	})
	return out, err
}

func (d *DB) DeleteAccount(ctx context.Context, id uuid.UUID) error {
	res, err := d.exec(ctx, "DELETE FROM accounts WHERE uuid = ?", bin(id))
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return identity.ErrNotFound
	}
	return nil
}

func (d *DB) ListDomainAccounts(ctx context.Context, domain, prefix, after string, max int) (usernames []string, more bool, err error) {
	domain, err = identity.NormalizeDomain(domain)
	if err != nil {
		return nil, false, err
	}
	domID, err := d.domainID(ctx, d.r(ctx), domain)
	if err != nil {
		if errors.Is(err, identity.ErrDomainUnknown) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return d.listStrings(ctx, "SELECT username FROM accounts WHERE domain_id = ? AND username LIKE ? AND username > ? ORDER BY username LIMIT ?", domID, likePrefix(prefix), prefix+after, max)
}

// ---- aliases ----

func (d *DB) loadAlias(ctx context.Context, q querier, address string, lock bool) (int64, identity.Alias, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	var id int64
	var al identity.Alias
	err := q.QueryRowContext(ctx, "SELECT a.id, a.address, d.name, n.name, a.enabled, a.created, a.updated FROM aliases a JOIN domains d ON d.id = a.domain_id JOIN namespaces n ON n.id = d.namespace_id WHERE a.address = ?"+suffix, address).
		Scan(&id, &al.Address, &al.Domain, &al.Namespace, &al.Enabled, &al.Created, &al.Updated)
	if err != nil {
		return 0, identity.Alias{}, mapErr(err)
	}
	rows, err := q.QueryContext(ctx, "SELECT m.uuid, ad.address FROM alias_destinations ad LEFT JOIN mailboxes m ON m.id = ad.mailbox_id WHERE ad.alias_id = ? ORDER BY ad.position", id)
	if err != nil {
		return 0, identity.Alias{}, mapErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var mb []byte
		var addr sql.NullString
		if err := rows.Scan(&mb, &addr); err != nil {
			return 0, identity.Alias{}, err
		}
		dst := identity.Destination{Address: addr.String}
		if len(mb) == 16 {
			dst.MailboxID = toUUID(mb)
		}
		al.Destinations = append(al.Destinations, dst)
	}
	return id, al, rows.Err()
}

// validateDestinations checks and normalizes destinations; mailbox
// destinations must exist and be in the alias domain's namespace.
func (d *DB) validateDestinations(ctx context.Context, tx *sql.Tx, address, namespace string, dsts []identity.Destination) ([]int64, error) {
	if len(dsts) == 0 {
		return nil, fmt.Errorf("%w: alias needs at least one destination", identity.ErrInvalid)
	}
	ids := make([]int64, len(dsts))
	for i, dst := range dsts {
		if (dst.MailboxID == uuid.Nil) == (dst.Address == "") {
			return nil, fmt.Errorf("%w: destination must be exactly one of mailbox or address", identity.ErrInvalid)
		}
		if dst.Address != "" {
			na, _, err := identity.NormalizeAddress(dst.Address)
			if err != nil {
				return nil, err
			}
			if na == address {
				return nil, fmt.Errorf("%w: alias points at itself", identity.ErrInvalid)
			}
			dsts[i].Address = na
			continue
		}
		rid, ns, err := d.mailboxRow(ctx, tx, dst.MailboxID)
		if err != nil {
			return nil, fmt.Errorf("mailbox %s: %w", dst.MailboxID, err)
		}
		if ns != namespace {
			return nil, fmt.Errorf("%w: mailbox %s is in namespace %s, alias domain in %s", identity.ErrNamespaceMismatch, dst.MailboxID, ns, namespace)
		}
		ids[i] = rid
	}
	return ids, nil
}

func (d *DB) writeDestinations(ctx context.Context, tx *sql.Tx, aliasID int64, dsts []identity.Destination, mbIDs []int64) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM alias_destinations WHERE alias_id = ?", aliasID); err != nil {
		return err
	}
	for i, dst := range dsts {
		var addr any
		if dst.Address != "" {
			addr = dst.Address
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO alias_destinations (alias_id, position, mailbox_id, address) VALUES (?, ?, ?, ?)", aliasID, i, nullID(mbIDs[i]), addr); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) CreateAlias(ctx context.Context, al identity.Alias) (identity.Alias, error) {
	address, domain, err := identity.NormalizeAliasAddress(al.Address)
	if err != nil {
		return identity.Alias{}, err
	}
	al.Address, al.Domain = address, domain
	now := d.Now().UTC()
	al.Created, al.Updated = now, now
	err = d.tx(ctx, func(tx *sql.Tx) error {
		domID, domNS, err := d.domainRow(ctx, tx, domain)
		if err != nil {
			return err
		}
		mbIDs, err := d.validateDestinations(ctx, tx, address, domNS, al.Destinations)
		if err != nil {
			return err
		}
		al.Namespace = domNS
		if !strings.HasPrefix(address, "@") {
			var n int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE username = ?", address).Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				return fmt.Errorf("%w: %s is an account", identity.ErrExists, address)
			}
		}
		res, err := tx.ExecContext(ctx, "INSERT INTO aliases (address, domain_id, enabled, created, updated) VALUES (?, ?, ?, ?, ?)", address, domID, al.Enabled, now, now)
		if err != nil {
			return err
		}
		id, _ := res.LastInsertId()
		return d.writeDestinations(ctx, tx, id, al.Destinations, mbIDs)
	})
	if err != nil {
		return identity.Alias{}, err
	}
	return al, nil
}

func (d *DB) ListMailboxAliases(ctx context.Context, id uuid.UUID) ([]string, error) {
	rid, err := d.mailboxRowID(ctx, d.r(ctx), id)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	rows, err := d.r(ctx).QueryContext(ctx, "SELECT DISTINCT a.address FROM aliases a JOIN alias_destinations ad ON ad.alias_id = a.id WHERE ad.mailbox_id = ? ORDER BY a.address", rid)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ListAliasesTo lists the aliases with address as a destination address
// (index ix_alias_destinations_address).
func (d *DB) ListAliasesTo(ctx context.Context, address string) ([]string, error) {
	na, _, err := identity.NormalizeAddress(address)
	if err != nil {
		return nil, err
	}
	rows, err := d.r(ctx).QueryContext(ctx, "SELECT DISTINCT a.address FROM aliases a JOIN alias_destinations ad ON ad.alias_id = a.id WHERE ad.address = ? ORDER BY a.address", na)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (d *DB) GetAlias(ctx context.Context, address string) (identity.Versioned[identity.Alias], error) {
	address, _, err := identity.NormalizeAliasAddress(address)
	if err != nil {
		return identity.Versioned[identity.Alias]{}, err
	}
	_, al, err := d.loadAlias(ctx, d.r(ctx), address, false)
	return identity.Versioned[identity.Alias]{Value: al}, err
}

func (d *DB) UpdateAlias(ctx context.Context, address string, fn func(*identity.Alias) error) (identity.Alias, error) {
	address, domain, err := identity.NormalizeAliasAddress(address)
	if err != nil {
		return identity.Alias{}, err
	}
	var out identity.Alias
	err = d.tx(ctx, func(tx *sql.Tx) error {
		id, al, err := d.loadAlias(ctx, tx, address, true)
		if err != nil {
			return err
		}
		ns := al.Namespace
		if err := fn(&al); err != nil {
			return err
		}
		mbIDs, err := d.validateDestinations(ctx, tx, address, ns, al.Destinations)
		if err != nil {
			return err
		}
		al.Address, al.Domain, al.Namespace = address, domain, ns
		al.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE aliases SET enabled = ?, updated = ? WHERE id = ?", al.Enabled, al.Updated, id); err != nil {
			return err
		}
		if err := d.writeDestinations(ctx, tx, id, al.Destinations, mbIDs); err != nil {
			return err
		}
		out = al
		return nil
	})
	return out, err
}

func (d *DB) DeleteAlias(ctx context.Context, address string) error {
	address, _, err := identity.NormalizeAliasAddress(address)
	if err != nil {
		return err
	}
	res, err := d.exec(ctx, "DELETE FROM aliases WHERE address = ?", address)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return identity.ErrNotFound
	}
	return nil
}

func (d *DB) ListDomainAliases(ctx context.Context, domain, after string, max int) (addresses []string, more bool, err error) {
	domain, err = identity.NormalizeDomain(domain)
	if err != nil {
		return nil, false, err
	}
	domID, err := d.domainID(ctx, d.r(ctx), domain)
	if err != nil {
		if errors.Is(err, identity.ErrDomainUnknown) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return d.listStrings(ctx, "SELECT address FROM aliases WHERE domain_id = ? AND address > ? ORDER BY address LIMIT ?", domID, after, max)
}

// ---- tokens ----

const tokenCols = "t.id, t.hash, a.uuid, ns.name, t.roles, t.comment, t.expires_at, t.created, t.last_used"
const tokenFrom = " FROM tokens t LEFT JOIN accounts a ON a.id = t.account_id LEFT JOIN namespaces ns ON ns.id = t.namespace_id "

func scanToken(row interface{ Scan(...any) error }) (identity.Token, error) {
	var t identity.Token
	var acct []byte
	var roles string
	var namespace sql.NullString
	var last sql.NullTime
	if err := row.Scan(&t.ID, &t.Hash, &acct, &namespace, &roles, &t.Comment, &t.ExpiresAt, &t.Created, &last); err != nil {
		return identity.Token{}, mapErr(err)
	}
	if len(acct) == 16 {
		t.AccountID = toUUID(acct)
	}
	t.Namespace = namespace.String
	t.Roles = splitList(roles)
	if last.Valid {
		t.LastUsed = last.Time
	}
	return t, nil
}

func (d *DB) CreateToken(ctx context.Context, accountID uuid.UUID, roles []string, expiresAt time.Time, comment string) (identity.Token, string, error) {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return identity.Token{}, "", fmt.Errorf("%w: token comment is required", identity.ErrInvalid)
	}
	now := d.Now().UTC()
	if !expiresAt.After(now) {
		return identity.Token{}, "", fmt.Errorf("%w: token expiration must be in the future", identity.ErrInvalid)
	}
	if err := identity.ValidateRoles(roles); err != nil {
		return identity.Token{}, "", err
	}
	var t identity.Token
	var secret string
	err := d.tx(ctx, func(tx *sql.Tx) error {
		rid, acct, err := d.scanAccount(ctx, tx, tx.QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.uuid = ?", bin(accountID)))
		if err != nil {
			return err
		}
		if len(roles) == 0 {
			roles = append([]string(nil), acct.Roles...)
		}
		for _, r := range roles {
			if !acct.HasRole(r) {
				return fmt.Errorf("%w: account lacks role %q", identity.ErrInvalid, r)
			}
		}
		secret, err = identity.NewTokenSecret()
		if err != nil {
			return err
		}
		t = identity.Token{ID: uuid.NewString(), Hash: identity.HashToken(secret), AccountID: accountID, Roles: roles, ExpiresAt: expiresAt.UTC(), Comment: comment, Created: now}
		_, err = tx.ExecContext(ctx, "INSERT INTO tokens (id, hash, account_id, roles, comment, expires_at, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
			t.ID, t.Hash, rid, joinList(t.Roles), t.Comment, t.ExpiresAt, t.Created)
		return err
	})
	if err != nil {
		return identity.Token{}, "", err
	}
	return t, secret, nil
}

func (d *DB) CreateAdminToken(ctx context.Context, role, namespace string, expiresAt time.Time, comment string) (identity.Token, string, error) {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return identity.Token{}, "", fmt.Errorf("%w: token comment is required", identity.ErrInvalid)
	}
	if !identity.AdminRoles[role] {
		return identity.Token{}, "", fmt.Errorf("%w: admin role %q", identity.ErrInvalid, role)
	}
	now := d.Now().UTC()
	if !expiresAt.After(now) {
		return identity.Token{}, "", fmt.Errorf("%w: token expiration must be in the future", identity.ErrInvalid)
	}
	var nsID any
	namespace = strings.TrimSpace(namespace)
	if namespace != "" {
		var err error
		if namespace, err = identity.NormalizeNamespace(namespace); err != nil {
			return identity.Token{}, "", err
		}
		id, err := d.namespaceID(ctx, d.sql, namespace)
		if err != nil {
			return identity.Token{}, "", err
		}
		nsID = id
	}
	secret, err := identity.NewTokenSecret()
	if err != nil {
		return identity.Token{}, "", err
	}
	t := identity.Token{ID: uuid.NewString(), Hash: identity.HashToken(secret), Namespace: namespace, Roles: []string{role}, ExpiresAt: expiresAt.UTC(), Comment: comment, Created: now}
	if _, err := d.exec(ctx, "INSERT INTO tokens (id, hash, account_id, namespace_id, roles, comment, expires_at, created) VALUES (?, ?, NULL, ?, ?, ?, ?, ?)",
		t.ID, t.Hash, nsID, joinList(t.Roles), t.Comment, t.ExpiresAt, t.Created); err != nil {
		return identity.Token{}, "", mapErr(err)
	}
	return t, secret, nil
}

func (d *DB) listTokens(ctx context.Context, q string, args ...any) ([]identity.Token, error) {
	rows, err := d.r(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []identity.Token
	for rows.Next() {
		t, err := scanToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (d *DB) ListAdminTokens(ctx context.Context) ([]identity.Token, error) {
	return d.listTokens(ctx, "SELECT "+tokenCols+tokenFrom+"WHERE t.account_id IS NULL ORDER BY t.created, t.id")
}

func (d *DB) DeleteAdminToken(ctx context.Context, tokenID string) error {
	res, err := d.exec(ctx, "DELETE FROM tokens WHERE id = ? AND account_id IS NULL", tokenID)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return identity.ErrNotFound
	}
	return nil
}

func (d *DB) LookupToken(ctx context.Context, secret string) (identity.Versioned[identity.Token], error) {
	if !strings.HasPrefix(secret, identity.TokenPrefix) {
		return identity.Versioned[identity.Token]{}, identity.ErrNotFound
	}
	t, err := scanToken(d.r(ctx).QueryRowContext(ctx, "SELECT "+tokenCols+tokenFrom+"WHERE t.hash = ?", identity.HashToken(secret)))
	return identity.Versioned[identity.Token]{Value: t}, err
}

func (d *DB) TouchToken(ctx context.Context, t identity.Versioned[identity.Token]) {
	_, _ = d.sql.ExecContext(ctx, "UPDATE tokens SET last_used = ? WHERE hash = ? AND (last_used IS NULL OR last_used < ?)", d.Now().UTC(), t.Value.Hash, d.Now().UTC().Add(-time.Minute))
}

func (d *DB) ListTokens(ctx context.Context, accountID uuid.UUID) ([]identity.Token, error) {
	return d.listTokens(ctx, "SELECT "+tokenCols+tokenFrom+"WHERE a.uuid = ? ORDER BY t.created, t.id", bin(accountID))
}

func (d *DB) DeleteToken(ctx context.Context, accountID uuid.UUID, tokenID string) error {
	res, err := d.exec(ctx, "DELETE t FROM tokens t JOIN accounts a ON a.id = t.account_id WHERE t.id = ? AND a.uuid = ?", tokenID, bin(accountID))
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return identity.ErrNotFound
	}
	return nil
}

// ---- storage ----

const storageCols = "s.id, s.fsid, s.name, n.name, s.mon_host, s.ceph_user, s.ceph_key, s.pool_messages, s.pool_attachments, s.pool_index, s.placement, s.created, s.updated"
const storageFrom = " FROM storage s JOIN namespaces n ON n.id = s.namespace_id "

func scanStorage(row interface{ Scan(...any) error }) (identity.Storage, error) {
	var c identity.Storage
	var fsid []byte
	if err := row.Scan(&c.ID, &fsid, &c.Name, &c.Namespace, &c.MonHost, &c.User, &c.Key, &c.Pools.Messages, &c.Pools.Attachments, &c.Pools.Index, &c.Placement, &c.Created, &c.Updated); err != nil {
		return identity.Storage{}, mapErr(err)
	}
	if len(fsid) == 16 {
		c.FSID = toUUID(fsid)
	}
	return c, nil
}

func (d *DB) CreateStorage(ctx context.Context, c identity.Storage) (identity.Storage, error) {
	if err := identity.ValidateStorage(&c); err != nil {
		return identity.Storage{}, err
	}
	now := d.Now().UTC()
	c.Created, c.Updated = now, now
	err := d.tx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.namespaceID(ctx, tx, c.Namespace)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, "INSERT INTO storage (fsid, namespace_id, name, mon_host, ceph_user, ceph_key, pool_messages, pool_attachments, pool_index, placement, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			nullBin(c.FSID), nsID, c.Name, c.MonHost, c.User, c.Key, c.Pools.Messages, c.Pools.Attachments, c.Pools.Index, c.Placement, now, now)
		if err != nil {
			return err
		}
		c.ID, _ = res.LastInsertId()
		return nil
	})
	if err != nil {
		return identity.Storage{}, err
	}
	return c, nil
}

func (d *DB) GetStorage(ctx context.Context, id int64) (identity.Storage, error) {
	return scanStorage(d.r(ctx).QueryRowContext(ctx, "SELECT "+storageCols+storageFrom+"WHERE s.id = ?", id))
}

func (d *DB) ListStorage(ctx context.Context) ([]identity.Storage, error) {
	rows, err := d.r(ctx).QueryContext(ctx, "SELECT "+storageCols+storageFrom+"ORDER BY s.id")
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []identity.Storage
	for rows.Next() {
		c, err := scanStorage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (d *DB) UpdateStorage(ctx context.Context, id int64, fn func(*identity.Storage) error) (identity.Storage, error) {
	var out identity.Storage
	err := d.tx(ctx, func(tx *sql.Tx) error {
		c, err := scanStorage(tx.QueryRowContext(ctx, "SELECT "+storageCols+storageFrom+"WHERE s.id = ? FOR UPDATE", id))
		if err != nil {
			return err
		}
		ns := c.Namespace
		if err := fn(&c); err != nil {
			return err
		}
		if err := identity.ValidateStorage(&c); err != nil {
			return err
		}
		if c.Namespace != ns {
			return fmt.Errorf("%w: a storage cannot move to another namespace", identity.ErrInvalid)
		}
		c.ID = id
		c.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE storage SET fsid = ?, name = ?, mon_host = ?, ceph_user = ?, ceph_key = ?, pool_messages = ?, pool_attachments = ?, pool_index = ?, placement = ?, updated = ? WHERE id = ?",
			nullBin(c.FSID), c.Name, c.MonHost, c.User, c.Key, c.Pools.Messages, c.Pools.Attachments, c.Pools.Index, c.Placement, c.Updated, id); err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

func (d *DB) DeleteStorage(ctx context.Context, id int64) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		if _, err := scanStorage(tx.QueryRowContext(ctx, "SELECT "+storageCols+storageFrom+"WHERE s.id = ? FOR UPDATE", id)); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM mailboxes WHERE storage_id = ?", id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: storage %d has %d mailboxes", identity.ErrInUse, id, n)
		}
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM attachment_gc WHERE storage_id=?", id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: storage %d has pending attachment collection", identity.ErrInUse, id)
		}
		_, err := tx.ExecContext(ctx, "DELETE FROM storage WHERE id = ?", id)
		return err
	})
}

var _ identity.Backend = (*DB)(nil)

// FinishMailboxRemoval releases the storage pin only after data cleanup succeeded.
func (d *DB) FinishMailboxRemoval(ctx context.Context, id uuid.UUID) error {
	_, err := d.exec(ctx, "DELETE FROM mailbox_removals WHERE uuid = ?", bin(id))
	return err
}
