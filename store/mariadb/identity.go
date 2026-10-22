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

// ---- domains ----

const domainCols = "id, name, enabled, settings, created, updated"

func scanDomain(row interface{ Scan(...any) error }) (int64, identity.Domain, error) {
	var id int64
	var d identity.Domain
	var settings sql.NullString
	if err := row.Scan(&id, &d.Name, &d.Enabled, &settings, &d.Created, &d.Updated); err != nil {
		return 0, identity.Domain{}, mapErr(err)
	}
	if settings.Valid && settings.String != "" {
		_ = json.Unmarshal([]byte(settings.String), &d.Settings)
	}
	return id, d, nil
}

func (d *DB) domainID(ctx context.Context, q querier, name string) (int64, error) {
	var id int64
	err := q.QueryRowContext(ctx, "SELECT id FROM domains WHERE name = ?", name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: %s", identity.ErrDomainUnknown, name)
	}
	return id, mapErr(err)
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
	now := d.Now().UTC()
	dom.Created, dom.Updated = now, now
	_, err = d.exec(ctx, "INSERT INTO domains (name, enabled, settings, created, updated) VALUES (?, ?, ?, ?, ?)",
		dom.Name, dom.Enabled, settingsJSON(dom.Settings), now, now)
	if err != nil {
		return identity.Domain{}, mapErr(err)
	}
	return dom, nil
}

func (d *DB) GetDomain(ctx context.Context, name string) (identity.Versioned[identity.Domain], error) {
	name, err := identity.NormalizeDomain(name)
	if err != nil {
		return identity.Versioned[identity.Domain]{}, err
	}
	_, dom, err := scanDomain(d.r().QueryRowContext(ctx, "SELECT "+domainCols+" FROM domains WHERE name = ?", name))
	return identity.Versioned[identity.Domain]{Value: dom}, err
}

func (d *DB) UpdateDomain(ctx context.Context, name string, fn func(*identity.Domain) error) (identity.Domain, error) {
	name, err := identity.NormalizeDomain(name)
	if err != nil {
		return identity.Domain{}, err
	}
	var out identity.Domain
	err = d.tx(ctx, func(tx *sql.Tx) error {
		id, dom, err := scanDomain(tx.QueryRowContext(ctx, "SELECT "+domainCols+" FROM domains WHERE name = ? FOR UPDATE", name))
		if err != nil {
			return err
		}
		if err := fn(&dom); err != nil {
			return err
		}
		dom.Name = name
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

func (d *DB) ListDomains(ctx context.Context, after string, max int) (names []string, more bool, err error) {
	return d.listStrings(ctx, "SELECT name FROM domains WHERE name > ? ORDER BY name LIMIT ?", after, max)
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
	rows, err := d.r().QueryContext(ctx, q, args...)
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

const mailboxCols = "id, uuid, cluster_id, state, state_reason, state_since, soft_bytes, hard_bytes, soft_messages, hard_messages, hold, released, created, updated"

func scanMailbox(row interface{ Scan(...any) error }) (int64, identity.Mailbox, error) {
	var id int64
	var m identity.Mailbox
	var u []byte
	var cluster sql.NullInt64
	var hold sql.NullString
	var released sql.NullTime
	if err := row.Scan(&id, &u, &cluster, &m.State, &m.StateReason, &m.StateSince, &m.Quota.SoftBytes, &m.Quota.HardBytes, &m.Quota.SoftMessages, &m.Quota.HardMessages, &hold, &released, &m.Created, &m.Updated); err != nil {
		return 0, identity.Mailbox{}, mapErr(err)
	}
	m.ID = toUUID(u)
	m.ClusterID = cluster.Int64
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
	_, err := d.exec(ctx, "INSERT INTO mailboxes (uuid, cluster_id, state, state_reason, state_since, soft_bytes, hard_bytes, soft_messages, hard_messages, hold, released, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
		bin(m.ID), nullID(m.ClusterID), m.State, m.StateReason, m.StateSince, m.Quota.SoftBytes, m.Quota.HardBytes, m.Quota.SoftMessages, m.Quota.HardMessages, now, now)
	if err != nil {
		err = mapErr(err)
		if errors.Is(err, identity.ErrNotFound) {
			return identity.Mailbox{}, fmt.Errorf("cluster %d: %w", m.ClusterID, identity.ErrNotFound)
		}
		return identity.Mailbox{}, err
	}
	return m, nil
}

func (d *DB) GetMailbox(ctx context.Context, id uuid.UUID) (identity.Versioned[identity.Mailbox], error) {
	_, m, err := scanMailbox(d.r().QueryRowContext(ctx, "SELECT "+mailboxCols+" FROM mailboxes WHERE uuid = ?", bin(id)))
	return identity.Versioned[identity.Mailbox]{Value: m}, err
}

func (d *DB) mailboxRowID(ctx context.Context, q querier, id uuid.UUID) (int64, error) {
	var rid int64
	err := q.QueryRowContext(ctx, "SELECT id FROM mailboxes WHERE uuid = ?", bin(id)).Scan(&rid)
	return rid, mapErr(err)
}

func (d *DB) UpdateMailbox(ctx context.Context, id uuid.UUID, fn func(*identity.Mailbox) error) (identity.Mailbox, error) {
	var out identity.Mailbox
	err := d.tx(ctx, func(tx *sql.Tx) error {
		rid, m, err := scanMailbox(tx.QueryRowContext(ctx, "SELECT "+mailboxCols+" FROM mailboxes WHERE uuid = ? FOR UPDATE", bin(id)))
		if err != nil {
			return err
		}
		prev := m
		if err := fn(&m); err != nil {
			return err
		}
		m.ID = id
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
		_, err = tx.ExecContext(ctx, "UPDATE mailboxes SET cluster_id = ?, state = ?, state_reason = ?, state_since = ?, soft_bytes = ?, hard_bytes = ?, soft_messages = ?, hard_messages = ?, hold = ?, released = ?, updated = ? WHERE id = ?",
			nullID(m.ClusterID), m.State, m.StateReason, m.StateSince, m.Quota.SoftBytes, m.Quota.HardBytes, m.Quota.SoftMessages, m.Quota.HardMessages, holdJSON(m.Hold), nullTime(m.Released), m.Updated, rid)
		if err != nil {
			return err
		}
		out = m
		return nil
	})
	return out, err
}

func (d *DB) DeleteMailbox(ctx context.Context, id uuid.UUID) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		rid, m, err := scanMailbox(tx.QueryRowContext(ctx, "SELECT "+mailboxCols+" FROM mailboxes WHERE uuid = ? FOR UPDATE", bin(id)))
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
		_, err = tx.ExecContext(ctx, "DELETE FROM mailboxes WHERE id = ?", rid)
		return err
	})
}

func (d *DB) ListMailboxes(ctx context.Context, fn func(id uuid.UUID) error) error {
	rows, err := d.r().QueryContext(ctx, "SELECT uuid FROM mailboxes ORDER BY id")
	if err != nil {
		return mapErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var u []byte
		if err := rows.Scan(&u); err != nil {
			return err
		}
		if err := fn(toUUID(u)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (d *DB) ListMailboxAccounts(ctx context.Context, id uuid.UUID, after string, max int) (usernames []string, more bool, err error) {
	rid, err := d.mailboxRowID(ctx, d.r(), id)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return d.listStrings(ctx, "SELECT username FROM accounts WHERE mailbox_id = ? AND username > ? ORDER BY username LIMIT ?", rid, after, max)
}

// ---- accounts ----

const accountCols = "a.id, a.uuid, a.username, d.name, m.uuid, a.name, a.roles, a.services, a.enabled, a.totp, a.created, a.updated"
const accountFrom = " FROM accounts a JOIN domains d ON d.id = a.domain_id JOIN mailboxes m ON m.id = a.mailbox_id "

func (d *DB) scanAccount(ctx context.Context, q querier, row interface{ Scan(...any) error }) (int64, identity.Account, error) {
	var id int64
	var a identity.Account
	var u, mb []byte
	var roles, services string
	var totp sql.NullString
	if err := row.Scan(&id, &u, &a.Username, &a.Domain, &mb, &a.Name, &roles, &services, &a.Enabled, &totp, &a.Created, &a.Updated); err != nil {
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
	rows, err := q.QueryContext(ctx, "SELECT id, hash, kind, comment, expires_at, created FROM account_passwords WHERE account_id = ? ORDER BY position", id)
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
		if _, err := tx.ExecContext(ctx, "INSERT INTO account_passwords (id, account_id, position, hash, kind, comment, expires_at, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			p.ID, accountID, i, p.Hash, p.Kind, p.Comment, nullTime(p.ExpiresAt), p.Created.UTC()); err != nil {
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
		mbID, err := d.mailboxRowID(ctx, tx, a.MailboxID)
		if err != nil {
			return fmt.Errorf("mailbox %s: %w", a.MailboxID, err)
		}
		domID, err := d.domainID(ctx, tx, domain)
		if err != nil {
			return err
		}
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
	_, a, err := d.scanAccount(ctx, d.r(), d.r().QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.uuid = ?", bin(id)))
	return identity.Versioned[identity.Account]{Value: a}, err
}

func (d *DB) GetAccountByUsername(ctx context.Context, username string) (identity.Versioned[identity.Account], error) {
	username, _, err := identity.NormalizeAddress(username)
	if err != nil {
		return identity.Versioned[identity.Account]{}, err
	}
	_, a, err := d.scanAccount(ctx, d.r(), d.r().QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.username = ?", username))
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
		a.ID = id
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
		mbID, err := d.mailboxRowID(ctx, tx, mailboxID)
		if err != nil {
			return fmt.Errorf("mailbox %s: %w", mailboxID, err)
		}
		rid, a, err := d.scanAccount(ctx, tx, tx.QueryRowContext(ctx, "SELECT "+accountCols+accountFrom+"WHERE a.uuid = ? FOR UPDATE", bin(id)))
		if err != nil {
			return err
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
	domID, err := d.domainID(ctx, d.r(), domain)
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
	err := q.QueryRowContext(ctx, "SELECT a.id, a.address, d.name, a.enabled, a.created, a.updated FROM aliases a JOIN domains d ON d.id = a.domain_id WHERE a.address = ?"+suffix, address).
		Scan(&id, &al.Address, &al.Domain, &al.Enabled, &al.Created, &al.Updated)
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

func (d *DB) validateDestinations(ctx context.Context, tx *sql.Tx, address string, dsts []identity.Destination) ([]int64, error) {
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
		rid, err := d.mailboxRowID(ctx, tx, dst.MailboxID)
		if err != nil {
			return nil, fmt.Errorf("mailbox %s: %w", dst.MailboxID, err)
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
		mbIDs, err := d.validateDestinations(ctx, tx, address, al.Destinations)
		if err != nil {
			return err
		}
		domID, err := d.domainID(ctx, tx, domain)
		if err != nil {
			return err
		}
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
	rid, err := d.mailboxRowID(ctx, d.r(), id)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	rows, err := d.r().QueryContext(ctx, "SELECT DISTINCT a.address FROM aliases a JOIN alias_destinations ad ON ad.alias_id = a.id WHERE ad.mailbox_id = ? ORDER BY a.address", rid)
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
	_, al, err := d.loadAlias(ctx, d.r(), address, false)
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
		if err := fn(&al); err != nil {
			return err
		}
		mbIDs, err := d.validateDestinations(ctx, tx, address, al.Destinations)
		if err != nil {
			return err
		}
		al.Address, al.Domain = address, domain
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
	domID, err := d.domainID(ctx, d.r(), domain)
	if err != nil {
		if errors.Is(err, identity.ErrDomainUnknown) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return d.listStrings(ctx, "SELECT address FROM aliases WHERE domain_id = ? AND address > ? ORDER BY address LIMIT ?", domID, after, max)
}

// ---- tokens ----

const tokenCols = "t.id, t.hash, a.uuid, t.roles, t.comment, t.expires_at, t.created, t.last_used"
const tokenFrom = " FROM tokens t LEFT JOIN accounts a ON a.id = t.account_id "

func scanToken(row interface{ Scan(...any) error }) (identity.Token, error) {
	var t identity.Token
	var acct []byte
	var roles string
	var last sql.NullTime
	if err := row.Scan(&t.ID, &t.Hash, &acct, &roles, &t.Comment, &t.ExpiresAt, &t.Created, &last); err != nil {
		return identity.Token{}, mapErr(err)
	}
	if len(acct) == 16 {
		t.AccountID = toUUID(acct)
	}
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

func (d *DB) CreateAdminToken(ctx context.Context, role string, expiresAt time.Time, comment string) (identity.Token, string, error) {
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
	secret, err := identity.NewTokenSecret()
	if err != nil {
		return identity.Token{}, "", err
	}
	t := identity.Token{ID: uuid.NewString(), Hash: identity.HashToken(secret), Roles: []string{role}, ExpiresAt: expiresAt.UTC(), Comment: comment, Created: now}
	if _, err := d.exec(ctx, "INSERT INTO tokens (id, hash, account_id, roles, comment, expires_at, created) VALUES (?, ?, NULL, ?, ?, ?, ?)",
		t.ID, t.Hash, joinList(t.Roles), t.Comment, t.ExpiresAt, t.Created); err != nil {
		return identity.Token{}, "", mapErr(err)
	}
	return t, secret, nil
}

func (d *DB) listTokens(ctx context.Context, q string, args ...any) ([]identity.Token, error) {
	rows, err := d.r().QueryContext(ctx, q, args...)
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
	t, err := scanToken(d.r().QueryRowContext(ctx, "SELECT "+tokenCols+tokenFrom+"WHERE t.hash = ?", identity.HashToken(secret)))
	return identity.Versioned[identity.Token]{Value: t}, err
}

func (d *DB) TouchToken(ctx context.Context, t identity.Versioned[identity.Token]) {
	_, _ = d.exec(ctx, "UPDATE tokens SET last_used = ? WHERE hash = ?", d.Now().UTC(), t.Value.Hash)
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

// ---- clusters ----

const clusterCols = "id, fsid, name, mon_host, ceph_user, ceph_key, pool_messages, pool_attachments, pool_index, placement, created, updated"

func scanCluster(row interface{ Scan(...any) error }) (identity.Cluster, error) {
	var c identity.Cluster
	var fsid []byte
	if err := row.Scan(&c.ID, &fsid, &c.Name, &c.MonHost, &c.User, &c.Key, &c.Pools.Messages, &c.Pools.Attachments, &c.Pools.Index, &c.Placement, &c.Created, &c.Updated); err != nil {
		return identity.Cluster{}, mapErr(err)
	}
	if len(fsid) == 16 {
		c.FSID = toUUID(fsid)
	}
	return c, nil
}

func (d *DB) CreateCluster(ctx context.Context, c identity.Cluster) (identity.Cluster, error) {
	if err := identity.ValidateCluster(&c); err != nil {
		return identity.Cluster{}, err
	}
	now := d.Now().UTC()
	c.Created, c.Updated = now, now
	res, err := d.exec(ctx, "INSERT INTO clusters (fsid, name, mon_host, ceph_user, ceph_key, pool_messages, pool_attachments, pool_index, placement, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		nullBin(c.FSID), c.Name, c.MonHost, c.User, c.Key, c.Pools.Messages, c.Pools.Attachments, c.Pools.Index, c.Placement, now, now)
	if err != nil {
		return identity.Cluster{}, mapErr(err)
	}
	c.ID, _ = res.LastInsertId()
	return c, nil
}

func (d *DB) GetCluster(ctx context.Context, id int64) (identity.Cluster, error) {
	return scanCluster(d.r().QueryRowContext(ctx, "SELECT "+clusterCols+" FROM clusters WHERE id = ?", id))
}

func (d *DB) ListClusters(ctx context.Context) ([]identity.Cluster, error) {
	rows, err := d.r().QueryContext(ctx, "SELECT "+clusterCols+" FROM clusters ORDER BY id")
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []identity.Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (d *DB) UpdateCluster(ctx context.Context, id int64, fn func(*identity.Cluster) error) (identity.Cluster, error) {
	var out identity.Cluster
	err := d.tx(ctx, func(tx *sql.Tx) error {
		c, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterCols+" FROM clusters WHERE id = ? FOR UPDATE", id))
		if err != nil {
			return err
		}
		if err := fn(&c); err != nil {
			return err
		}
		if err := identity.ValidateCluster(&c); err != nil {
			return err
		}
		c.ID = id
		c.Updated = d.Now().UTC()
		if _, err := tx.ExecContext(ctx, "UPDATE clusters SET fsid = ?, name = ?, mon_host = ?, ceph_user = ?, ceph_key = ?, pool_messages = ?, pool_attachments = ?, pool_index = ?, placement = ?, updated = ? WHERE id = ?",
			nullBin(c.FSID), c.Name, c.MonHost, c.User, c.Key, c.Pools.Messages, c.Pools.Attachments, c.Pools.Index, c.Placement, c.Updated, id); err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

func (d *DB) DeleteCluster(ctx context.Context, id int64) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		if _, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterCols+" FROM clusters WHERE id = ? FOR UPDATE", id)); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM mailboxes WHERE cluster_id = ?", id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: cluster %d has %d mailboxes", identity.ErrInUse, id, n)
		}
		_, err := tx.ExecContext(ctx, "DELETE FROM clusters WHERE id = ?", id)
		return err
	})
}

var _ identity.Backend = (*DB)(nil)
