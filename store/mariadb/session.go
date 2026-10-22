package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	cryptoauth "kittiwake/auth"
	"kittiwake/store/session"
)

const sessionCols = "id, account_uuid, refresh_hash, prev_refresh_hash, roles, client, created, last_used, last_ip, expires_at"

func scanSession(row interface{ Scan(...any) error }) (session.Session, error) {
	var s session.Session
	var acct []byte
	var prev sql.NullString
	var roles string
	if err := row.Scan(&s.ID, &acct, &s.RefreshHash, &prev, &roles, &s.Client, &s.Created, &s.LastUsed, &s.LastIP, &s.ExpiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return session.Session{}, session.ErrNotFound
		}
		return session.Session{}, err
	}
	s.AccountID = toUUID(acct)
	s.PrevRefreshHash = prev.String
	s.Roles = splitList(roles)
	return s, nil
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (d *DB) Create(ctx context.Context, acct uuid.UUID, roles []string, client, ip string, lifetime time.Duration) (session.Session, string, error) {
	plain, hash, err := cryptoauth.NewRefreshToken()
	if err != nil {
		return session.Session{}, "", err
	}
	now := d.Now().UTC()
	s := session.Session{ID: uuid.NewString(), AccountID: acct, RefreshHash: hash, Roles: roles, Client: client, Created: now, LastUsed: now, LastIP: ip, ExpiresAt: now.Add(lifetime)}
	_, err = d.sql.ExecContext(ctx, "INSERT INTO sessions ("+sessionCols+") VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)",
		s.ID, bin(acct), s.RefreshHash, joinList(roles), client, now, now, ip, s.ExpiresAt)
	if err != nil {
		return session.Session{}, "", err
	}
	return s, plain, nil
}

func (d *DB) Get(ctx context.Context, acct uuid.UUID, id string) (session.Session, error) {
	return scanSession(d.sql.QueryRowContext(ctx, "SELECT "+sessionCols+" FROM sessions WHERE id = ? AND account_uuid = ?", id, bin(acct)))
}

func (d *DB) Refresh(ctx context.Context, acct uuid.UUID, id, presented, ip string, lifetime time.Duration) (session.Session, string, error) {
	var out session.Session
	var plain string
	var verdict error // ErrReused / ErrExpired: the session is revoked after the transaction
	err := d.tx(ctx, func(tx *sql.Tx) error {
		verdict = nil
		s, err := scanSession(tx.QueryRowContext(ctx, "SELECT "+sessionCols+" FROM sessions WHERE id = ? AND account_uuid = ? FOR UPDATE", id, bin(acct)))
		if err != nil {
			return err
		}
		now := d.Now().UTC()
		h := cryptoauth.HashRefreshToken(presented)
		switch {
		case cryptoauth.ConstantTimeEqual(h, s.RefreshHash):
		case s.PrevRefreshHash != "" && cryptoauth.ConstantTimeEqual(h, s.PrevRefreshHash):
			verdict = session.ErrReused
			return nil
		default:
			return session.ErrNotFound
		}
		if !now.Before(s.ExpiresAt) {
			verdict = session.ErrExpired
			return nil
		}
		var hash string
		plain, hash, err = cryptoauth.NewRefreshToken()
		if err != nil {
			return err
		}
		s.PrevRefreshHash = s.RefreshHash
		s.RefreshHash = hash
		s.LastUsed = now
		s.LastIP = ip
		s.ExpiresAt = now.Add(lifetime)
		if _, err := tx.ExecContext(ctx, "UPDATE sessions SET refresh_hash = ?, prev_refresh_hash = ?, last_used = ?, last_ip = ?, expires_at = ? WHERE id = ?",
			s.RefreshHash, nullStr(s.PrevRefreshHash), s.LastUsed, s.LastIP, s.ExpiresAt, id); err != nil {
			return err
		}
		out = s
		return nil
	})
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return session.Session{}, "", session.ErrNotFound
		}
		return session.Session{}, "", err
	}
	if verdict != nil {
		_ = d.Revoke(ctx, acct, id)
		return session.Session{}, "", verdict
	}
	return out, plain, nil
}

func (d *DB) Touch(ctx context.Context, acct uuid.UUID, id, ip string) error {
	var res sql.Result
	var err error
	if ip != "" {
		res, err = d.sql.ExecContext(ctx, "UPDATE sessions SET last_used = ?, last_ip = ? WHERE id = ? AND account_uuid = ?", d.Now().UTC(), ip, id, bin(acct))
	} else {
		res, err = d.sql.ExecContext(ctx, "UPDATE sessions SET last_used = ? WHERE id = ? AND account_uuid = ?", d.Now().UTC(), id, bin(acct))
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return session.ErrNotFound
	}
	return nil
}

func (d *DB) Revoke(ctx context.Context, acct uuid.UUID, id string) error {
	_, err := d.sql.ExecContext(ctx, "DELETE FROM sessions WHERE id = ? AND account_uuid = ?", id, bin(acct))
	return err
}

func (d *DB) RevokeAll(ctx context.Context, acct uuid.UUID) error {
	_, err := d.sql.ExecContext(ctx, "DELETE FROM sessions WHERE account_uuid = ?", bin(acct))
	return err
}

func (d *DB) List(ctx context.Context, acct uuid.UUID) ([]session.Session, error) {
	now := d.Now().UTC()
	_, _ = d.sql.ExecContext(ctx, "DELETE FROM sessions WHERE account_uuid = ? AND expires_at <= ?", bin(acct), now)
	rows, err := d.sql.QueryContext(ctx, "SELECT "+sessionCols+" FROM sessions WHERE account_uuid = ? AND expires_at > ? ORDER BY created, id", bin(acct), now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []session.Session
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ---- signing keys: one row, optimistic version ----

func (d *DB) Keys(ctx context.Context) (session.KeySet, error) {
	for attempt := 0; attempt < 3; attempt++ {
		var raw string
		err := d.sql.QueryRowContext(ctx, "SELECT keyset FROM signing_keys WHERE id = 1").Scan(&raw)
		if err == nil {
			var ks session.KeySet
			if err := json.Unmarshal([]byte(raw), &ks); err != nil {
				return session.KeySet{}, fmt.Errorf("session: decode keys: %w", err)
			}
			return ks, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return session.KeySet{}, err
		}
		key, err := cryptoauth.NewSigningKey(d.Now().UTC())
		if err != nil {
			return session.KeySet{}, err
		}
		ks := session.KeySet{Keys: []*cryptoauth.SigningKey{key}, CurrentID: key.ID}
		b, _ := json.Marshal(&ks)
		if _, err := d.sql.ExecContext(ctx, "INSERT IGNORE INTO signing_keys (id, keyset, version) VALUES (1, ?, 1)", string(b)); err != nil {
			return session.KeySet{}, err
		}
	}
	return session.KeySet{}, session.ErrConflict
}

func (d *DB) Rotate(ctx context.Context, keep int) (session.KeySet, error) {
	if _, err := d.Keys(ctx); err != nil {
		return session.KeySet{}, err
	}
	var out session.KeySet
	err := d.tx(ctx, func(tx *sql.Tx) error {
		var raw string
		var ver int64
		if err := tx.QueryRowContext(ctx, "SELECT keyset, version FROM signing_keys WHERE id = 1 FOR UPDATE").Scan(&raw, &ver); err != nil {
			return err
		}
		var ks session.KeySet
		if err := json.Unmarshal([]byte(raw), &ks); err != nil {
			return err
		}
		key, err := cryptoauth.NewSigningKey(d.Now().UTC())
		if err != nil {
			return err
		}
		ks.Keys = append([]*cryptoauth.SigningKey{key}, ks.Keys...)
		if len(ks.Keys) > keep+1 {
			ks.Keys = ks.Keys[:keep+1]
		}
		ks.CurrentID = key.ID
		b, _ := json.Marshal(&ks)
		if _, err := tx.ExecContext(ctx, "UPDATE signing_keys SET keyset = ?, version = ? WHERE id = 1", string(b), ver+1); err != nil {
			return err
		}
		out = ks
		return nil
	})
	return out, err
}

// ---- TOTP replay state ----

func (d *DB) ClaimTOTPStep(ctx context.Context, acct uuid.UUID, step int64) (bool, error) {
	var ok bool
	err := d.tx(ctx, func(tx *sql.Tx) error {
		var last sql.NullInt64
		err := tx.QueryRowContext(ctx, "SELECT last_step FROM totp_steps WHERE account_uuid = ? FOR UPDATE", bin(acct)).Scan(&last)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if last.Valid && step <= last.Int64 {
			ok = false
			return nil
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO totp_steps (account_uuid, last_step) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_step = VALUES(last_step)", bin(acct), step); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

func (d *DB) ResetTOTP(ctx context.Context, acct uuid.UUID) error {
	_, err := d.sql.ExecContext(ctx, "DELETE FROM totp_steps WHERE account_uuid = ?", bin(acct))
	return err
}

var _ session.Store = (*DB)(nil)
