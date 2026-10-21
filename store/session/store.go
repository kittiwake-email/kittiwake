// Package session persists OAuth sessions (refresh tokens), JWT signing keys
// and TOTP replay state in the RADOS index pool. See README.md "API
// authentication: sessions and tokens".
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"dovetail/auth"
	"dovetail/rados"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("session: not found")
	ErrReused   = errors.New("session: refresh token reuse detected; session revoked")
	ErrExpired  = errors.New("session: expired")
	ErrConflict = errors.New("session: concurrent modification")
)

// Session is one login session; the refresh token is stored only as a hash.
type Session struct {
	ID              string    `json:"id"`
	AccountID       uuid.UUID `json:"account_id"`
	RefreshHash     string    `json:"refresh_hash"`
	PrevRefreshHash string    `json:"prev_refresh_hash,omitempty"`
	Roles           []string  `json:"roles"`
	Client          string    `json:"client"`
	Created         time.Time `json:"created"`
	LastUsed        time.Time `json:"last_used"`
	LastIP          string    `json:"last_ip,omitempty"`
	ExpiresAt       time.Time `json:"expires_at"`
}

// KeySet is the stored JWT signing key set.
type KeySet struct {
	Keys      []*auth.SigningKey `json:"keys"`
	CurrentID string             `json:"current"`
}

// Current returns the current signing key.
func (k *KeySet) Current() *auth.SigningKey {
	for _, key := range k.Keys {
		if key.ID == k.CurrentID {
			return key
		}
	}
	return nil
}

// ByID returns the key with the id, or nil.
func (k *KeySet) ByID(id string) *auth.SigningKey {
	for _, key := range k.Keys {
		if key.ID == id {
			return key
		}
	}
	return nil
}

// Store is the session store.
type Store struct {
	io  rados.IOContext
	Now func() time.Time
}

// New returns a store on the index pool context.
func New(indexPool rados.IOContext) *Store {
	return &Store{io: indexPool, Now: time.Now}
}

const keysOID = "keys/jwt"

func sessionsOID(acct uuid.UUID) string { return "sessions/" + acct.String() }
func totpOID(acct uuid.UUID) string     { return "totp/" + acct.String() }

func mapErr(err error) error {
	switch {
	case errors.Is(err, rados.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, rados.ErrVersionMismatch):
		return ErrConflict
	}
	return err
}

// ---- sessions ----

// Create stores a new session and returns it with the plaintext refresh token.
func (s *Store) Create(ctx context.Context, acct uuid.UUID, roles []string, client, ip string, lifetime time.Duration) (Session, string, error) {
	plain, hash, err := auth.NewRefreshToken()
	if err != nil {
		return Session{}, "", err
	}
	now := s.Now().UTC()
	sess := Session{
		ID: uuid.NewString(), AccountID: acct, RefreshHash: hash, Roles: roles, Client: client,
		Created: now, LastUsed: now, LastIP: ip, ExpiresAt: now.Add(lifetime),
	}
	if err := s.put(ctx, sess); err != nil {
		return Session{}, "", err
	}
	return sess, plain, nil
}

func (s *Store) put(ctx context.Context, sess Session) error {
	data, err := json.Marshal(&sess)
	if err != nil {
		return err
	}
	_, err = s.io.Write(ctx, sessionsOID(sess.AccountID), rados.NewWriteTx().OmapSet(map[string][]byte{sess.ID: data}))
	return mapErr(err)
}

// Get returns a session.
func (s *Store) Get(ctx context.Context, acct uuid.UUID, id string) (Session, error) {
	m, err := s.io.OmapGet(ctx, sessionsOID(acct), []string{id})
	if err != nil {
		return Session{}, mapErr(err)
	}
	v, ok := m[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	var sess Session
	if err := json.Unmarshal(v, &sess); err != nil {
		return Session{}, fmt.Errorf("session: decode: %w", err)
	}
	return sess, nil
}

// Refresh validates the presented refresh token for the session, rotates it
// and extends the sliding expiry. A reused (already rotated) token revokes
// the session and returns ErrReused. Returns the new plaintext token.
func (s *Store) Refresh(ctx context.Context, acct uuid.UUID, id, presented, ip string, lifetime time.Duration) (Session, string, error) {
	sess, err := s.Get(ctx, acct, id)
	if err != nil {
		return Session{}, "", err
	}
	now := s.Now().UTC()
	h := auth.HashRefreshToken(presented)
	switch {
	case auth.ConstantTimeEqual(h, sess.RefreshHash):
		// ok
	case sess.PrevRefreshHash != "" && auth.ConstantTimeEqual(h, sess.PrevRefreshHash):
		_ = s.Revoke(ctx, acct, id)
		return Session{}, "", ErrReused
	default:
		return Session{}, "", ErrNotFound
	}
	if !now.Before(sess.ExpiresAt) {
		_ = s.Revoke(ctx, acct, id)
		return Session{}, "", ErrExpired
	}
	plain, hash, err := auth.NewRefreshToken()
	if err != nil {
		return Session{}, "", err
	}
	sess.PrevRefreshHash = sess.RefreshHash
	sess.RefreshHash = hash
	sess.LastUsed = now
	sess.LastIP = ip
	sess.ExpiresAt = now.Add(lifetime)
	if err := s.put(ctx, sess); err != nil {
		return Session{}, "", err
	}
	return sess, plain, nil
}

// Touch records use of the session (access token issued or used).
func (s *Store) Touch(ctx context.Context, acct uuid.UUID, id, ip string) error {
	sess, err := s.Get(ctx, acct, id)
	if err != nil {
		return err
	}
	sess.LastUsed = s.Now().UTC()
	if ip != "" {
		sess.LastIP = ip
	}
	return s.put(ctx, sess)
}

// Revoke deletes a session. Missing is not an error.
func (s *Store) Revoke(ctx context.Context, acct uuid.UUID, id string) error {
	_, err := s.io.Write(ctx, sessionsOID(acct), rados.NewWriteTx().OmapRemove(id))
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return mapErr(err)
}

// RevokeAll deletes every session of the account.
func (s *Store) RevokeAll(ctx context.Context, acct uuid.UUID) error {
	err := s.io.Delete(ctx, sessionsOID(acct))
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return mapErr(err)
}

// List returns the account's sessions, dropping expired ones.
func (s *Store) List(ctx context.Context, acct uuid.UUID) ([]Session, error) {
	kv, _, err := s.io.OmapList(ctx, sessionsOID(acct), "", "", 0)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, nil
		}
		return nil, mapErr(err)
	}
	now := s.Now().UTC()
	var out []Session
	var expired []string
	for _, e := range kv {
		var sess Session
		if err := json.Unmarshal(e.Value, &sess); err != nil {
			continue
		}
		if !now.Before(sess.ExpiresAt) {
			expired = append(expired, e.Key)
			continue
		}
		out = append(out, sess)
	}
	if len(expired) > 0 {
		_, _ = s.io.Write(ctx, sessionsOID(acct), rados.NewWriteTx().OmapRemove(expired...))
	}
	return out, nil
}

// ---- signing keys ----

// Keys returns the key set, creating the first key if none exists.
func (s *Store) Keys(ctx context.Context) (KeySet, error) {
	for attempt := 0; attempt < 3; attempt++ {
		data, _, err := s.io.Get(ctx, keysOID)
		if err == nil {
			var ks KeySet
			if err := json.Unmarshal(data, &ks); err != nil {
				return KeySet{}, fmt.Errorf("session: decode keys: %w", err)
			}
			return ks, nil
		}
		if !errors.Is(err, rados.ErrNotFound) {
			return KeySet{}, mapErr(err)
		}
		key, err := auth.NewSigningKey(s.Now().UTC())
		if err != nil {
			return KeySet{}, err
		}
		ks := KeySet{Keys: []*auth.SigningKey{key}, CurrentID: key.ID}
		b, _ := json.Marshal(&ks)
		if _, err := s.io.Write(ctx, keysOID, rados.NewWriteTx().CreateExclusive().WriteFull(b)); err != nil && !errors.Is(err, rados.ErrExists) {
			return KeySet{}, mapErr(err)
		}
	}
	return KeySet{}, ErrConflict
}

// Rotate adds a new current key and keeps the previous `keep` keys so
// outstanding access tokens still verify.
func (s *Store) Rotate(ctx context.Context, keep int) (KeySet, error) {
	if _, err := s.Keys(ctx); err != nil {
		return KeySet{}, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		data, ver, err := s.io.Get(ctx, keysOID)
		if err != nil {
			return KeySet{}, mapErr(err)
		}
		var ks KeySet
		if err := json.Unmarshal(data, &ks); err != nil {
			return KeySet{}, err
		}
		key, err := auth.NewSigningKey(s.Now().UTC())
		if err != nil {
			return KeySet{}, err
		}
		ks.Keys = append([]*auth.SigningKey{key}, ks.Keys...)
		if len(ks.Keys) > keep+1 {
			ks.Keys = ks.Keys[:keep+1]
		}
		ks.CurrentID = key.ID
		b, _ := json.Marshal(&ks)
		if _, err := s.io.Write(ctx, keysOID, rados.NewWriteTx().AssertVersion(ver).WriteFull(b)); err != nil {
			if errors.Is(err, rados.ErrVersionMismatch) {
				continue
			}
			return KeySet{}, mapErr(err)
		}
		return ks, nil
	}
	return KeySet{}, ErrConflict
}

// ---- TOTP replay state ----

// ClaimTOTPStep records that `step` was used for the account. It returns
// false if step is not greater than the last recorded step (replay).
func (s *Store) ClaimTOTPStep(ctx context.Context, acct uuid.UUID, step int64) (bool, error) {
	for attempt := 0; attempt < 5; attempt++ {
		data, ver, err := s.io.Get(ctx, totpOID(acct))
		if errors.Is(err, rados.ErrNotFound) {
			_, err := s.io.Write(ctx, totpOID(acct), rados.NewWriteTx().CreateExclusive().WriteFull([]byte(strconv.FormatInt(step, 10))))
			if errors.Is(err, rados.ErrExists) {
				continue
			}
			return err == nil, mapErr(err)
		}
		if err != nil {
			return false, mapErr(err)
		}
		last, _ := strconv.ParseInt(string(data), 10, 64)
		if step <= last {
			return false, nil
		}
		_, err = s.io.Write(ctx, totpOID(acct), rados.NewWriteTx().AssertVersion(ver).WriteFull([]byte(strconv.FormatInt(step, 10))))
		if errors.Is(err, rados.ErrVersionMismatch) {
			continue
		}
		return err == nil, mapErr(err)
	}
	return false, ErrConflict
}

// ResetTOTP clears replay state (when TOTP is disabled or re-enrolled).
func (s *Store) ResetTOTP(ctx context.Context, acct uuid.UUID) error {
	err := s.io.Delete(ctx, totpOID(acct))
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return mapErr(err)
}
