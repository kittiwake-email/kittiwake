// Package auth is the authentication service: password logins with TOTP,
// OAuth-style sessions (JWT access tokens + rotating refresh tokens),
// long-lived API tokens, and the per-protocol credential rules. Every
// protocol adapter authenticates through this package.
package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	cryptoauth "dovetail/auth"
	"dovetail/metrics"
	"dovetail/service/actor"
	"dovetail/service/auditor"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/session"
)

// Errors returned to protocol adapters. They are deliberately coarse so
// callers cannot leak which part failed, except where the spec requires it
// (TOTP required).
var (
	ErrInvalidCredentials = errors.New("auth: invalid credentials")
	ErrTOTPRequired       = errors.New("auth: totp code required")
	ErrTOTPInvalid        = errors.New("auth: totp code invalid")
	ErrAccountDisabled    = errors.New("auth: account disabled")
	ErrTokenExpired       = errors.New("auth: token expired")
	ErrInvalidToken       = errors.New("auth: invalid token")
	ErrScope              = errors.New("auth: requested scope exceeds account roles")
	ErrSessionRevoked     = errors.New("auth: session revoked")
)

// Config for the service.
type Config struct {
	AccessTokenLifetime  time.Duration
	RefreshTokenLifetime time.Duration
	Issuer               string
	TOTPSkew             int // steps either side, default 1
	KeyCacheTTL          time.Duration
	// CacheTTL caches successful credential verifications (password and
	// API token) so repeated requests skip RADOS and argon2id. 0 disables.
	CacheTTL time.Duration
}

// Service is the auth service.
type Service struct {
	ids      *identity.Store
	sessions session.Store
	audit    *auditor.Auditor
	cfg      Config
	Now      func() time.Time

	keyMu     sync.Mutex
	keys      session.KeySet
	keysAt    time.Time
	keysValid bool

	cache credCache
}

// credCache remembers successful verifications for Config.CacheTTL. Keys
// are opaque (hash of the credential); values are the resulting actor.
// Only positive results are cached, so a wrong password is always checked
// (and still costs the hash: rate-limit at the proxy).
type credCache struct {
	mu      sync.Mutex
	entries map[string]credEntry
	byAcct  map[uuid.UUID][]string // account -> keys, for invalidation
}

type credEntry struct {
	actor actor.Actor
	at    time.Time
}

const maxCredEntries = 20000

func (c *credCache) get(key string, now time.Time, ttl time.Duration) (actor.Actor, bool) {
	if ttl <= 0 {
		return actor.Actor{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || now.Sub(e.at) >= ttl {
		return actor.Actor{}, false
	}
	a := e.actor
	a.Roles = append([]string(nil), e.actor.Roles...)
	return a, true
}

func (c *credCache) put(key string, a actor.Actor, now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]credEntry{}
		c.byAcct = map[uuid.UUID][]string{}
	}
	if len(c.entries) >= maxCredEntries {
		c.entries = map[string]credEntry{}
		c.byAcct = map[uuid.UUID][]string{}
	}
	c.entries[key] = credEntry{actor: a, at: now}
	if a.AccountID != uuid.Nil {
		c.byAcct[a.AccountID] = append(c.byAcct[a.AccountID], key)
	}
}

// invalidateAccount drops cached verifications of one account (this
// process; other processes rely on the TTL).
func (c *credCache) invalidateAccount(id uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range c.byAcct[id] {
		delete(c.entries, k)
	}
	delete(c.byAcct, id)
}

func (c *credCache) clear() {
	c.mu.Lock()
	c.entries, c.byAcct = nil, nil
	c.mu.Unlock()
}

func passwordKey(username, password string) string {
	sum := sha256.Sum256([]byte(username + "\x00" + password))
	return "p:" + hex.EncodeToString(sum[:])
}

func tokenKey(token string) string { return "t:" + identity.HashToken(token) }

// InvalidateCache drops every cached verification in this process.
func (s *Service) InvalidateCache() { s.cache.clear() }

// New returns the service.
func New(ids *identity.Store, sessions session.Store, aud *auditor.Auditor, cfg Config) *Service {
	if cfg.TOTPSkew == 0 {
		cfg.TOTPSkew = 1
	}
	if cfg.KeyCacheTTL == 0 {
		cfg.KeyCacheTTL = time.Minute
	}
	if cfg.AccessTokenLifetime == 0 {
		cfg.AccessTokenLifetime = 15 * time.Minute
	}
	if cfg.RefreshTokenLifetime == 0 {
		cfg.RefreshTokenLifetime = 30 * 24 * time.Hour
	}
	if cfg.Issuer == "" {
		cfg.Issuer = "dovetail"
	}
	return &Service{ids: ids, sessions: sessions, audit: aud, cfg: cfg, Now: time.Now}
}

// Tokens is the token endpoint response.
type Tokens struct {
	AccessToken  string
	TokenType    string
	ExpiresIn    int
	RefreshToken string
	SessionID    string
	Scope        []string
}

// LoginRequest is a password login.
type LoginRequest struct {
	Username string
	Password string
	TOTPCode string
	Scope    []string // requested roles; empty = all of the account's
	Client   string   // device/client name
	ClientIP string
	Protocol string // for audit; default "rest"
}

// matchPassword finds the (unexpired) password entry that matches plain.
func (s *Service) matchPassword(acct *identity.Account, plain string) (*identity.Password, bool) {
	now := s.Now()
	for i := range acct.Passwords {
		if acct.Passwords[i].Expired(now) {
			continue
		}
		ok, err := cryptoauth.VerifyPassword(acct.Passwords[i].Hash, plain)
		if err == nil && ok {
			return &acct.Passwords[i], true
		}
	}
	return nil, false
}

// totpRequired reports whether a login with pw needs a TOTP code.
func totpRequired(acct *identity.Account, pw *identity.Password) bool {
	return acct.TOTP != nil && acct.TOTP.Confirmed && pw.Kind == identity.PasswordPrimary
}

func totpParams(t *identity.TOTP) cryptoauth.TOTPParams {
	p := cryptoauth.DefaultTOTPParams()
	if t.Digits > 0 {
		p.Digits = t.Digits
	}
	if t.Period > 0 {
		p.Period = t.Period
	}
	if t.Algorithm != "" {
		p.Algorithm = t.Algorithm
	}
	return p
}

// Login authenticates with username/password (+TOTP) and opens a session.
func (s *Service) Login(ctx context.Context, req LoginRequest) (Tokens, error) {
	proto := req.Protocol
	if proto == "" {
		proto = audit.ProtoREST
	}
	auditCtx := actor.With(ctx, actor.Actor{Username: strings.ToLower(strings.TrimSpace(req.Username)), Protocol: proto, ClientIP: req.ClientIP, Credential: "password"})
	fail := func(detail string, err error) (Tokens, error) {
		metrics.Auth.WithLabelValues(proto, "failed").Inc()
		s.audit.Log(auditCtx, auditor.Event{Action: "login-failed", Detail: detail, Result: audit.ResultDenied})
		return Tokens{}, err
	}
	v, err := s.ids.GetAccountByUsername(ctx, req.Username)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) || errors.Is(err, identity.ErrInvalid) {
			// Burn time comparably to a real verify so timing does not leak existence.
			_, _ = cryptoauth.VerifyPassword(dummyHash, req.Password)
			return fail("unknown account", ErrInvalidCredentials)
		}
		return Tokens{}, err
	}
	acct := v.Value
	pw, ok := s.matchPassword(&acct, req.Password)
	if !ok {
		return fail("wrong password", ErrInvalidCredentials)
	}
	if !acct.Enabled {
		return fail("account disabled", ErrAccountDisabled)
	}
	if totpRequired(&acct, pw) {
		if req.TOTPCode == "" {
			return fail("totp required", ErrTOTPRequired)
		}
		if err := s.checkTOTP(ctx, &acct, req.TOTPCode); err != nil {
			return fail("totp invalid", err)
		}
	}
	roles, err := narrowRoles(acct.Roles, req.Scope)
	if err != nil {
		return fail("scope", err)
	}
	cred := "password"
	if pw.Kind == identity.PasswordApplication {
		cred = "app-password"
	}
	sess, refresh, err := s.sessions.Create(ctx, acct.ID, roles, req.Client, req.ClientIP, s.cfg.RefreshTokenLifetime)
	if err != nil {
		return Tokens{}, err
	}
	toks, err := s.issue(ctx, &acct, sess, refresh)
	if err != nil {
		return Tokens{}, err
	}
	metrics.Auth.WithLabelValues(proto, "ok").Inc()
	s.audit.Log(actor.With(ctx, s.actorFor(&acct, roles, cred, "", sess.ID, proto, req.ClientIP)),
		auditor.Event{Mailbox: acct.MailboxID, Action: "login", Detail: "client=" + req.Client})
	// Opportunistic hash upgrade.
	if cryptoauth.NeedsRehash(pw.Hash) {
		s.rehash(ctx, acct.ID, pw.ID, req.Password)
	}
	return toks, nil
}

// dummyHash is an argon2id hash of a random string, used to equalise timing.
var dummyHash = func() string {
	h, _ := cryptoauth.HashPassword("dovetail-dummy-" + uuid.NewString())
	return h
}()

func (s *Service) rehash(ctx context.Context, acctID uuid.UUID, pwID, plain string) {
	h, err := cryptoauth.HashPassword(plain)
	if err != nil {
		return
	}
	_, _ = s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error {
		for i := range a.Passwords {
			if a.Passwords[i].ID == pwID {
				a.Passwords[i].Hash = h
			}
		}
		return nil
	})
}

func (s *Service) checkTOTP(ctx context.Context, acct *identity.Account, code string) error {
	code = strings.ReplaceAll(strings.TrimSpace(code), " ", "")
	ok, step := cryptoauth.VerifyTOTP(acct.TOTP.Secret, code, s.Now(), totpParams(acct.TOTP), s.cfg.TOTPSkew)
	if ok {
		fresh, err := s.sessions.ClaimTOTPStep(ctx, acct.ID, step)
		if err != nil {
			return err
		}
		if !fresh {
			return ErrTOTPInvalid // replay
		}
		return nil
	}
	// Recovery code?
	if i := cryptoauth.VerifyRecoveryCode(acct.TOTP.Recovery, code); i >= 0 {
		_, err := s.ids.UpdateAccount(ctx, acct.ID, func(a *identity.Account) error {
			if a.TOTP == nil || i >= len(a.TOTP.Recovery) {
				return nil
			}
			a.TOTP.Recovery = append(a.TOTP.Recovery[:i], a.TOTP.Recovery[i+1:]...)
			return nil
		})
		return err
	}
	return ErrTOTPInvalid
}

func narrowRoles(have, want []string) ([]string, error) {
	if len(want) == 0 {
		return append([]string(nil), have...), nil
	}
	var out []string
	for _, w := range want {
		found := false
		for _, h := range have {
			if h == w {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrScope, w)
		}
		out = append(out, w)
	}
	return out, nil
}

func (s *Service) actorFor(a *identity.Account, roles []string, cred, tokenID, sessionID, proto, ip string) actor.Actor {
	return actor.Actor{
		AccountID: a.ID, Username: a.Username, MailboxID: a.MailboxID, Roles: roles,
		Credential: cred, TokenID: tokenID, SessionID: sessionID, Protocol: proto, ClientIP: ip,
	}
}

// refresh token wire format: "dvr1.<session-id>.<account-id>.<secret>"
func encodeRefresh(sess session.Session, secret string) string {
	return "dvr1." + sess.ID + "." + sess.AccountID.String() + "." + secret
}

func decodeRefresh(tok string) (sessionID string, acct uuid.UUID, secret string, err error) {
	parts := strings.SplitN(tok, ".", 4)
	if len(parts) != 4 || parts[0] != "dvr1" {
		return "", uuid.Nil, "", ErrInvalidToken
	}
	id, err := uuid.Parse(parts[2])
	if err != nil {
		return "", uuid.Nil, "", ErrInvalidToken
	}
	return parts[1], id, parts[3], nil
}

func (s *Service) issue(ctx context.Context, acct *identity.Account, sess session.Session, refreshSecret string) (Tokens, error) {
	ks, err := s.keySet(ctx)
	if err != nil {
		return Tokens{}, err
	}
	key := ks.Current()
	if key == nil {
		return Tokens{}, errors.New("auth: no signing key")
	}
	now := s.Now().UTC()
	jwt, err := cryptoauth.SignAccessToken(key, cryptoauth.Claims{
		AccountID: acct.ID, MailboxID: acct.MailboxID, Username: acct.Username, Roles: sess.Roles,
		SessionID: sess.ID, Issuer: s.cfg.Issuer, IssuedAt: now, ExpiresAt: now.Add(s.cfg.AccessTokenLifetime),
	})
	if err != nil {
		return Tokens{}, err
	}
	return Tokens{
		AccessToken: jwt, TokenType: "Bearer", ExpiresIn: int(s.cfg.AccessTokenLifetime.Seconds()),
		RefreshToken: encodeRefresh(sess, refreshSecret), SessionID: sess.ID, Scope: sess.Roles,
	}, nil
}

// Refresh rotates the refresh token and issues a new access token.
func (s *Service) Refresh(ctx context.Context, refreshToken, clientIP, proto string) (Tokens, error) {
	if proto == "" {
		proto = audit.ProtoREST
	}
	sid, acctID, secret, err := decodeRefresh(refreshToken)
	if err != nil {
		return Tokens{}, err
	}
	v, err := s.ids.GetAccount(ctx, acctID)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			_ = s.sessions.RevokeAll(ctx, acctID)
			return Tokens{}, ErrInvalidToken
		}
		return Tokens{}, err
	}
	acct := v.Value
	auditCtx := actor.With(ctx, s.actorFor(&acct, nil, "session", "", sid, proto, clientIP))
	if !acct.Enabled {
		_ = s.sessions.RevokeAll(ctx, acctID)
		s.audit.Log(auditCtx, auditor.Event{Mailbox: acct.MailboxID, Action: "refresh", Detail: "account disabled", Result: audit.ResultDenied})
		return Tokens{}, ErrAccountDisabled
	}
	sess, newSecret, err := s.sessions.Refresh(ctx, acctID, sid, secret, clientIP, s.cfg.RefreshTokenLifetime)
	if err != nil {
		detail := "invalid"
		out := ErrInvalidToken
		switch {
		case errors.Is(err, session.ErrReused):
			detail, out = "reuse detected; session revoked", ErrSessionRevoked
		case errors.Is(err, session.ErrExpired):
			detail, out = "expired", ErrTokenExpired
		case errors.Is(err, session.ErrNotFound):
		default:
			return Tokens{}, err
		}
		s.audit.Log(auditCtx, auditor.Event{Mailbox: acct.MailboxID, Action: "refresh", Detail: detail, Result: audit.ResultDenied})
		return Tokens{}, out
	}
	// Roles may have been narrowed since the session was created.
	roles, err := narrowRoles(acct.Roles, sess.Roles)
	if err != nil {
		_ = s.sessions.Revoke(ctx, acctID, sid)
		s.audit.Log(auditCtx, auditor.Event{Mailbox: acct.MailboxID, Action: "refresh", Detail: "roles revoked", Result: audit.ResultDenied})
		return Tokens{}, ErrSessionRevoked
	}
	sess.Roles = roles
	toks, err := s.issue(ctx, &acct, sess, newSecret)
	if err != nil {
		return Tokens{}, err
	}
	s.audit.Log(auditCtx, auditor.Event{Mailbox: acct.MailboxID, Action: "refresh"})
	return toks, nil
}

// Revoke ends the session a refresh token belongs to (RFC 7009). Unknown
// tokens are not an error, per the RFC.
func (s *Service) Revoke(ctx context.Context, refreshToken, clientIP, proto string) error {
	sid, acctID, _, err := decodeRefresh(refreshToken)
	if err != nil {
		return nil
	}
	if err := s.sessions.Revoke(ctx, acctID, sid); err != nil {
		return err
	}
	a := actor.Actor{AccountID: acctID, SessionID: sid, Protocol: proto, ClientIP: clientIP, Credential: "session"}
	if v, err := s.ids.GetAccount(ctx, acctID); err == nil {
		a.Username, a.MailboxID = v.Value.Username, v.Value.MailboxID
	}
	s.audit.Log(actor.With(ctx, a), auditor.Event{Mailbox: a.MailboxID, Action: "revoke", Target: sid})
	return nil
}

// RevokeSession ends a session by id; used by the self-service and admin APIs.
func (s *Service) RevokeSession(ctx context.Context, acctID uuid.UUID, sessionID string) error {
	if err := s.sessions.Revoke(ctx, acctID, sessionID); err != nil {
		return err
	}
	mb := uuid.Nil
	if v, err := s.ids.GetAccount(ctx, acctID); err == nil {
		mb = v.Value.MailboxID
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "revoke", Target: sessionID})
	return nil
}

// RevokeAllSessions ends every session of an account.
func (s *Service) RevokeAllSessions(ctx context.Context, acctID uuid.UUID) error {
	return s.sessions.RevokeAll(ctx, acctID)
}

// ListSessions returns an account's live sessions.
func (s *Service) ListSessions(ctx context.Context, acctID uuid.UUID) ([]session.Session, error) {
	return s.sessions.List(ctx, acctID)
}

// ---- verification of presented credentials ----

func (s *Service) keySet(ctx context.Context) (session.KeySet, error) {
	s.keyMu.Lock()
	defer s.keyMu.Unlock()
	if s.keysValid && s.Now().Sub(s.keysAt) < s.cfg.KeyCacheTTL {
		return s.keys, nil
	}
	ks, err := s.sessions.Keys(ctx)
	if err != nil {
		return session.KeySet{}, err
	}
	s.keys, s.keysAt, s.keysValid = ks, s.Now(), true
	return ks, nil
}

// InvalidateKeyCache forces the next verification to reload keys (after rotation).
func (s *Service) InvalidateKeyCache() {
	s.keyMu.Lock()
	s.keysValid = false
	s.keyMu.Unlock()
}

// RotateKeys installs a new signing key; previous keys remain valid for
// outstanding access tokens.
func (s *Service) RotateKeys(ctx context.Context) error {
	if _, err := s.sessions.Rotate(ctx, 1); err != nil {
		return err
	}
	s.InvalidateKeyCache()
	s.audit.Log(ctx, auditor.Event{Action: "rotate-keys"})
	return nil
}

// VerifyAccessToken validates a JWT and returns the actor. Stateless: no
// store lookup. The caller sets Protocol and ClientIP on the result.
func (s *Service) VerifyAccessToken(ctx context.Context, token string) (actor.Actor, error) {
	ks, err := s.keySet(ctx)
	if err != nil {
		return actor.Actor{}, err
	}
	keyFor := func(kid string) (ed25519.PublicKey, bool) {
		if k := ks.ByID(kid); k != nil {
			return k.Public, true
		}
		// Maybe rotated since cached: reload once.
		s.InvalidateKeyCache()
		if ks2, err := s.keySet(ctx); err == nil {
			if k := ks2.ByID(kid); k != nil {
				return k.Public, true
			}
		}
		return nil, false
	}
	c, err := cryptoauth.VerifyAccessToken(token, keyFor, s.cfg.Issuer, s.Now())
	if err != nil {
		if errors.Is(err, cryptoauth.ErrTokenExpired) {
			return actor.Actor{}, ErrTokenExpired
		}
		return actor.Actor{}, ErrInvalidToken
	}
	return actor.Actor{
		AccountID: c.AccountID, Username: c.Username, MailboxID: c.MailboxID, Roles: c.Roles,
		Credential: "session", SessionID: c.SessionID,
	}, nil
}

// VerifyAPIToken validates a long-lived API token and returns the actor.
// Successful verifications are cached for Config.CacheTTL.
func (s *Service) VerifyAPIToken(ctx context.Context, token string) (actor.Actor, error) {
	if a, ok := s.cache.get(tokenKey(token), s.Now(), s.cfg.CacheTTL); ok {
		return a, nil
	}
	a, err := s.verifyAPIToken(ctx, token)
	if err == nil {
		s.cache.put(tokenKey(token), a, s.Now(), s.cfg.CacheTTL)
	}
	return a, err
}

func (s *Service) verifyAPIToken(ctx context.Context, token string) (actor.Actor, error) {
	v, err := s.ids.LookupToken(ctx, token)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return actor.Actor{}, ErrInvalidToken
		}
		return actor.Actor{}, err
	}
	t := v.Value
	if t.Expired(s.Now()) {
		return actor.Actor{}, ErrTokenExpired
	}
	if t.IsAdmin() {
		if s.Now().Sub(t.LastUsed) > time.Minute {
			s.ids.TouchToken(ctx, v)
		}
		return actor.Actor{
			Username: "admin:" + t.Comment, Roles: append([]string(nil), t.Roles...),
			Credential: "admin-token", TokenID: t.ID, TokenNote: t.Comment,
		}, nil
	}
	av, err := s.ids.GetAccount(ctx, t.AccountID)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return actor.Actor{}, ErrInvalidToken
		}
		return actor.Actor{}, err
	}
	acct := av.Value
	if !acct.Enabled {
		return actor.Actor{}, ErrAccountDisabled
	}
	roles, err := narrowRoles(acct.Roles, t.Roles)
	if err != nil {
		return actor.Actor{}, ErrInvalidToken
	}
	if s.Now().Sub(t.LastUsed) > time.Minute {
		s.ids.TouchToken(ctx, v)
	}
	a := s.actorFor(&acct, roles, "token", t.ID, "", "", "")
	a.TokenNote = t.Comment
	return a, nil
}

// VerifyBearer dispatches on the token form: API token or JWT.
func (s *Service) VerifyBearer(ctx context.Context, token string) (actor.Actor, error) {
	token = strings.TrimSpace(token)
	if strings.HasPrefix(token, "dvt_") {
		return s.VerifyAPIToken(ctx, token)
	}
	return s.VerifyAccessToken(ctx, token)
}

// VerifyPassword checks username/password for protocols that cannot carry
// a TOTP code (IMAP, POP3, HTTP basic). A primary password of a
// TOTP-enabled account is refused with ErrTOTPRequired; application
// passwords always work.
func (s *Service) VerifyPassword(ctx context.Context, username, password, proto, clientIP string) (actor.Actor, error) {
	key := passwordKey(strings.ToLower(strings.TrimSpace(username)), password)
	if a, ok := s.cache.get(key, s.Now(), s.cfg.CacheTTL); ok {
		a.Protocol, a.ClientIP = proto, clientIP
		return a, nil
	}
	a, err := s.verifyPassword(ctx, username, password, proto, clientIP)
	if err == nil {
		s.cache.put(key, a, s.Now(), s.cfg.CacheTTL)
	}
	return a, err
}

func (s *Service) verifyPassword(ctx context.Context, username, password, proto, clientIP string) (actor.Actor, error) {
	auditCtx := actor.With(ctx, actor.Actor{Username: strings.ToLower(strings.TrimSpace(username)), Protocol: proto, ClientIP: clientIP, Credential: "password"})
	fail := func(detail string, err error) (actor.Actor, error) {
		metrics.Auth.WithLabelValues(proto, "failed").Inc()
		s.audit.Log(auditCtx, auditor.Event{Action: "login-failed", Detail: detail, Result: audit.ResultDenied})
		return actor.Actor{}, err
	}
	acct, err := s.ids.GetAccountByUsernameCached(ctx, username)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) || errors.Is(err, identity.ErrInvalid) {
			_, _ = cryptoauth.VerifyPassword(dummyHash, password)
			return fail("unknown account", ErrInvalidCredentials)
		}
		return actor.Actor{}, err
	}
	pw, ok := s.matchPassword(&acct, password)
	if !ok {
		return fail("wrong password", ErrInvalidCredentials)
	}
	if !acct.Enabled {
		return fail("account disabled", ErrAccountDisabled)
	}
	if totpRequired(&acct, pw) {
		return fail("totp required; use an application password", ErrTOTPRequired)
	}
	cred := "password"
	if pw.Kind == identity.PasswordApplication {
		cred = "app-password"
	}
	a := s.actorFor(&acct, acct.Roles, cred, "", "", proto, clientIP)
	metrics.Auth.WithLabelValues(proto, "ok").Inc()
	s.audit.Log(actor.With(ctx, a), auditor.Event{Mailbox: acct.MailboxID, Action: "login"})
	if cryptoauth.NeedsRehash(pw.Hash) {
		s.rehash(ctx, acct.ID, pw.ID, password)
	}
	return a, nil
}

// VerifyOAuthBearer authenticates IMAP/POP3 OAUTHBEARER/XOAUTH2: the token
// must be a session access token (or API token) whose account matches the
// presented username (if any).
func (s *Service) VerifyOAuthBearer(ctx context.Context, username, token, proto, clientIP string) (actor.Actor, error) {
	a, err := s.VerifyBearer(ctx, token)
	if err != nil {
		s.audit.Log(actor.With(ctx, actor.Actor{Username: strings.ToLower(username), Protocol: proto, ClientIP: clientIP, Credential: "session"}),
			auditor.Event{Action: "login-failed", Detail: "bearer: " + err.Error(), Result: audit.ResultDenied})
		return actor.Actor{}, err
	}
	if username != "" && !strings.EqualFold(username, a.Username) {
		s.audit.Log(actor.With(ctx, a), auditor.Event{Mailbox: a.MailboxID, Action: "login-failed", Detail: "bearer username mismatch", Result: audit.ResultDenied})
		return actor.Actor{}, ErrInvalidCredentials
	}
	a.Protocol, a.ClientIP = proto, clientIP
	s.audit.Log(actor.With(ctx, a), auditor.Event{Mailbox: a.MailboxID, Action: "login"})
	return a, nil
}

// ---- TOTP enrolment (self-service) ----

// TOTPEnrolment is what a client needs to set up an authenticator app.
type TOTPEnrolment struct {
	Secret        string
	URI           string
	RecoveryCodes []string
}

// BeginTOTP generates a secret and recovery codes for the account and
// stores them unconfirmed. Existing confirmed TOTP must be disabled first.
func (s *Service) BeginTOTP(ctx context.Context, acctID uuid.UUID) (TOTPEnrolment, error) {
	secret, err := cryptoauth.NewTOTPSecret()
	if err != nil {
		return TOTPEnrolment{}, err
	}
	codes, hashes, err := cryptoauth.NewRecoveryCodes(10)
	if err != nil {
		return TOTPEnrolment{}, err
	}
	p := cryptoauth.DefaultTOTPParams()
	var username string
	_, err = s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error {
		if a.TOTP != nil && a.TOTP.Confirmed {
			return fmt.Errorf("%w: totp already enabled", identity.ErrExists)
		}
		username = a.Username
		a.TOTP = &identity.TOTP{Secret: secret, Digits: p.Digits, Period: p.Period, Algorithm: p.Algorithm, Recovery: hashes}
		return nil
	})
	if err != nil {
		return TOTPEnrolment{}, err
	}
	_ = s.sessions.ResetTOTP(ctx, acctID)
	return TOTPEnrolment{Secret: secret, URI: cryptoauth.TOTPProvisioningURI(s.cfg.Issuer, username, secret, p), RecoveryCodes: codes}, nil
}

// ConfirmTOTP activates TOTP after the user proves the app works.
func (s *Service) ConfirmTOTP(ctx context.Context, acctID uuid.UUID, code string) error {
	v, err := s.ids.GetAccount(ctx, acctID)
	if err != nil {
		return err
	}
	acct := v.Value
	if acct.TOTP == nil {
		return ErrTOTPInvalid
	}
	ok, step := cryptoauth.VerifyTOTP(acct.TOTP.Secret, strings.TrimSpace(code), s.Now(), totpParams(acct.TOTP), s.cfg.TOTPSkew)
	if !ok {
		return ErrTOTPInvalid
	}
	if fresh, err := s.sessions.ClaimTOTPStep(ctx, acctID, step); err != nil || !fresh {
		return ErrTOTPInvalid
	}
	_, err = s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error {
		if a.TOTP != nil {
			a.TOTP.Confirmed = true
		}
		return nil
	})
	if err == nil {
		s.cache.invalidateAccount(acctID)
		s.audit.Log(ctx, auditor.Event{Mailbox: acct.MailboxID, Action: "totp-enable"})
	}
	return err
}

// DisableTOTP removes TOTP from the account (self-service or admin).
func (s *Service) DisableTOTP(ctx context.Context, acctID uuid.UUID) error {
	a, err := s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error { a.TOTP = nil; return nil })
	if err != nil {
		return err
	}
	_ = s.sessions.ResetTOTP(ctx, acctID)
	s.cache.invalidateAccount(acctID)
	s.audit.Log(ctx, auditor.Event{Mailbox: a.MailboxID, Action: "totp-disable", Subject: subjectIfOther(ctx, a.Username)})
	return nil
}

// subjectIfOther returns username when the actor is somebody else (admin
// acting on behalf), else "".
func subjectIfOther(ctx context.Context, username string) string {
	if a := actor.From(ctx); a.Username != username {
		return username
	}
	return ""
}

// ---- passwords (self-service and admin) ----

// SetPassword adds or replaces a password of the given kind. A primary
// password replaces the existing primary (a temporary one with an expiry
// is added next to it instead, so the permanent one keeps working);
// application passwords are added. The comment is required; expiresAt is
// optional (nil = never). Returns the password id.
func (s *Service) SetPassword(ctx context.Context, acctID uuid.UUID, plain, kind, comment string, expiresAt *time.Time) (string, error) {
	if len(plain) < 8 {
		return "", fmt.Errorf("%w: password too short", identity.ErrInvalid)
	}
	if kind == "" {
		kind = identity.PasswordPrimary
	}
	if kind != identity.PasswordPrimary && kind != identity.PasswordApplication {
		return "", fmt.Errorf("%w: password kind", identity.ErrInvalid)
	}
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return "", fmt.Errorf("%w: password comment is required", identity.ErrInvalid)
	}
	now := s.Now().UTC()
	if expiresAt != nil {
		t := expiresAt.UTC()
		if !t.After(now) {
			return "", fmt.Errorf("%w: password expiry must be in the future", identity.ErrInvalid)
		}
		expiresAt = &t
	}
	h, err := cryptoauth.HashPassword(plain)
	if err != nil {
		return "", err
	}
	id := uuid.NewString()
	a, err := s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error {
		kept := a.Passwords[:0]
		for _, p := range a.Passwords {
			// Replace the permanent primary with a permanent primary; drop
			// expired entries while we are here.
			if kind == identity.PasswordPrimary && expiresAt == nil && p.Kind == identity.PasswordPrimary && p.ExpiresAt == nil {
				continue
			}
			if p.Expired(now) {
				continue
			}
			kept = append(kept, p)
		}
		a.Passwords = append(kept, identity.Password{ID: id, Hash: h, Kind: kind, Comment: comment, ExpiresAt: expiresAt, Created: now})
		return nil
	})
	if err != nil {
		return "", err
	}
	s.cache.invalidateAccount(acctID)
	exp := "never"
	if expiresAt != nil {
		exp = expiresAt.Format(time.RFC3339)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: a.MailboxID, Action: "password-set", Target: kind, Detail: fmt.Sprintf("comment=%q expires=%s", comment, exp), Subject: subjectIfOther(ctx, a.Username)})
	return id, nil
}

// RemovePassword deletes a password by id.
func (s *Service) RemovePassword(ctx context.Context, acctID uuid.UUID, pwID string) error {
	a, err := s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error {
		kept := a.Passwords[:0]
		found := false
		for _, p := range a.Passwords {
			if p.ID == pwID {
				found = true
				continue
			}
			kept = append(kept, p)
		}
		if !found {
			return identity.ErrNotFound
		}
		a.Passwords = kept
		return nil
	})
	if err != nil {
		return err
	}
	s.cache.invalidateAccount(acctID)
	s.audit.Log(ctx, auditor.Event{Mailbox: a.MailboxID, Action: "password-remove", Target: pwID, Subject: subjectIfOther(ctx, a.Username)})
	return nil
}
