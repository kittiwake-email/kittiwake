// Package auth is the authentication service: password logins with TOTP,
// OAuth-style sessions (JWT access tokens + rotating refresh tokens),
// long-lived API tokens, and the per-protocol credential rules. Every
// protocol adapter authenticates through this package.
package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	cryptoauth "kittiwake/auth"
	"kittiwake/metrics"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/session"
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
	ErrServiceDisabled    = errors.New("auth: service not enabled for this account")
	// ErrNamespaceDisabled: the account's namespace is disabled; every
	// account in it is locked out.
	ErrNamespaceDisabled = errors.New("auth: namespace disabled")
	// ErrNotServed: this process does not serve the account's namespace
	// (its config restricts the namespaces); the client must use a server
	// that does.
	ErrNotServed = errors.New("auth: mailbox not served by this server")
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
	// ReadOnly: the service runs in a proxy that never writes (README
	// "Proxies"): no hash upgrades, no sessions; the audit sink should
	// be log-only.
	ReadOnly bool
	// HandoffKey signs handoff tokens for backends after a local
	// verification (proxy.auth local); nil in backends.
	HandoffKey ed25519.PrivateKey
}

// Service is the auth service.
type Service struct {
	ids      *identity.Store
	sessions session.Store
	audit    *auditor.Auditor
	cfg      Config
	Now      func() time.Time
	// Serves reports whether this process serves a namespace; nil means
	// all of them. A login into an unserved namespace fails with
	// ErrNotServed.
	Serves func(namespace string) bool

	keyMu     sync.Mutex
	keys      session.KeySet
	keysAt    time.Time
	keysValid bool

	proxyMu    sync.Mutex
	proxyKeys  map[string]identity.ProxyKey // by id
	proxyAt    time.Time
	proxyValid bool

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
	v  Verified
	at time.Time
}

// Verified is a successful password verification: the actor, and what
// a proxy needs to hand the client over (which password matched, and
// whether its hash is due for an upgrade, which only a backend can do).
type Verified struct {
	Actor      actor.Actor
	PasswordID string
	Upgrade    bool
}

const maxCredEntries = 20000

func (c *credCache) get(key string, now time.Time, ttl time.Duration) (Verified, bool) {
	if ttl <= 0 {
		return Verified{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || now.Sub(e.at) >= ttl || (!e.v.Actor.ExpiresAt.IsZero() && !now.Before(e.v.Actor.ExpiresAt)) {
		return Verified{}, false
	}
	v := e.v
	v.Actor.Roles = append([]string(nil), e.v.Actor.Roles...)
	return v, true
}

func (c *credCache) put(key string, v Verified, now time.Time, ttl time.Duration) {
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
	c.entries[key] = credEntry{v: v, at: now}
	if v.Actor.AccountID != uuid.Nil {
		c.byAcct[v.Actor.AccountID] = append(c.byAcct[v.Actor.AccountID], key)
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
		cfg.Issuer = "kittiwake"
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
// checkNamespace refuses accounts of a disabled namespace and of
// namespaces this process does not serve.
func (s *Service) checkNamespace(ctx context.Context, acct *identity.Account) error {
	ns, err := s.ids.GetNamespaceCached(ctx, acct.Namespace)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return ErrNamespaceDisabled
		}
		return err
	}
	if !ns.Enabled {
		return ErrNamespaceDisabled
	}
	if s.Serves != nil && !s.Serves(ns.Name) {
		return ErrNotServed
	}
	return nil
}

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
	if err := s.checkNamespace(ctx, &acct); err != nil {
		return fail(err.Error(), err)
	}
	if !serviceAllowed(&acct, proto) {
		return fail("service "+identity.ServiceFor(proto)+" not enabled", ErrServiceDisabled)
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

// serviceAllowed reports whether the account may use the protocol's
// service (see identity.ServiceFor). Protocols without a service map
// (LMTP, admin) are always allowed.
func serviceAllowed(acct *identity.Account, proto string) bool {
	svc := identity.ServiceFor(proto)
	return svc == "" || acct.HasService(svc)
}

// dummyHash is an argon2id hash of a random string, used to equalise timing.
var dummyHash = func() string {
	h, _ := cryptoauth.HashPassword("kittiwake-dummy-" + uuid.NewString())
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
	if cryptoauth.VerifyRecoveryCode(acct.TOTP.Recovery, code) >= 0 {
		_, err := s.ids.UpdateAccount(ctx, acct.ID, func(a *identity.Account) error {
			if a.TOTP == nil {
				return ErrTOTPInvalid
			}
			i := cryptoauth.VerifyRecoveryCode(a.TOTP.Recovery, code)
			if i < 0 {
				return ErrTOTPInvalid
			}
			a.TOTP.Recovery = append(a.TOTP.Recovery[:i:i], a.TOTP.Recovery[i+1:]...)
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

// refresh token wire format: "kwr1.<session-id>.<account-id>.<secret>"
func encodeRefresh(sess session.Session, secret string) string {
	return "kwr1." + sess.ID + "." + sess.AccountID.String() + "." + secret
}

func decodeRefresh(tok string) (sessionID string, acct uuid.UUID, secret string, err error) {
	parts := strings.SplitN(tok, ".", 4)
	if len(parts) != 4 || parts[0] != "kwr1" {
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
	if err := s.checkNamespace(ctx, &acct); err != nil {
		s.audit.Log(auditCtx, auditor.Event{Mailbox: acct.MailboxID, Action: "refresh", Detail: err.Error(), Result: audit.ResultDenied})
		return Tokens{}, err
	}
	if !serviceAllowed(&acct, proto) {
		s.audit.Log(auditCtx, auditor.Event{Mailbox: acct.MailboxID, Action: "refresh", Detail: "service " + identity.ServiceFor(proto) + " not enabled", Result: audit.ResultDenied})
		return Tokens{}, ErrServiceDisabled
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
		Credential: "session", SessionID: c.SessionID, ExpiresAt: c.ExpiresAt,
	}, nil
}

// VerifyAPIToken validates a long-lived API token and returns the actor.
// Successful verifications are cached for Config.CacheTTL.
func (s *Service) VerifyAPIToken(ctx context.Context, token string) (actor.Actor, error) {
	if v, ok := s.cache.get(tokenKey(token), s.Now(), s.cfg.CacheTTL); ok {
		return v.Actor, nil
	}
	a, err := s.verifyAPIToken(ctx, token)
	if err == nil {
		s.cache.put(tokenKey(token), Verified{Actor: a}, s.Now(), s.cfg.CacheTTL)
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
			Username: "admin:" + t.Comment, Roles: append([]string(nil), t.Roles...), Namespace: t.Namespace,
			Credential: "admin-token", TokenID: t.ID, TokenNote: t.Comment, ExpiresAt: t.ExpiresAt,
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
	if err := s.checkNamespace(ctx, &acct); err != nil {
		return actor.Actor{}, err
	}
	if !acct.HasService(identity.ServiceJMAP) { // account tokens are only accepted on the JMAP listener
		return actor.Actor{}, ErrServiceDisabled
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
	a.ExpiresAt = t.ExpiresAt
	return a, nil
}

// VerifyBearer dispatches on the token form: API token or JWT.
func (s *Service) VerifyBearer(ctx context.Context, token string) (actor.Actor, error) {
	token = strings.TrimSpace(token)
	if strings.HasPrefix(token, "kwt_") {
		return s.VerifyAPIToken(ctx, token)
	}
	return s.VerifyAccessToken(ctx, token)
}

// VerifyPassword checks username/password for protocols that cannot carry
// a TOTP code (IMAP, POP3, HTTP basic). A primary password of a
// TOTP-enabled account is refused with ErrTOTPRequired; application
// passwords always work.
func (s *Service) VerifyPassword(ctx context.Context, username, password, proto, clientIP string) (actor.Actor, error) {
	v, err := s.CheckPassword(ctx, username, password, proto, clientIP)
	return v.Actor, err
}

// CheckPassword is VerifyPassword with the details a proxy needs to hand
// the client over (Verified). In a ReadOnly service a hash due for an
// upgrade is reported, not rewritten.
func (s *Service) CheckPassword(ctx context.Context, username, password, proto, clientIP string) (Verified, error) {
	key := identity.ServiceFor(proto) + ":" + passwordKey(strings.ToLower(strings.TrimSpace(username)), password)
	if v, ok := s.cache.get(key, s.Now(), s.cfg.CacheTTL); ok {
		v.Actor.Protocol, v.Actor.ClientIP = proto, clientIP
		return v, nil
	}
	v, err := s.verifyPassword(ctx, username, password, proto, clientIP)
	if err == nil {
		s.cache.put(key, v, s.Now(), s.cfg.CacheTTL)
	}
	return v, err
}

func (s *Service) verifyPassword(ctx context.Context, username, password, proto, clientIP string) (Verified, error) {
	auditCtx := actor.With(ctx, actor.Actor{Username: strings.ToLower(strings.TrimSpace(username)), Protocol: proto, ClientIP: clientIP, Credential: "password"})
	fail := func(detail string, err error) (Verified, error) {
		metrics.Auth.WithLabelValues(proto, "failed").Inc()
		s.audit.Log(auditCtx, auditor.Event{Action: "login-failed", Detail: detail, Result: audit.ResultDenied})
		return Verified{}, err
	}
	acct, err := s.ids.GetAccountByUsernameCached(ctx, username)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) || errors.Is(err, identity.ErrInvalid) {
			_, _ = cryptoauth.VerifyPassword(dummyHash, password)
			return fail("unknown account", ErrInvalidCredentials)
		}
		return Verified{}, err
	}
	pw, ok := s.matchPassword(&acct, password)
	if !ok {
		return fail("wrong password", ErrInvalidCredentials)
	}
	if !acct.Enabled {
		return fail("account disabled", ErrAccountDisabled)
	}
	if err := s.checkNamespace(ctx, &acct); err != nil {
		return fail(err.Error(), err)
	}
	if !serviceAllowed(&acct, proto) {
		return fail("service "+identity.ServiceFor(proto)+" not enabled", ErrServiceDisabled)
	}
	if totpRequired(&acct, pw) {
		return fail("totp required; use an application password", ErrTOTPRequired)
	}
	cred := "password"
	if pw.Kind == identity.PasswordApplication {
		cred = "app-password"
	}
	a := s.actorFor(&acct, acct.Roles, cred, "", "", proto, clientIP)
	if pw.ExpiresAt != nil {
		a.ExpiresAt = *pw.ExpiresAt
	}
	metrics.Auth.WithLabelValues(proto, "ok").Inc()
	s.audit.Log(actor.With(ctx, a), auditor.Event{Mailbox: acct.MailboxID, Action: "login"})
	v := Verified{Actor: a, PasswordID: pw.ID, Upgrade: cryptoauth.NeedsRehash(pw.Hash)}
	if v.Upgrade && !s.cfg.ReadOnly {
		s.rehash(ctx, acct.ID, pw.ID, password)
	}
	return v, nil
}

// ---- proxy handoff ----

// Handoff signs a handoff token for a verified login, for the proxy to
// present to a backend in place of the password (XOAUTH2 on IMAP, POP3
// and submission, Bearer on HTTP).
func (s *Service) Handoff(v Verified, proto, clientIP string) (string, error) {
	if s.cfg.HandoffKey == nil {
		return "", errors.New("auth: no handoff key configured")
	}
	now := s.Now().UTC()
	return cryptoauth.SignHandoff(s.cfg.HandoffKey, cryptoauth.Handoff{
		AccountID: v.Actor.AccountID, Username: v.Actor.Username, PasswordID: v.PasswordID, Credential: v.Actor.Credential,
		Service: handoffService(proto), ClientIP: clientIP, IssuedAt: now, ExpiresAt: now.Add(cryptoauth.HandoffLifetime),
	})
}

// handoffService is what a handoff token is bound to: the protocol, with
// the HTTP front ends (JMAP and the REST account endpoints, one
// listener) as one.
func handoffService(proto string) string {
	switch proto {
	case "jmap", "rest":
		return "http"
	}
	return proto
}

// IsHandoff reports whether a token is a proxy handoff token.
func IsHandoff(token string) bool { return strings.HasPrefix(token, cryptoauth.HandoffPrefix) }

// VerifyHandoff accepts a handoff token from a proxy site whose key is
// registered: the proxy verified the password, this process checks the
// token's signature, age, binding to the client address and service,
// and the account's current state, then writes the login audit record
// as it would for the password itself.
func (s *Service) VerifyHandoff(ctx context.Context, token, proto, clientIP string) (actor.Actor, error) {
	auditCtx := actor.With(ctx, actor.Actor{Protocol: proto, ClientIP: clientIP, Credential: "password"})
	fail := func(detail string, err error) (actor.Actor, error) {
		metrics.Auth.WithLabelValues(proto, "failed").Inc()
		s.audit.Log(auditCtx, auditor.Event{Action: "login-failed", Detail: "handoff: " + detail, Result: audit.ResultDenied})
		return actor.Actor{}, err
	}
	var key identity.ProxyKey
	keyFor := func(kid string) (ed25519.PublicKey, bool) {
		k, ok := s.proxyKey(ctx, kid)
		if ok {
			key = k
		}
		return ed25519.PublicKey(k.PublicKey), ok
	}
	h, err := cryptoauth.VerifyHandoff(token, keyFor, s.Now())
	if err != nil {
		if errors.Is(err, cryptoauth.ErrTokenExpired) {
			return fail("expired", ErrTokenExpired)
		}
		return fail(err.Error(), ErrInvalidToken)
	}
	if h.Service != handoffService(proto) {
		return fail("service mismatch", ErrInvalidToken)
	}
	if subtle.ConstantTimeCompare([]byte(h.ClientIP), []byte(clientIP)) != 1 {
		return fail("client address mismatch", ErrInvalidToken)
	}
	v, err := s.ids.GetAccount(ctx, h.AccountID)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return fail("unknown account", ErrInvalidCredentials)
		}
		return actor.Actor{}, err
	}
	acct := v.Value
	if !strings.EqualFold(acct.Username, h.Username) {
		return fail("username mismatch", ErrInvalidCredentials)
	}
	var pw *identity.Password
	for i := range acct.Passwords {
		if acct.Passwords[i].ID == h.PasswordID && !acct.Passwords[i].Expired(s.Now()) {
			pw = &acct.Passwords[i]
		}
	}
	if pw == nil {
		return fail("password gone", ErrInvalidCredentials)
	}
	if !acct.Enabled {
		return fail("account disabled", ErrAccountDisabled)
	}
	if err := s.checkNamespace(ctx, &acct); err != nil {
		return fail(err.Error(), err)
	}
	if !serviceAllowed(&acct, proto) {
		return fail("service "+identity.ServiceFor(proto)+" not enabled", ErrServiceDisabled)
	}
	if totpRequired(&acct, pw) {
		return fail("totp required", ErrTOTPRequired)
	}
	cred := "password"
	if pw.Kind == identity.PasswordApplication {
		cred = "app-password"
	}
	a := s.actorFor(&acct, acct.Roles, cred, "", "", proto, clientIP)
	if pw.ExpiresAt != nil {
		a.ExpiresAt = *pw.ExpiresAt
	}
	metrics.Auth.WithLabelValues(proto, "ok").Inc()
	s.audit.Log(actor.With(ctx, a), auditor.Event{Mailbox: acct.MailboxID, Action: "login", Detail: "proxy=" + key.Name})
	return a, nil
}

// proxyKey returns a registered proxy key by id, from a listing cached
// for KeyCacheTTL; an unknown id reloads the listing once (a key
// registered since).
func (s *Service) proxyKey(ctx context.Context, id string) (identity.ProxyKey, bool) {
	s.proxyMu.Lock()
	defer s.proxyMu.Unlock()
	for attempt := 0; attempt < 2; attempt++ {
		if !s.proxyValid || s.Now().Sub(s.proxyAt) >= s.cfg.KeyCacheTTL {
			list, err := s.ids.ListProxyKeys(ctx)
			if err != nil {
				return identity.ProxyKey{}, false
			}
			s.proxyKeys = make(map[string]identity.ProxyKey, len(list))
			for _, k := range list {
				s.proxyKeys[k.ID] = k
			}
			s.proxyAt, s.proxyValid = s.Now(), true
		}
		if k, ok := s.proxyKeys[id]; ok {
			return k, true
		}
		s.proxyValid = false
	}
	return identity.ProxyKey{}, false
}

// InvalidateProxyKeys forces the next handoff to reload the proxy keys
// (after a registration or removal in this process).
func (s *Service) InvalidateProxyKeys() {
	s.proxyMu.Lock()
	s.proxyValid = false
	s.proxyMu.Unlock()
}

// VerifyOAuthBearer authenticates IMAP/POP3 OAUTHBEARER/XOAUTH2: the token
// must be a session access token (or API token) whose account matches the
// presented username (if any).
func (s *Service) VerifyOAuthBearer(ctx context.Context, username, token, proto, clientIP string) (actor.Actor, error) {
	if IsHandoff(token) {
		a, err := s.VerifyHandoff(ctx, token, proto, clientIP)
		if err == nil && username != "" && !strings.EqualFold(username, a.Username) {
			return actor.Actor{}, ErrInvalidCredentials
		}
		return a, err
	}
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
	acct, err := s.ids.GetAccountByUsernameCached(ctx, a.Username)
	if err != nil || acct.ID != a.AccountID {
		return actor.Actor{}, ErrInvalidCredentials
	}
	if !acct.Enabled {
		return actor.Actor{}, ErrAccountDisabled
	}
	if err := s.checkNamespace(ctx, &acct); err != nil {
		return actor.Actor{}, err
	}
	if !serviceAllowed(&acct, proto) {
		return actor.Actor{}, ErrServiceDisabled
	}
	a.Protocol = proto
	if err := actor.CheckAccount(ctx, s.ids, a, "", s.Now()); err != nil {
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

// SetServices sets which services the account may log in to (self-service
// or admin). Sessions already issued keep working until they expire or
// refresh; other processes see the change within the cache TTL.
func (s *Service) SetServices(ctx context.Context, acctID uuid.UUID, services []string) error {
	if services == nil {
		services = []string{}
	}
	a, err := s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error { a.Services = services; return nil })
	if err != nil {
		return err
	}
	s.cache.invalidateAccount(acctID)
	s.audit.Log(ctx, auditor.Event{Mailbox: a.MailboxID, Action: "services-change", Detail: strings.Join(services, ","), Subject: subjectIfOther(ctx, a.Username)})
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
	return s.storePassword(ctx, acctID, h, kind, comment, expiresAt, "")
}

// SetPasswordHash stores an existing hash as the account's primary password,
// as migrations from other platforms need: the hash is taken in the form
// it has in the old database (Dovecot scheme prefix, or a bare crypt-style
// hash whose scheme is inferred), validated for shape and never verified.
// The first successful login upgrades it to argon2id like any legacy hash.
func (s *Service) SetPasswordHash(ctx context.Context, acctID uuid.UUID, hash, kind, comment string, expiresAt *time.Time) (string, error) {
	h, err := cryptoauth.NormalizeHash(hash)
	if err != nil {
		return "", fmt.Errorf("%w: %v", identity.ErrInvalid, err)
	}
	if kind == "" {
		kind = identity.PasswordPrimary
	}
	if kind != identity.PasswordPrimary && kind != identity.PasswordApplication {
		return "", fmt.Errorf("%w: password kind", identity.ErrInvalid)
	}
	comment = strings.TrimSpace(comment)
	if comment == "" {
		comment = "migrated hash"
	}
	if expiresAt != nil && !expiresAt.After(s.Now()) {
		return "", fmt.Errorf("%w: password expiry must be in the future", identity.ErrInvalid)
	}
	return s.storePassword(ctx, acctID, h, kind, comment, expiresAt, "hash")
}

// storePassword adds the hash as a password of the account; a permanent
// primary replaces the existing permanent primary. source is "" for a
// password hashed here and "hash" for one taken over from elsewhere.
func (s *Service) storePassword(ctx context.Context, acctID uuid.UUID, h, kind, comment string, expiresAt *time.Time, source string) (string, error) {
	now := s.Now().UTC()
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
	detail := fmt.Sprintf("comment=%q expires=%s", comment, exp)
	if source != "" {
		detail += " source=" + source + " scheme=" + cryptoauth.Scheme(h)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: a.MailboxID, Action: "password-set", Target: kind, Detail: detail, Subject: subjectIfOther(ctx, a.Username)})
	return id, nil
}

// RemovePassword deletes a password by id.
// UpdatePassword changes a password's expiry (nil = never) and, when
// comment is not empty, its comment. The hash is untouched.
func (s *Service) UpdatePassword(ctx context.Context, acctID uuid.UUID, pwID string, expiresAt *time.Time, comment string) (identity.Password, error) {
	if expiresAt != nil && !expiresAt.After(s.Now()) {
		return identity.Password{}, fmt.Errorf("%w: password expiry must be in the future", identity.ErrInvalid)
	}
	var out identity.Password
	a, err := s.ids.UpdateAccount(ctx, acctID, func(a *identity.Account) error {
		for i := range a.Passwords {
			if a.Passwords[i].ID != pwID {
				continue
			}
			a.Passwords[i].ExpiresAt = expiresAt
			if c := strings.TrimSpace(comment); c != "" {
				a.Passwords[i].Comment = c
			}
			out = a.Passwords[i]
			return nil
		}
		return identity.ErrNotFound
	})
	if err != nil {
		return identity.Password{}, err
	}
	s.cache.invalidateAccount(acctID)
	exp := "never"
	if expiresAt != nil {
		exp = expiresAt.UTC().Format(time.RFC3339)
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: a.MailboxID, Action: "password-update", Target: pwID, Detail: "expires=" + exp, Subject: subjectIfOther(ctx, a.Username)})
	return out, nil
}

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

// ValidateActor applies current account policy after the HTTP listener has set
// the protocol, using the identity cache shared with mailbox authorization.
func (s *Service) ValidateActor(ctx context.Context, a actor.Actor) error {
	return actor.CheckAccount(ctx, s.ids, a, "", s.Now())
}
