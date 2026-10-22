package auth

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	cryptoauth "dovetail/auth"
	"dovetail/rados"
	"dovetail/service/auditor"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/session"
)

type fixture struct {
	svc  *Service
	ids  *identity.Store
	aud  *audit.Store
	now  time.Time
	acct identity.Account
	mb   identity.Mailbox
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idx, _ := c.Pool("index")
	f := &fixture{now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	now := func() time.Time { return f.now }
	f.ids = identity.New(meta)
	f.ids.Now = now
	sess := session.New(idx)
	sess.Now = now
	f.aud = audit.New(idx)
	f.aud.Now = now
	aud := auditor.New(f.aud, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	aud.Now = now
	f.svc = New(f.ids, sess, aud, Config{AccessTokenLifetime: 15 * time.Minute, RefreshTokenLifetime: time.Hour, Issuer: "test"})
	f.svc.Now = now
	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	h, _ := cryptoauth.HashPassword("correct horse")
	apph, _ := cryptoauth.HashPassword("app-pass-1234")
	f.acct, _ = f.ids.CreateAccount(ctx, identity.Account{
		Username: "wido@example.org", MailboxID: f.mb.ID, Enabled: true,
		Roles:     []string{identity.RoleRead, identity.RoleWrite, identity.RoleSend},
		Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordPrimary}, {Hash: apph, Kind: identity.PasswordApplication, Comment: "phone"}},
	})
	return f
}

func (f *fixture) auditActions(t *testing.T, mb uuid.UUID) []string {
	t.Helper()
	recs, _, err := f.aud.Read(context.Background(), audit.Query{Mailbox: mb, From: f.now.Add(-24 * time.Hour), To: f.now.Add(24 * time.Hour), Max: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range recs {
		out = append(out, r.Action+":"+r.Result)
	}
	return out
}

func TestLoginRefreshRevoke(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "nobody@example.org", Password: "x"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "wrong"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("wrong: %v", err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse", Scope: []string{"admin"}}); !errors.Is(err, ErrScope) {
		t.Fatalf("scope: %v", err)
	}
	toks, err := f.svc.Login(ctx, LoginRequest{Username: "Wido@Example.org", Password: "correct horse", Scope: []string{"read"}, Client: "phone", ClientIP: "2001:db8::1"})
	if err != nil || toks.TokenType != "Bearer" || toks.ExpiresIn != 900 || len(toks.Scope) != 1 {
		t.Fatalf("login: %+v %v", toks, err)
	}
	a, err := f.svc.VerifyAccessToken(ctx, toks.AccessToken)
	if err != nil || a.AccountID != f.acct.ID || a.MailboxID != f.mb.ID || !a.HasRole("read") || a.HasRole("write") || a.SessionID != toks.SessionID {
		t.Fatalf("verify: %+v %v", a, err)
	}
	// Expired access token.
	f.now = f.now.Add(16 * time.Minute)
	if _, err := f.svc.VerifyAccessToken(ctx, toks.AccessToken); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expired: %v", err)
	}
	if _, err := f.svc.VerifyBearer(ctx, "garbage"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("garbage: %v", err)
	}
	// Refresh rotates.
	toks2, err := f.svc.Refresh(ctx, toks.RefreshToken, "2001:db8::2", "")
	if err != nil || toks2.RefreshToken == toks.RefreshToken || toks2.SessionID != toks.SessionID {
		t.Fatalf("refresh: %v", err)
	}
	if _, err := f.svc.VerifyAccessToken(ctx, toks2.AccessToken); err != nil {
		t.Fatal(err)
	}
	// Reuse revokes.
	if _, err := f.svc.Refresh(ctx, toks.RefreshToken, "", ""); !errors.Is(err, ErrSessionRevoked) {
		t.Fatalf("reuse: %v", err)
	}
	if _, err := f.svc.Refresh(ctx, toks2.RefreshToken, "", ""); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("after revoke: %v", err)
	}
	if _, err := f.svc.Refresh(ctx, "dvr1.bad", "", ""); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("malformed: %v", err)
	}
	// Sessions list, revoke by id.
	toks3, _ := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse", Client: "laptop"})
	sessions, _ := f.svc.ListSessions(ctx, f.acct.ID)
	if len(sessions) != 1 || sessions[0].Client != "laptop" {
		t.Fatalf("sessions: %+v", sessions)
	}
	if err := f.svc.Revoke(ctx, toks3.RefreshToken, "", ""); err != nil {
		t.Fatal(err)
	}
	if sessions, _ := f.svc.ListSessions(ctx, f.acct.ID); len(sessions) != 0 {
		t.Fatal("not revoked")
	}
	// Disabled account: login fails, refresh fails.
	toks4, _ := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"})
	_, _ = f.ids.UpdateAccount(ctx, f.acct.ID, func(a *identity.Account) error { a.Enabled = false; return nil })
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"}); !errors.Is(err, ErrAccountDisabled) {
		t.Fatalf("disabled login: %v", err)
	}
	if _, err := f.svc.Refresh(ctx, toks4.RefreshToken, "", ""); !errors.Is(err, ErrAccountDisabled) {
		t.Fatalf("disabled refresh: %v", err)
	}
	// Audit trail exists on the mailbox.
	acts := f.auditActions(t, f.mb.ID)
	if len(acts) < 5 || acts[0] != "login:ok" {
		t.Fatalf("audit: %v", acts)
	}
	// Failed logins are in the admin log (no mailbox).
	adm, _, _ := f.aud.Read(context.Background(), audit.Query{From: f.now.Add(-24 * time.Hour), To: f.now, Action: "login-failed", Max: 100})
	if len(adm) < 3 {
		t.Fatalf("failed logins not audited: %d", len(adm))
	}
}

func TestTOTPFlow(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	enr, err := f.svc.BeginTOTP(ctx, f.acct.ID)
	if err != nil || enr.Secret == "" || len(enr.RecoveryCodes) != 10 || enr.URI[:10] != "otpauth://" {
		t.Fatalf("begin: %+v %v", enr, err)
	}
	// Not confirmed yet: login works without a code.
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"}); err != nil {
		t.Fatalf("login before confirm: %v", err)
	}
	if err := f.svc.ConfirmTOTP(ctx, f.acct.ID, "000000"); !errors.Is(err, ErrTOTPInvalid) {
		t.Fatalf("confirm bad: %v", err)
	}
	code := totpCode(t, enr.Secret, f.now)
	if err := f.svc.ConfirmTOTP(ctx, f.acct.ID, code); err != nil {
		t.Fatal(err)
	}
	// Now the primary password needs a code.
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"}); !errors.Is(err, ErrTOTPRequired) {
		t.Fatalf("totp required: %v", err)
	}
	// Same code again is a replay (same step as confirm).
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse", TOTPCode: code}); !errors.Is(err, ErrTOTPInvalid) {
		t.Fatalf("replay: %v", err)
	}
	f.now = f.now.Add(31 * time.Second)
	code2 := totpCode(t, enr.Secret, f.now)
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse", TOTPCode: code2}); err != nil {
		t.Fatalf("login with code: %v", err)
	}
	// Recovery code works once.
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse", TOTPCode: enr.RecoveryCodes[3]}); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse", TOTPCode: enr.RecoveryCodes[3]}); !errors.Is(err, ErrTOTPInvalid) {
		t.Fatalf("recovery reuse: %v", err)
	}
	// Application password never needs a code.
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "app-pass-1234"}); err != nil {
		t.Fatalf("app password: %v", err)
	}
	// IMAP-style password check: primary refused, app password ok.
	if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "correct horse", "imap", "::1"); !errors.Is(err, ErrTOTPRequired) {
		t.Fatalf("imap primary: %v", err)
	}
	a, err := f.svc.VerifyPassword(ctx, "wido@example.org", "app-pass-1234", "imap", "::1")
	if err != nil || a.Credential != "app-password" || a.Protocol != "imap" || len(a.Roles) != 3 {
		t.Fatalf("imap app: %+v %v", a, err)
	}
	if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "nope", "imap", "::1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatal("imap wrong password")
	}
	// Disable and login plain again.
	if err := f.svc.DisableTOTP(ctx, f.acct.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"}); err != nil {
		t.Fatalf("after disable: %v", err)
	}
}

func totpCode(t *testing.T, secret string, at time.Time) string {
	t.Helper()
	return generateTOTP(t, secret, at)
}

func TestAPITokens(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	exp := f.now.Add(24 * time.Hour)
	tok, secret, err := f.ids.CreateToken(ctx, f.acct.ID, []string{"read"}, exp, "ci")
	if err != nil {
		t.Fatal(err)
	}
	a, err := f.svc.VerifyBearer(ctx, secret)
	if err != nil || a.TokenID != tok.ID || a.TokenNote != "ci" || a.Credential != "token" || !a.HasRole("read") || a.HasRole("write") {
		t.Fatalf("token actor: %+v %v", a, err)
	}
	if _, err := f.svc.VerifyBearer(ctx, "dvt_unknown"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("unknown token: %v", err)
	}
	// Roles narrowed on the account later -> token invalid.
	_, _ = f.ids.UpdateAccount(ctx, f.acct.ID, func(a *identity.Account) error { a.Roles = []string{"send"}; return nil })
	if _, err := f.svc.VerifyBearer(ctx, secret); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("narrowed roles: %v", err)
	}
	_, _ = f.ids.UpdateAccount(ctx, f.acct.ID, func(a *identity.Account) error { a.Roles = []string{"read"}; return nil })
	f.now = exp.Add(time.Second)
	if _, err := f.svc.VerifyBearer(ctx, secret); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expired token: %v", err)
	}
	// OAUTHBEARER on IMAP with username mismatch.
	f.now = exp.Add(-time.Hour)
	if _, err := f.svc.VerifyOAuthBearer(ctx, "other@example.org", secret, "imap", "::1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("mismatch: %v", err)
	}
	a, err = f.svc.VerifyOAuthBearer(ctx, "WIDO@example.org", secret, "imap", "::1")
	if err != nil || a.Protocol != "imap" {
		t.Fatalf("oauthbearer: %+v %v", a, err)
	}
}

func TestKeyRotationKeepsOldTokensValid(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	toks, _ := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"})
	if err := f.svc.RotateKeys(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.VerifyAccessToken(ctx, toks.AccessToken); err != nil {
		t.Fatalf("old token after rotation: %v", err)
	}
	toks2, _ := f.svc.Refresh(ctx, toks.RefreshToken, "", "")
	if _, err := f.svc.VerifyAccessToken(ctx, toks2.AccessToken); err != nil {
		t.Fatal(err)
	}
	// Two rotations later the first key is gone.
	_ = f.svc.RotateKeys(ctx)
	if _, err := f.svc.VerifyAccessToken(ctx, toks.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("very old token: %v", err)
	}
}

func TestPasswordManagement(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.svc.SetPassword(ctx, f.acct.ID, "short", "", "x", nil); !errors.Is(err, identity.ErrInvalid) {
		t.Fatal("short accepted")
	}
	if _, err := f.svc.SetPassword(ctx, f.acct.ID, "new primary pw", identity.PasswordPrimary, "", nil); !errors.Is(err, identity.ErrInvalid) {
		t.Fatal("comment must be required")
	}
	past := f.now.Add(-time.Hour)
	if _, err := f.svc.SetPassword(ctx, f.acct.ID, "temporary pw!", identity.PasswordPrimary, "temp", &past); !errors.Is(err, identity.ErrInvalid) {
		t.Fatal("expiry in the past accepted")
	}
	if _, err := f.svc.SetPassword(ctx, f.acct.ID, "new primary pw", identity.PasswordPrimary, "rotation", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "correct horse"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatal("old primary still valid")
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "new primary pw"}); err != nil {
		t.Fatal(err)
	}
	id, err := f.svc.SetPassword(ctx, f.acct.ID, "second-app-pw", identity.PasswordApplication, "tablet", nil)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := f.ids.GetAccount(ctx, f.acct.ID)
	if len(v.Value.Passwords) != 3 {
		t.Fatalf("passwords: %d", len(v.Value.Passwords))
	}
	// A temporary primary password is added next to the permanent one and
	// stops working at its expiry; the permanent one keeps working.
	exp := f.now.Add(time.Hour)
	tmpID, err := f.svc.SetPassword(ctx, f.acct.ID, "temporary pw!", identity.PasswordPrimary, "helpdesk reset", &exp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "temporary pw!"}); err != nil {
		t.Fatalf("temporary password: %v", err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "new primary pw"}); err != nil {
		t.Fatalf("permanent password alongside temporary: %v", err)
	}
	f.now = exp.Add(time.Minute)
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "temporary pw!"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expired temporary password still works: %v", err)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "new primary pw"}); err != nil {
		t.Fatal("permanent password broken by expiry")
	}
	_ = tmpID
	if err := f.svc.RemovePassword(ctx, f.acct.ID, id); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.RemovePassword(ctx, f.acct.ID, id); !errors.Is(err, identity.ErrNotFound) {
		t.Fatal("double remove")
	}
}

func TestLegacyHashUpgradedOnLogin(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_, _ = f.ids.UpdateAccount(ctx, f.acct.ID, func(a *identity.Account) error {
		a.Passwords = []identity.Password{{Hash: "{PLAIN}legacy-pass", Kind: identity.PasswordPrimary}}
		return nil
	})
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "legacy-pass"}); err != nil {
		t.Fatal(err)
	}
	v, _ := f.ids.GetAccount(ctx, f.acct.ID)
	if cryptoauth.Scheme(v.Value.Passwords[0].Hash) != "ARGON2ID" {
		t.Fatalf("not upgraded: %s", v.Value.Passwords[0].Hash)
	}
	if _, err := f.svc.Login(ctx, LoginRequest{Username: "wido@example.org", Password: "legacy-pass"}); err != nil {
		t.Fatal("upgraded hash does not verify")
	}
}

func TestVerificationCache(t *testing.T) {
	f := newFixture(t)
	f.svc.cfg.CacheTTL = 5 * time.Second
	ctx := context.Background()

	// Password: first call verifies (and audits a login), repeats within the
	// TTL do not touch the store or the audit log.
	if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "correct horse", "imap", "198.51.100.7"); err != nil {
		t.Fatal(err)
	}
	a, err := f.svc.VerifyPassword(ctx, "WIDO@example.org ", "correct horse", "pop3", "198.51.100.8")
	if err != nil {
		t.Fatal(err)
	}
	if a.AccountID != f.acct.ID || a.Protocol != "pop3" || a.ClientIP != "198.51.100.8" {
		t.Fatalf("cached actor wrong: %+v", a)
	}
	if n := countOf(f.auditActions(t, f.mb.ID), "login:ok"); n != 1 {
		t.Fatalf("want 1 login audit record, got %d", n)
	}
	// A wrong password is never served from the cache.
	if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "wrong", "imap", ""); err == nil {
		t.Fatal("wrong password accepted")
	}
	// Changing the password drops this process's cached entry.
	if _, err := f.svc.SetPassword(ctx, f.acct.ID, "new secret", identity.PasswordPrimary, "rotated", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "correct horse", "imap", ""); err == nil {
		t.Fatal("old password still accepted after SetPassword")
	}
	if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "new secret", "imap", ""); err != nil {
		t.Fatal(err)
	}
	if n := countOf(f.auditActions(t, f.mb.ID), "login:ok"); n != 2 {
		t.Fatalf("want 2 login audit records, got %d", n)
	}

	// API token: cached, expires with the TTL, and a revoked token stops
	// working at the latest after the TTL.
	trec, tok, err := f.ids.CreateToken(ctx, f.acct.ID, []string{identity.RoleRead}, f.now.Add(24*time.Hour), "cli")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.VerifyAPIToken(ctx, tok); err != nil {
		t.Fatal(err)
	}
	if err := f.ids.DeleteToken(ctx, f.acct.ID, trec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.VerifyAPIToken(ctx, tok); err != nil {
		t.Fatal("revoked token should still be served from the cache within the TTL")
	}
	f.now = f.now.Add(5 * time.Second)
	if _, err := f.svc.VerifyAPIToken(ctx, tok); err == nil {
		t.Fatal("revoked token accepted after the TTL")
	}

	// TTL 0 disables the cache entirely.
	f.svc.cfg.CacheTTL = 0
	f.svc.InvalidateCache()
	for i := 0; i < 2; i++ {
		if _, err := f.svc.VerifyPassword(ctx, "wido@example.org", "new secret", "imap", ""); err != nil {
			t.Fatal(err)
		}
	}
	if n := countOf(f.auditActions(t, f.mb.ID), "login:ok"); n != 4 {
		t.Fatalf("want 4 login audit records with cache off, got %d", n)
	}
}

func countOf(ss []string, want string) int {
	n := 0
	for _, s := range ss {
		if s == want {
			n++
		}
	}
	return n
}
