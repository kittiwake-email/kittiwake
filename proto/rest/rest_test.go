package rest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	cryptoauth "kittiwake/auth"
	"kittiwake/rados"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	"kittiwake/service/auth"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/queue"
	"kittiwake/store/session"
)

type fixture struct {
	t       *testing.T
	srv     *Server
	ts      *httptest.Server
	ids     *identity.Store
	now     time.Time
	mb      identity.Mailbox
	user    identity.Account
	admin   identity.Account
	adminTk string
	roTk    string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	f := &fixture{t: t, now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	now := func() time.Time { return f.now }
	f.ids = identity.New(meta)
	f.ids.Now = now
	sess := session.New(idxp)
	sess.Now = now
	auditStore := audit.New(idxp)
	auditStore.Now = now
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(auditStore, log, nil)
	aud.Now = now
	authSvc := auth.New(f.ids, sess, aud, auth.Config{AccessTokenLifetime: 15 * time.Minute, RefreshTokenLifetime: time.Hour, Issuer: "test"})
	authSvc.Now = now
	q := queue.New(idxp)
	adminSvc := admin.New(f.ids, index.New(idxp), q, aud)
	adminSvc.Now = now
	f.srv = New(Config{Realm: "test", DefaultQuota: identity.QuotaLimits{HardBytes: 1 << 30}, Identity: f.ids}, authSvc, adminSvc, auditStore, q, log)
	f.ts = httptest.NewServer(f.srv.Handler())
	t.Cleanup(f.ts.Close)

	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{})
	h, _ := cryptoauth.HashPassword("correct horse")
	f.user, _ = f.ids.CreateAccount(ctx, identity.Account{
		Username: "wido@example.org", MailboxID: f.mb.ID, Enabled: true,
		Roles: []string{"read", "write", "send"}, Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordPrimary}},
	})
	_, f.adminTk, _ = f.ids.CreateAdminToken(ctx, identity.RoleAdmin, f.now.Add(24*time.Hour), "test admin")
	_, f.roTk, _ = f.ids.CreateAdminToken(ctx, identity.RoleAdminRead, f.now.Add(24*time.Hour), "test read-only")
	return f
}

type resp struct {
	code int
	body map[string]any
	raw  []byte
}

func (f *fixture) do(method, path, auth string, body any, contentType string) resp {
	f.t.Helper()
	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rdr = strings.NewReader(b)
		case []byte:
			rdr = bytes.NewReader(b)
		default:
			j, _ := json.Marshal(b)
			rdr = bytes.NewReader(j)
			if contentType == "" {
				contentType = "application/json"
			}
		}
	}
	req, _ := http.NewRequest(method, f.ts.URL+path, rdr)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	out := resp{code: r.StatusCode, raw: raw}
	if len(raw) > 0 && strings.Contains(r.Header.Get("Content-Type"), "json") {
		_ = json.Unmarshal(raw, &out.body)
	}
	return out
}

func (f *fixture) bearer(tok string) string { return "Bearer " + tok }
func (f *fixture) adminAuth() string        { return "Bearer " + f.adminTk }

func (f *fixture) login(username, password string, extra map[string]string) resp {
	form := url.Values{"grant_type": {"password"}, "username": {username}, "password": {password}, "client_name": {"test"}}
	for k, v := range extra {
		form.Set(k, v)
	}
	return f.do("POST", "/v1/oauth/token", "", form.Encode(), "application/x-www-form-urlencoded")
}

func (f *fixture) expect(r resp, code int, what string) {
	f.t.Helper()
	if r.code != code {
		f.t.Fatalf("%s: got %d want %d: %s", what, r.code, code, r.raw)
	}
}

func totpCode(secret string, at time.Time) string {
	key, _ := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(secret))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(at.Unix()/30))
	mac := hmac.New(sha1.New, key)
	mac.Write(buf[:])
	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & 0x0f
	return fmt.Sprintf("%06d", (binary.BigEndian.Uint32(sum[off:off+4])&0x7fffffff)%1000000)
}

func TestOAuthPasswordAndRefresh(t *testing.T) {
	f := newFixture(t)
	// Form login.
	r := f.login("wido@example.org", "correct horse", map[string]string{"scope": "read write"})
	f.expect(r, 200, "login")
	if r.body["token_type"] != "Bearer" || r.body["scope"] != "read write" || r.body["session_id"] == "" || r.body["expires_in"].(float64) != 900 {
		t.Fatalf("token response: %v", r.body)
	}
	access := r.body["access_token"].(string)
	refresh := r.body["refresh_token"].(string)
	// JSON login.
	r = f.do("POST", "/v1/oauth/token", "", map[string]string{"grant_type": "password", "username": "wido@example.org", "password": "correct horse", "client_id": "json-client"}, "")
	f.expect(r, 200, "json login")
	// Errors.
	r = f.login("wido@example.org", "wrong", nil)
	f.expect(r, 400, "wrong password")
	if r.body["error"] != "invalid_grant" {
		t.Fatalf("error: %v", r.body)
	}
	r = f.login("wido@example.org", "correct horse", map[string]string{"scope": "admin"})
	if r.code != 400 || r.body["error"] != "invalid_scope" {
		t.Fatalf("scope: %d %v", r.code, r.body)
	}
	r = f.do("POST", "/v1/oauth/token", "", "grant_type=client_credentials", "application/x-www-form-urlencoded")
	if r.code != 400 || r.body["error"] != "unsupported_grant_type" {
		t.Fatalf("grant: %d %v", r.code, r.body)
	}
	r = f.do("POST", "/v1/oauth/token", "", "grant_type=password", "application/x-www-form-urlencoded")
	if r.code != 400 || r.body["error"] != "invalid_request" {
		t.Fatalf("missing fields: %d %v", r.code, r.body)
	}
	// Use the access token.
	r = f.do("GET", "/v1/me", f.bearer(access), nil, "")
	f.expect(r, 200, "me")
	if r.body["username"] != "wido@example.org" || r.body["credential"] != "session" || r.body["totp_enabled"] != false {
		t.Fatalf("me: %v", r.body)
	}
	if pw := r.body["passwords"].([]any); len(pw) != 1 || pw[0].(map[string]any)["hash"] != nil {
		t.Fatalf("passwords leak or missing: %v", pw)
	}
	// Refresh rotates; old refresh token reuse → invalid_grant and session dead.
	r = f.do("POST", "/v1/oauth/token", "", map[string]string{"grant_type": "refresh_token", "refresh_token": refresh}, "")
	f.expect(r, 200, "refresh")
	refresh2 := r.body["refresh_token"].(string)
	if refresh2 == refresh {
		t.Fatal("not rotated")
	}
	r = f.do("POST", "/v1/oauth/token", "", map[string]string{"grant_type": "refresh_token", "refresh_token": refresh}, "")
	if r.code != 400 || r.body["error"] != "invalid_grant" {
		t.Fatalf("reuse: %d %v", r.code, r.body)
	}
	r = f.do("POST", "/v1/oauth/token", "", map[string]string{"grant_type": "refresh_token", "refresh_token": refresh2}, "")
	if r.code != 400 {
		t.Fatalf("after revoke by reuse: %d", r.code)
	}
	// Revoke is always 200.
	r = f.login("wido@example.org", "correct horse", nil)
	rt := r.body["refresh_token"].(string)
	f.expect(f.do("POST", "/v1/oauth/revoke", "", "token="+url.QueryEscape(rt), "application/x-www-form-urlencoded"), 200, "revoke")
	f.expect(f.do("POST", "/v1/oauth/revoke", "", map[string]string{"token": "garbage"}, ""), 200, "revoke unknown")
	r = f.do("POST", "/v1/oauth/token", "", map[string]string{"grant_type": "refresh_token", "refresh_token": rt}, "")
	f.expect(r, 400, "refresh after revoke")
	// Expired access token → 401.
	f.now = f.now.Add(time.Hour)
	r = f.do("GET", "/v1/me", f.bearer(access), nil, "")
	f.expect(r, 401, "expired")
	// Unauthenticated → 401 with challenge.
	r = f.do("GET", "/v1/me", "", nil, "")
	f.expect(r, 401, "no auth")
}

func TestSelfService(t *testing.T) {
	f := newFixture(t)
	basic := func(u, p string) string { return "Basic " + basicEnc(u, p) }
	// Basic auth works on self-service.
	r := f.do("GET", "/v1/me", basic("wido@example.org", "correct horse"), nil, "")
	f.expect(r, 200, "basic me")
	if r.body["credential"] != "password" {
		t.Fatalf("credential: %v", r.body["credential"])
	}
	// Sessions.
	l := f.login("wido@example.org", "correct horse", nil)
	access := l.body["access_token"].(string)
	r = f.do("GET", "/v1/me/sessions", f.bearer(access), nil, "")
	f.expect(r, 200, "sessions")
	items := r.body["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["client"] != "test" {
		t.Fatalf("sessions: %v", items)
	}
	sid := items[0].(map[string]any)["id"].(string)
	f.expect(f.do("DELETE", "/v1/me/sessions/"+sid, f.bearer(access), nil, ""), 204, "revoke session")
	r = f.do("GET", "/v1/me/sessions", f.bearer(access), nil, "")
	if len(r.body["items"].([]any)) != 0 {
		t.Fatal("session not revoked")
	}
	// App password: create, then use with basic auth; list shows it.
	r = f.do("POST", "/v1/me/app-passwords", f.bearer(access), map[string]string{"password": "app-password-1", "comment": "phone"}, "")
	f.expect(r, 201, "app password")
	appID := r.body["id"].(string)
	r = f.do("GET", "/v1/me", basic("wido@example.org", "app-password-1"), nil, "")
	f.expect(r, 200, "basic with app password")
	if r.body["credential"] != "app-password" || len(r.body["passwords"].([]any)) != 2 {
		t.Fatalf("app pw me: %v", r.body)
	}
	f.expect(f.do("POST", "/v1/me/app-passwords", f.bearer(access), map[string]string{"password": "short"}, ""), 400, "short app pw")
	f.expect(f.do("POST", "/v1/me/app-passwords", f.bearer(access), map[string]string{"password": "longenough-nocomment"}, ""), 400, "app pw without comment")
	// Primary password change.
	f.expect(f.do("PUT", "/v1/me/password", f.bearer(access), map[string]string{"password": "new primary pw"}, ""), 200, "set password")
	f.expect(f.do("GET", "/v1/me", basic("wido@example.org", "correct horse"), nil, ""), 401, "old primary")
	f.expect(f.do("GET", "/v1/me", basic("wido@example.org", "new primary pw"), nil, ""), 200, "new primary")
	f.expect(f.do("DELETE", "/v1/me/passwords/"+appID, f.bearer(access), nil, ""), 204, "delete app pw")
	f.expect(f.do("DELETE", "/v1/me/passwords/"+appID, f.bearer(access), nil, ""), 404, "delete twice")
	f.expect(f.do("GET", "/v1/me", basic("wido@example.org", "app-password-1"), nil, ""), 401, "deleted app pw")
	// TOTP round trip.
	r = f.do("POST", "/v1/me/totp", f.bearer(access), nil, "")
	f.expect(r, 200, "totp begin")
	secret := r.body["secret"].(string)
	if len(r.body["recovery_codes"].([]any)) != 10 || !strings.HasPrefix(r.body["uri"].(string), "otpauth://") {
		t.Fatalf("enrolment: %v", r.body)
	}
	f.expect(f.do("POST", "/v1/me/totp/confirm", f.bearer(access), map[string]string{"code": "000000"}, ""), 400, "bad code")
	f.expect(f.do("POST", "/v1/me/totp/confirm", f.bearer(access), map[string]string{"code": totpCode(secret, f.now)}, ""), 204, "confirm")
	r = f.do("GET", "/v1/me", f.bearer(access), nil, "")
	if r.body["totp_enabled"] != true {
		t.Fatal("totp not enabled")
	}
	// Now login needs a code, and basic auth with the primary is refused.
	r = f.login("wido@example.org", "new primary pw", nil)
	if r.code != 400 || r.body["error"] != "totp_required" {
		t.Fatalf("totp_required: %d %v", r.code, r.body)
	}
	f.expect(f.do("GET", "/v1/me", basic("wido@example.org", "new primary pw"), nil, ""), 401, "basic with totp primary")
	f.now = f.now.Add(31 * time.Second)
	f.expect(f.login("wido@example.org", "new primary pw", map[string]string{"totp_code": totpCode(secret, f.now)}), 200, "login with code")
	f.expect(f.do("DELETE", "/v1/me/totp", f.bearer(access), nil, ""), 204, "disable totp")
	f.expect(f.login("wido@example.org", "new primary pw", nil), 200, "login after disable")
	// Malformed JSON → 400.
	f.expect(f.do("PUT", "/v1/me/password", f.bearer(access), "{not json", "application/json"), 400, "bad json")
}

func basicEnc(u, p string) string {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	src := []byte(u + ":" + p)
	var out []byte
	for i := 0; i < len(src); i += 3 {
		var n uint32
		pad := 0
		for j := 0; j < 3; j++ {
			n <<= 8
			if i+j < len(src) {
				n |= uint32(src[i+j])
			} else {
				pad++
			}
		}
		for j := 3; j >= 0; j-- {
			if j < pad {
				out = append(out, '=')
			} else {
				out = append(out, tbl[(n>>(6*uint(j)))&63])
			}
		}
	}
	return string(out)
}

func TestAdminAuthz(t *testing.T) {
	f := newFixture(t)
	f.expect(f.do("GET", "/v1/admin/status", "", nil, ""), 401, "no auth")
	l := f.login("wido@example.org", "correct horse", nil)
	f.expect(f.do("GET", "/v1/admin/status", f.bearer(l.body["access_token"].(string)), nil, ""), 403, "non-admin")
	r := f.do("GET", "/v1/admin/status", f.adminAuth(), nil, "")
	f.expect(r, 200, "admin status")
	if r.body["version"] != "dev" {
		t.Fatalf("status: %v", r.body)
	}
	f.expect(f.do("GET", "/v1/admin/status", f.bearer("kwt_nope"), nil, ""), 401, "bad token")
}

func TestAdminCRUD(t *testing.T) {
	f := newFixture(t)
	a := f.adminAuth()
	// Domains.
	r := f.do("POST", "/v1/admin/domains", a, map[string]any{"name": "Test.Example"}, "")
	f.expect(r, 201, "domain create")
	if r.body["name"] != "test.example" || r.body["enabled"] != true {
		t.Fatalf("domain: %v", r.body)
	}
	f.expect(f.do("POST", "/v1/admin/domains", a, map[string]any{"name": "test.example"}, ""), 409, "domain dup")
	f.expect(f.do("POST", "/v1/admin/domains", a, map[string]any{"name": "bad domain"}, ""), 400, "domain invalid")
	f.expect(f.do("POST", "/v1/admin/domains", a, map[string]any{"nme": "x"}, ""), 400, "unknown field")
	r = f.do("GET", "/v1/admin/domains?limit=1", a, nil, "")
	f.expect(r, 200, "domain list")
	if items := r.body["items"].([]any); len(items) != 1 || r.body["more"] != true || r.body["next"] != "example.org" {
		t.Fatalf("list: %v", r.body)
	}
	r = f.do("GET", "/v1/admin/domains?after=example.org", a, nil, "")
	if items := r.body["items"].([]any); len(items) != 1 || items[0] != "test.example" {
		t.Fatalf("list page 2: %v", r.body)
	}
	r = f.do("PATCH", "/v1/admin/domains/test.example", a, map[string]any{"enabled": false, "settings": map[string]string{"k": "v"}}, "")
	f.expect(r, 200, "domain patch")
	if r.body["enabled"] != false || r.body["settings"].(map[string]any)["k"] != "v" {
		t.Fatalf("patch: %v", r.body)
	}
	f.expect(f.do("GET", "/v1/admin/domains/nope.example", a, nil, ""), 404, "domain missing")
	// Mailboxes.
	r = f.do("POST", "/v1/admin/mailboxes", a, map[string]any{"quota": map[string]int{"hard_messages": 10}}, "")
	f.expect(r, 201, "mailbox create")
	mbID := r.body["id"].(string)
	r = f.do("POST", "/v1/admin/mailboxes", a, nil, "")
	f.expect(r, 201, "mailbox create empty body")
	mb2 := r.body["id"].(string)
	r = f.do("GET", "/v1/admin/mailboxes/"+mbID, a, nil, "")
	f.expect(r, 200, "mailbox inspect")
	if r.body["indexed"] != false || r.body["limits"].(map[string]any)["hard_bytes"].(float64) != 1<<30 || r.body["limits"].(map[string]any)["hard_messages"].(float64) != 10 {
		t.Fatalf("inspect: %v", r.body)
	}
	f.expect(f.do("GET", "/v1/admin/mailboxes/not-a-uuid", a, nil, ""), 400, "bad uuid")
	f.expect(f.do("GET", "/v1/admin/mailboxes/"+uuid.NewString(), a, nil, ""), 404, "mailbox missing")
	f.expect(f.do("PATCH", "/v1/admin/mailboxes/"+mbID, a, map[string]any{"state": "suspended"}, ""), 400, "suspend without reason")
	f.expect(f.do("PATCH", "/v1/admin/mailboxes/"+mbID, a, map[string]any{"state": "bogus", "state_reason": "x"}, ""), 400, "bad state")
	r = f.do("PATCH", "/v1/admin/mailboxes/"+mbID, a, map[string]any{"state": "suspended", "state_reason": "unpaid invoice"}, "")
	f.expect(r, 200, "mailbox suspend")
	if r.body["state"] != "suspended" || r.body["state_reason"] != "unpaid invoice" {
		t.Fatalf("patch state: %v", r.body)
	}
	r = f.do("PATCH", "/v1/admin/mailboxes/"+mbID, a, map[string]any{"state": "active"}, "")
	f.expect(r, 200, "mailbox activate")
	if r.body["state"] != "active" {
		t.Fatal("activate")
	}
	r = f.do("POST", "/v1/admin/mailboxes/"+mbID+"/recalculate", a, nil, "")
	f.expect(r, 404, "recalculate unindexed")
	// Accounts.
	r = f.do("POST", "/v1/admin/accounts", a, map[string]any{"username": "Bob@test.example", "mailbox_id": mbID, "roles": []string{"read", "write"}, "password": "bobpassword"}, "")
	f.expect(r, 201, "account create")
	if r.body["username"] != "bob@test.example" || len(r.body["passwords"].([]any)) != 1 || r.body["passwords"].([]any)[0].(map[string]any)["hash"] != nil {
		t.Fatalf("account: %v", r.body)
	}
	acctID := r.body["id"].(string)
	f.expect(f.do("POST", "/v1/admin/accounts", a, map[string]any{"username": "bob@test.example", "mailbox_id": mbID, "roles": []string{"read"}}, ""), 409, "account dup")
	f.expect(f.do("POST", "/v1/admin/accounts", a, map[string]any{"username": "x@nope.example", "mailbox_id": mbID, "roles": []string{"read"}}, ""), 400, "unknown domain")
	f.expect(f.do("POST", "/v1/admin/accounts", a, map[string]any{"username": "y@test.example", "mailbox_id": uuid.NewString(), "roles": []string{"read"}}, ""), 404, "unknown mailbox")
	f.expect(f.do("POST", "/v1/admin/accounts", a, map[string]any{"username": "z@test.example", "mailbox_id": mbID, "roles": []string{"god"}}, ""), 400, "bad role")
	r = f.do("GET", "/v1/admin/accounts?domain=test.example&prefix=bo", a, nil, "")
	f.expect(r, 200, "account list")
	if items := r.body["items"].([]any); len(items) != 1 || items[0] != "bob@test.example" {
		t.Fatalf("list: %v", r.body)
	}
	f.expect(f.do("GET", "/v1/admin/accounts", a, nil, ""), 400, "list without domain")
	f.expect(f.do("GET", "/v1/admin/accounts/bob@test.example", a, nil, ""), 200, "get by username")
	f.expect(f.do("GET", "/v1/admin/accounts/"+acctID, a, nil, ""), 200, "get by id")
	f.expect(f.do("GET", "/v1/admin/accounts/nobody@test.example", a, nil, ""), 404, "get missing")
	r = f.do("PATCH", "/v1/admin/accounts/"+acctID, a, map[string]any{"roles": []string{"read"}, "enabled": false, "mailbox_id": mb2}, "")
	f.expect(r, 200, "account patch")
	if r.body["enabled"] != false || len(r.body["roles"].([]any)) != 1 || r.body["mailbox_id"] != mb2 {
		t.Fatalf("patch: %v", r.body)
	}
	f.expect(f.do("PATCH", "/v1/admin/accounts/"+acctID, a, map[string]any{"username": "new@test.example"}, ""), 400, "rename refused")
	f.expect(f.do("PUT", "/v1/admin/accounts/"+acctID+"/password", a, map[string]any{"password": "application-pw", "kind": "application", "comment": "imap"}, ""), 200, "admin set app password")
	r = f.do("GET", "/v1/admin/accounts/"+acctID, a, nil, "")
	if len(r.body["passwords"].([]any)) != 2 {
		t.Fatalf("passwords: %v", r.body)
	}
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+acctID+"/totp", a, nil, ""), 204, "admin disable totp")
	// Sessions of a user via admin.
	_, _ = f.ids.UpdateAccount(context.Background(), f.user.ID, func(x *identity.Account) error { return nil })
	l := f.login("wido@example.org", "correct horse", nil)
	r = f.do("GET", "/v1/admin/accounts/wido@example.org/sessions", a, nil, "")
	f.expect(r, 200, "admin sessions")
	sid := r.body["items"].([]any)[0].(map[string]any)["id"].(string)
	f.expect(f.do("DELETE", "/v1/admin/accounts/wido@example.org/sessions/"+sid, a, nil, ""), 204, "admin revoke session")
	rr := f.do("POST", "/v1/oauth/token", "", map[string]string{"grant_type": "refresh_token", "refresh_token": l.body["refresh_token"].(string)}, "")
	f.expect(rr, 400, "refresh after admin revoke")
	// Tokens.
	r = f.do("POST", "/v1/admin/accounts/"+acctID+"/tokens", a, map[string]any{"roles": []string{"read"}, "expires_at": f.now.Add(time.Hour), "comment": "ci"}, "")
	f.expect(r, 201, "token create")
	secret := r.body["secret"].(string)
	tokID := r.body["token"].(map[string]any)["id"].(string)
	if !strings.HasPrefix(secret, "kwt_") {
		t.Fatal("secret")
	}
	f.expect(f.do("POST", "/v1/admin/accounts/"+acctID+"/tokens", a, map[string]any{"expires_at": f.now.Add(time.Hour)}, ""), 400, "token without comment")
	f.expect(f.do("POST", "/v1/admin/accounts/"+acctID+"/tokens", a, map[string]any{"roles": []string{"send"}, "expires_at": f.now.Add(time.Hour), "comment": "x"}, ""), 400, "token role escalation")
	r = f.do("GET", "/v1/admin/accounts/"+acctID+"/tokens", a, nil, "")
	if len(r.body["items"].([]any)) != 1 {
		t.Fatalf("tokens: %v", r.body)
	}
	// Disabled account's token → 403.
	f.expect(f.do("GET", "/v1/me", f.bearer(secret), nil, ""), 403, "disabled account token")
	_, _ = f.ids.UpdateAccount(context.Background(), uuid.MustParse(acctID), func(x *identity.Account) error { x.Enabled = true; return nil })
	f.expect(f.do("GET", "/v1/me", f.bearer(secret), nil, ""), 200, "token works")
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+acctID+"/tokens/"+tokID, a, nil, ""), 204, "token delete")
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+acctID+"/tokens/"+tokID, a, nil, ""), 404, "token delete twice")
	f.expect(f.do("GET", "/v1/me", f.bearer(secret), nil, ""), 401, "revoked token")
	// Aliases.
	r = f.do("POST", "/v1/admin/aliases", a, map[string]any{"address": "Info@test.example", "destinations": []map[string]any{{"mailbox_id": mbID}, {"address": "bob@test.example"}}}, "")
	f.expect(r, 201, "alias create")
	if r.body["address"] != "info@test.example" {
		t.Fatalf("alias: %v", r.body)
	}
	f.expect(f.do("POST", "/v1/admin/aliases", a, map[string]any{"address": "@test.example", "destinations": []map[string]any{{"mailbox_id": mbID}}}, ""), 201, "catch-all")
	f.expect(f.do("POST", "/v1/admin/aliases", a, map[string]any{"address": "x@test.example"}, ""), 400, "alias no destinations")
	f.expect(f.do("POST", "/v1/admin/aliases", a, map[string]any{"address": "bob@test.example", "destinations": []map[string]any{{"mailbox_id": mbID}}}, ""), 409, "alias over account")
	r = f.do("GET", "/v1/admin/aliases?domain=test.example", a, nil, "")
	if items := r.body["items"].([]any); len(items) != 2 {
		t.Fatalf("alias list: %v", r.body)
	}
	f.expect(f.do("GET", "/v1/admin/aliases/info@test.example", a, nil, ""), 200, "alias get")
	r = f.do("PATCH", "/v1/admin/aliases/info@test.example", a, map[string]any{"enabled": false}, "")
	f.expect(r, 200, "alias patch")
	if r.body["enabled"] != false {
		t.Fatal("alias patch")
	}
	f.expect(f.do("DELETE", "/v1/admin/aliases/info@test.example", a, nil, ""), 204, "alias delete")
	f.expect(f.do("DELETE", "/v1/admin/aliases/info@test.example", a, nil, ""), 404, "alias delete twice")
	// Export / import.
	r = f.do("GET", "/v1/admin/export", a, nil, "")
	f.expect(r, 200, "export")
	if len(r.body["domains"].([]any)) != 2 {
		t.Fatalf("export: %v", r.body["domains"])
	}
	r = f.do("POST", "/v1/admin/import", a, r.raw, "application/json")
	f.expect(r, 200, "import")
	if !strings.HasPrefix(r.body["summary"].(string), "created=0 skipped=") {
		t.Fatalf("import summary: %v", r.body)
	}
	f.expect(f.do("POST", "/v1/admin/import", a, "nope", "application/json"), 400, "import bad")
	// Delete chain: account, mailbox (202 + queue), domain in use → 409 then ok.
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+acctID, a, nil, ""), 204, "account delete")
	f.expect(f.do("DELETE", "/v1/admin/domains/test.example", a, nil, ""), 409, "domain in use (alias)")
	f.expect(f.do("DELETE", "/v1/admin/aliases/@test.example", a, nil, ""), 204, "catch-all delete")
	f.expect(f.do("DELETE", "/v1/admin/domains/test.example", a, nil, ""), 204, "domain delete")
	f.expect(f.do("DELETE", "/v1/admin/mailboxes/"+mb2, a, nil, ""), 202, "mailbox delete")
	r = f.do("GET", "/v1/admin/queues/remove-mailbox", a, nil, "")
	f.expect(r, 200, "queue stats")
	if r.body["pending"].(float64) != 1 {
		t.Fatalf("queue: %v", r.body)
	}
	r = f.do("GET", "/v1/admin/queues/remove-mailbox/dead", a, nil, "")
	if len(r.body["items"].([]any)) != 0 {
		t.Fatal("dead")
	}
	f.expect(f.do("POST", "/v1/admin/queues/remove-mailbox/dead/nope/retry", a, nil, ""), 404, "retry missing")
	// Key rotation keeps admin token working (API tokens are not JWTs) and sessions valid.
	l = f.login("wido@example.org", "correct horse", nil)
	f.expect(f.do("POST", "/v1/admin/keys/rotate", a, nil, ""), 204, "rotate")
	f.expect(f.do("GET", "/v1/me", f.bearer(l.body["access_token"].(string)), nil, ""), 200, "old jwt after rotate")
}

func TestHoldAndAudit(t *testing.T) {
	f := newFixture(t)
	a := f.adminAuth()
	mb := f.mb.ID.String()
	f.expect(f.do("POST", "/v1/admin/mailboxes/"+mb+"/hold", a, map[string]any{"case_ref": "42"}, ""), 400, "hold without reason")
	r := f.do("POST", "/v1/admin/mailboxes/"+mb+"/hold", a, map[string]any{"reason": "subpoena", "case_ref": "42", "sealed": true}, "")
	f.expect(r, 200, "hold")
	hold := r.body["hold"].(map[string]any)
	if hold["set_by"] != "admin:test admin" || hold["accept_delivery"] != true || hold["sealed"] != true {
		t.Fatalf("hold: %v", hold)
	}
	f.expect(f.do("POST", "/v1/admin/mailboxes/"+mb+"/hold", a, map[string]any{"reason": "again"}, ""), 409, "double hold")
	r = f.do("GET", "/v1/admin/holds", a, nil, "")
	if len(r.body["items"].([]any)) != 1 {
		t.Fatalf("holds: %v", r.body)
	}
	// User is locked out of the sealed mailbox at the token level? No: login still works; mailbox access is the JMAP side.
	f.expect(f.do("DELETE", "/v1/admin/mailboxes/"+mb, a, nil, ""), 409, "delete on hold")
	r = f.do("DELETE", "/v1/admin/mailboxes/"+mb+"/hold", a, map[string]any{"note": "closed"}, "")
	f.expect(r, 200, "release")
	if r.body["hold"] != nil || r.body["released"] == nil {
		t.Fatalf("release: %v", r.body)
	}
	f.expect(f.do("DELETE", "/v1/admin/mailboxes/"+mb+"/hold", a, nil, ""), 404, "release twice")
	// Audit: mailbox log has hold/release; admin log has them too with admin actor.
	day := f.now.Format("2006-01-02")
	r = f.do("GET", "/v1/admin/audit?mailbox="+mb+"&from="+day+"&to="+day, a, nil, "")
	f.expect(r, 200, "audit mailbox")
	actions := map[string]bool{}
	for _, it := range r.body["items"].([]any) {
		rec := it.(map[string]any)
		actions[rec["action"].(string)] = true
		if rec["account"] != "admin:test admin" || rec["protocol"] != "admin" {
			t.Fatalf("audit actor: %v", rec)
		}
	}
	if !actions["hold"] || !actions["release"] {
		t.Fatalf("audit actions: %v", actions)
	}
	r = f.do("GET", "/v1/admin/audit?from="+day+"&action=hold", a, nil, "")
	// One ok record plus the audited failure of the double hold.
	if items := r.body["items"].([]any); len(items) != 2 || items[0].(map[string]any)["result"] != "ok" || items[1].(map[string]any)["result"] != "error" {
		t.Fatalf("admin log filter: %v", r.body)
	}
	r = f.do("GET", "/v1/admin/audit?from="+day+"&limit=2", a, nil, "")
	if len(r.body["items"].([]any)) != 2 || r.body["next"] == "" {
		t.Fatalf("audit paging: %v", r.body)
	}
	r = f.do("GET", "/v1/admin/audit?from="+day+"&after="+url.QueryEscape(r.body["next"].(string)), a, nil, "")
	f.expect(r, 200, "audit page 2")
	f.expect(f.do("GET", "/v1/admin/audit?from=yesterday", a, nil, ""), 400, "audit bad date")
	f.expect(f.do("GET", "/v1/admin/audit?mailbox=x", a, nil, ""), 400, "audit bad mailbox")
}

// TestSpecMatchesRoutes keeps api/openapi.yaml and the mux in sync.
func TestSpecMatchesRoutes(t *testing.T) {
	f := newFixture(t)
	data, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	var specRoutes []string
	for p, ops := range spec.Paths {
		for m := range ops {
			switch m {
			case "get", "post", "put", "patch", "delete":
				specRoutes = append(specRoutes, strings.ToUpper(m)+" "+p)
			}
		}
	}
	sort.Strings(specRoutes)
	routes := f.srv.Routes()
	for i, r := range routes {
		routes[i] = strings.Replace(r, " /{$}", " /", 1) // ServeMux exact-root spelling
	}
	sort.Strings(routes)
	if strings.Join(routes, "\n") != strings.Join(specRoutes, "\n") {
		t.Fatalf("routes and spec differ:\nmux:\n%s\nspec:\n%s", strings.Join(routes, "\n"), strings.Join(specRoutes, "\n"))
	}
}

func TestReadOnlyAdminAndPublicSpec(t *testing.T) {
	f := newFixture(t)
	// Spec and banner are public on the admin handler.
	f.expect(f.do("GET", "/openapi.yaml", "", nil, ""), 200, "public spec")
	f.expect(f.do("GET", "/v1/admin/openapi.yaml", "", nil, ""), 200, "public spec under prefix")
	f.expect(f.do("GET", "/", "", nil, ""), 200, "banner")
	// Read-only token: GETs work, mutations are 403.
	f.expect(f.do("GET", "/v1/admin/status", f.bearer(f.roTk), nil, ""), 200, "ro status")
	f.expect(f.do("GET", "/v1/admin/domains", f.bearer(f.roTk), nil, ""), 200, "ro list")
	f.expect(f.do("GET", "/v1/admin/tokens", f.bearer(f.roTk), nil, ""), 200, "ro token list")
	f.expect(f.do("POST", "/v1/admin/domains", f.bearer(f.roTk), map[string]any{"name": "ro.example"}, ""), 403, "ro create")
	f.expect(f.do("POST", "/v1/admin/tokens", f.bearer(f.roTk), map[string]any{"comment": "x", "expires_at": f.now.Add(time.Hour)}, ""), 403, "ro mint")
	f.expect(f.do("DELETE", "/v1/admin/domains/example.org", f.bearer(f.roTk), nil, ""), 403, "ro delete")
	// Full admin manages admin tokens; a user token is never an admin.
	r := f.do("POST", "/v1/admin/tokens", f.bearer(f.adminTk), map[string]any{"comment": "ci", "expires_at": f.now.Add(time.Hour), "read_only": true}, "")
	f.expect(r, 201, "mint ro")
	id := r.body["token"].(map[string]any)["id"].(string)
	secret := r.body["secret"].(string)
	f.expect(f.do("GET", "/v1/admin/mailboxes", f.bearer(secret), nil, ""), 200, "new ro token works")
	r = f.do("GET", "/v1/admin/tokens", f.bearer(f.adminTk), nil, "")
	f.expect(r, 200, "list")
	if n := len(r.body["items"].([]any)); n != 3 {
		t.Fatalf("admin tokens: %d", n)
	}
	f.expect(f.do("DELETE", "/v1/admin/tokens/"+id, f.bearer(f.adminTk), nil, ""), 204, "revoke")
	f.expect(f.do("GET", "/v1/admin/mailboxes", f.bearer(secret), nil, ""), 401, "revoked token")
	// Accounts cannot be given admin roles.
	f.expect(f.do("POST", "/v1/admin/accounts", f.bearer(f.adminTk), map[string]any{"username": "x@example.org", "mailbox_id": f.mb.ID, "roles": []string{"admin"}}, ""), 400, "admin role on account")
	// Audit shows the token actor.
	r = f.do("GET", "/v1/admin/audit?from="+f.now.Format("2006-01-02")+"&action=admin-token-create", f.bearer(f.adminTk), nil, "")
	f.expect(r, 200, "audit")
	if items := r.body["items"].([]any); len(items) != 1 || items[0].(map[string]any)["account"] != "admin:test admin" {
		t.Fatalf("audit actor: %v", items)
	}
}

func TestStorageRegistry(t *testing.T) {
	f := newFixture(t)
	a := f.adminAuth()
	pools := map[string]string{"messages": "m", "attachments": "a", "index": "i"}
	f.expect(f.do("POST", "/v1/admin/storage", a, map[string]any{"name": "ams1", "mon_host": "[2001:db8::1]:6789", "user": "kittiwake", "key": "AQ==", "pools": map[string]string{"messages": "m"}}, ""), 400, "storage add incomplete pools")
	r := f.do("POST", "/v1/admin/storage", a, map[string]any{"name": "ams1", "mon_host": "[2001:db8::1]:6789", "user": "kittiwake", "key": "AQ==", "pools": pools}, "")
	f.expect(r, 201, "storage add")
	if _, hasKey := r.body["key"]; hasKey || r.body["name"] != "ams1" || r.body["placement"] != true || r.body["id"] == nil {
		t.Fatalf("storage view: %v", r.body)
	}
	id := fmt.Sprint(int64(r.body["id"].(float64)))
	f.expect(f.do("POST", "/v1/admin/storage", a, map[string]any{"name": "ams1", "mon_host": "x", "user": "u", "key": "k", "pools": pools}, ""), 409, "storage dup name")
	r = f.do("POST", "/v1/admin/storage", a, map[string]any{"name": "rtd1", "mon_host": "[2001:db8::2]:6789", "user": "kittiwake", "key": "AQ==", "pools": pools, "placement": false}, "")
	f.expect(r, 201, "storage add closed")
	id2 := fmt.Sprint(int64(r.body["id"].(float64)))
	r = f.do("GET", "/v1/admin/storage", a, nil, "")
	f.expect(r, 200, "storage list")
	if items := r.body["items"].([]any); len(items) != 2 || items[1].(map[string]any)["placement"] != false {
		t.Fatalf("storage list: %v", r.body)
	}
	// A mailbox pinned to a storage, and one placed by the service (memory: storage 0).
	r = f.do("POST", "/v1/admin/mailboxes", a, map[string]any{"storage_id": json.Number(id2)}, "")
	f.expect(r, 201, "mailbox on storage")
	if fmt.Sprint(r.body["storage_id"]) != id2 {
		t.Fatalf("mailbox storage: %v", r.body)
	}
	mb := r.body["id"].(string)
	f.expect(f.do("POST", "/v1/admin/mailboxes", a, map[string]any{"storage_id": 999}, ""), 404, "mailbox on unknown storage")
	f.expect(f.do("PATCH", "/v1/admin/mailboxes/"+mb, a, map[string]any{"storage_id": json.Number(id)}, ""), 400, "mailbox storage change refused")
	r = f.do("GET", "/v1/admin/mailboxes", a, nil, "")
	f.expect(r, 200, "mailbox list")
	found := false
	for _, it := range r.body["items"].([]any) {
		m := it.(map[string]any)
		if m["id"] == mb && fmt.Sprint(m["storage_id"]) == id2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("mailbox list lacks storage: %v", r.body)
	}
	// Placement toggle and removal rules.
	r = f.do("PATCH", "/v1/admin/storage/"+id, a, map[string]any{"placement": false}, "")
	f.expect(r, 200, "storage close")
	if r.body["placement"] != false {
		t.Fatalf("placement not changed: %v", r.body)
	}
	f.expect(f.do("DELETE", "/v1/admin/storage/"+id2, a, nil, ""), 409, "storage in use")
	f.expect(f.do("DELETE", "/v1/admin/storage/"+id, a, nil, ""), 204, "storage remove")
	f.expect(f.do("GET", "/v1/admin/storage/"+id, a, nil, ""), 404, "storage gone")
	f.expect(f.do("GET", "/v1/admin/storage/abc", a, nil, ""), 400, "bad storage id")
	// Read-only admins may list but not change.
	ro := f.bearer(f.roTk)
	f.expect(f.do("GET", "/v1/admin/storage", ro, nil, ""), 200, "ro list")
	f.expect(f.do("PATCH", "/v1/admin/storage/"+id2, ro, map[string]any{"placement": true}, ""), 403, "ro patch")
}

func TestAdminPasswordManagement(t *testing.T) {
	f := newFixture(t)
	a := f.adminAuth()
	r := f.do("POST", "/v1/admin/domains", a, map[string]any{"name": "pw.example"}, "")
	f.expect(r, 201, "domain")
	r = f.do("POST", "/v1/admin/mailboxes", a, map[string]any{}, "")
	f.expect(r, 201, "mailbox")
	mb := r.body["id"].(string)
	r = f.do("POST", "/v1/admin/accounts", a, map[string]any{"username": "u@pw.example", "mailbox_id": mb, "roles": []string{"read"}, "password": "first-password-1"}, "")
	f.expect(r, 201, "account")
	id := r.body["id"].(string)
	// A second, application password with an expiry.
	exp := f.now.Add(48 * time.Hour).UTC().Format(time.RFC3339)
	r = f.do("PUT", "/v1/admin/accounts/"+id+"/password", a, map[string]any{"password": "phone-password-1", "kind": "application", "comment": "phone", "expires_at": exp}, "")
	f.expect(r, 200, "app password")
	pid := r.body["id"].(string)
	r = f.do("GET", "/v1/admin/accounts/"+id+"/passwords", a, nil, "")
	f.expect(r, 200, "list")
	items := r.body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("passwords: %v", items)
	}
	// Expiry to never, comment changed.
	r = f.do("PATCH", "/v1/admin/accounts/"+id+"/passwords/"+pid, a, map[string]any{"expires_at": nil, "comment": "old phone"}, "")
	f.expect(r, 200, "patch")
	if r.body["expires_at"] != nil || r.body["comment"] != "old phone" {
		t.Fatalf("patch result: %v", r.body)
	}
	// Expiry in the past is refused; unknown id is 404.
	f.expect(f.do("PATCH", "/v1/admin/accounts/"+id+"/passwords/"+pid, a, map[string]any{"expires_at": f.now.Add(-time.Hour).UTC().Format(time.RFC3339)}, ""), 400, "past expiry")
	f.expect(f.do("PATCH", "/v1/admin/accounts/"+id+"/passwords/nope", a, map[string]any{"expires_at": nil}, ""), 404, "unknown password")
	// Remove, then it is gone.
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+id+"/passwords/"+pid, a, nil, ""), 204, "remove")
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+id+"/passwords/"+pid, a, nil, ""), 404, "remove twice")
	r = f.do("GET", "/v1/admin/accounts/"+id+"/passwords", a, nil, "")
	if len(r.body["items"].([]any)) != 1 {
		t.Fatalf("after remove: %v", r.body)
	}
	// A migrated hash as the primary password: exactly one of password/hash, primary only.
	f.expect(f.do("PUT", "/v1/admin/accounts/"+id+"/password", a, map[string]any{"password": "x", "hash": "y"}, ""), 400, "both")
	f.expect(f.do("PUT", "/v1/admin/accounts/"+id+"/password", a, map[string]any{}, ""), 400, "neither")
	f.expect(f.do("PUT", "/v1/admin/accounts/"+id+"/password", a, map[string]any{"hash": "$6$saltsalt$W1Pe3prjOPvOhWOPQaRxextX94W2cyh4mpskonVD2EAq/QU1pFhqobuszJeSY4R9obTz8JJsiQrZMn.15n4Ep/", "kind": "application", "comment": "migrated app"}, ""), 200, "hash as application password")
	f.expect(f.do("PUT", "/v1/admin/accounts/"+id+"/password", a, map[string]any{"hash": "$1$nope$abc"}, ""), 400, "unsupported scheme")
	r = f.do("PUT", "/v1/admin/accounts/"+id+"/password", a, map[string]any{"hash": "$6$saltsalt$W1Pe3prjOPvOhWOPQaRxextX94W2cyh4mpskonVD2EAq/QU1pFhqobuszJeSY4R9obTz8JJsiQrZMn.15n4Ep/"}, "")
	f.expect(r, 200, "hash")
	r = f.do("GET", "/v1/admin/accounts/"+id+"/passwords", a, nil, "")
	var schemes []string
	for _, it := range r.body["items"].([]any) {
		schemes = append(schemes, it.(map[string]any)["scheme"].(string))
	}
	if len(schemes) != 2 || schemes[0] != "SHA512-CRYPT" || schemes[1] != "SHA512-CRYPT" {
		t.Fatalf("schemes after hash import: %v", schemes)
	}
	// Read-only admins may list, not change.
	f.expect(f.do("GET", "/v1/admin/accounts/"+id+"/passwords", f.bearer(f.roTk), nil, ""), 200, "ro list")
	f.expect(f.do("DELETE", "/v1/admin/accounts/"+id+"/passwords/"+pid, f.bearer(f.roTk), nil, ""), 403, "ro remove")
}
