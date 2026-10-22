// Package rest is the custom REST surface described by api/openapi.yaml:
// the OAuth token/revoke endpoints, self-service credential management
// (/v1/me/...) and the admin API (/v1/admin/...). It is a thin adapter over
// service/auth and service/admin; it holds no logic of its own beyond
// request decoding, error mapping and route wiring.
package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	cryptoauth "kittiwake/auth"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/api"
	"kittiwake/proto/httpx"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/service/auth"
	"kittiwake/service/export"
	"kittiwake/service/submit"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/queue"
	"kittiwake/store/session"
	"kittiwake/store/storage"
)

// Version reported by /v1/admin/status.
var Version = "dev"

// Config for the server.
type Config struct {
	Realm          string
	TrustedProxies []net.IPNet
	DefaultQuota   identity.QuotaLimits
	MaxBody        int64
	// Identity is optional; when set, /v1/me includes the password list and
	// TOTP state (the auth service does not expose account records).
	Identity *identity.Store
	// Export and ExportDir enable mailbox export (worker task) and import.
	Export    *export.Service
	ExportDir string
	// Submit is optional; it backs PATCH /v1/me (display name).
	Submit *submit.Service
}

// Server serves the REST endpoints.
type Server struct {
	cfg    Config
	auth   *auth.Service
	admin  *admin.Service
	audit  *audit.Store
	queue  queue.Store
	log    *slog.Logger
	mu     sync.Mutex
	routes []string
}

// New returns a server.
func New(cfg Config, authSvc *auth.Service, adminSvc *admin.Service, auditStore *audit.Store, queueStore queue.Store, log *slog.Logger) *Server {
	if cfg.Realm == "" {
		cfg.Realm = "kittiwake"
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 4 << 20
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, auth: authSvc, admin: adminSvc, audit: auditStore, queue: queueStore, log: log}
}

// Routes returns the registered "METHOD /path" patterns (for the spec test).
func (s *Server) Routes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.routes...)
	sort.Strings(out)
	return out
}

type route struct {
	pattern string
	h       http.HandlerFunc
}

// register installs routes on mux with the wrapper and records them.
func (s *Server) register(mux *http.ServeMux, wrap func(http.Handler) http.Handler, rs ...route) {
	body := httpx.MaxBody(s.cfg.MaxBody)
	for _, r := range rs {
		var h http.Handler = r.h
		if wrap != nil {
			h = wrap(h)
		}
		mux.Handle(r.pattern, body(h))
		s.mu.Lock()
		s.routes = append(s.routes, r.pattern)
		s.mu.Unlock()
	}
}

// AccountHandler serves the user-facing endpoints that belong next to JMAP:
// the OAuth token/revoke endpoints and the /v1/me self-service endpoints.
// It is mounted on the jmap listener.
func (s *Server) AccountHandler() http.Handler {
	mux := http.NewServeMux()
	selfAuth := httpx.Auth(s.auth, audit.ProtoREST, s.cfg.TrustedProxies, s.cfg.Realm)
	public := func(h http.Handler) http.Handler { return h }
	self := func(h http.Handler) http.Handler { return selfAuth(h) }
	reg := func(wrap func(http.Handler) http.Handler, rs ...route) { s.register(mux, wrap, rs...) }
	reg(public,
		route{"POST /v1/oauth/token", s.oauthToken},
		route{"POST /v1/oauth/revoke", s.oauthRevoke},
	)
	reg(self,
		route{"GET /v1/me", s.me},
		route{"PATCH /v1/me", s.mePatch},
		route{"GET /v1/me/sessions", s.meSessions},
		route{"DELETE /v1/me/sessions/{id}", s.meRevokeSession},
		route{"PUT /v1/me/password", s.mePassword},
		route{"POST /v1/me/app-passwords", s.meAppPassword},
		route{"DELETE /v1/me/passwords/{id}", s.meDeletePassword},
		route{"POST /v1/me/totp", s.meTOTPBegin},
		route{"POST /v1/me/totp/confirm", s.meTOTPConfirm},
		route{"DELETE /v1/me/totp", s.meTOTPDisable},
	)
	// TODO: self-service API token management (/v1/me/tokens) needs a
	// token CRUD on the auth service; until then tokens are admin-managed.
	return mux
}

// AdminHandler serves the operator-facing /v1/admin endpoints. It is
// mounted on the admin listener, which should only be reachable by
// operators.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	adminAuth := httpx.Auth(s.auth, audit.ProtoAdmin, s.cfg.TrustedProxies, s.cfg.Realm)
	// Mutations need a full admin token; reads accept read-only admin tokens.
	adm := func(h http.Handler) http.Handler { return adminAuth(httpx.RequireRole(identity.RoleAdmin)(h)) }
	admRead := func(h http.Handler) http.Handler {
		return adminAuth(httpx.RequireAnyRole(identity.RoleAdmin, identity.RoleAdminRead)(h))
	}
	public := func(h http.Handler) http.Handler { return h }
	reg := func(wrap func(http.Handler) http.Handler, rs ...route) {
		// Every GET is a read: read-only admins may use it.
		for _, r := range rs {
			w := wrap
			if strings.HasPrefix(r.pattern, "GET ") && isAdminWrap(wrap, adm) {
				w = admRead
			}
			s.register(mux, w, r)
		}
	}
	// The contract itself and the banner are public: the operations need a
	// token, the description of them does not.
	reg(public, route{"GET /openapi.yaml", s.openAPI}, route{"GET /{$}", s.root}, route{"GET /v1/admin/openapi.yaml", s.openAPI})
	// Admin tokens (admins are tokens, not accounts).
	reg(adm,
		route{"POST /v1/admin/tokens", s.adminTokenCreate},
		route{"GET /v1/admin/tokens", s.adminTokenList},
		route{"DELETE /v1/admin/tokens/{tid}", s.adminTokenDelete},
	)
	reg(adm,
		route{"GET /v1/admin/status", s.adminStatus},
		route{"GET /v1/admin/nodes", s.nodeList},
		route{"POST /v1/admin/storage", s.storageAdd},
		route{"GET /v1/admin/storage", s.storageList},
		route{"GET /v1/admin/storage/{sid}", s.storageGet},
		route{"PATCH /v1/admin/storage/{sid}", s.storagePatch},
		route{"DELETE /v1/admin/storage/{sid}", s.storageDelete},
		route{"POST /v1/admin/domains", s.domainCreate},
		route{"GET /v1/admin/domains", s.domainList},
		route{"GET /v1/admin/domains/{name}", s.domainGet},
		route{"PATCH /v1/admin/domains/{name}", s.domainPatch},
		route{"DELETE /v1/admin/domains/{name}", s.domainDelete},
		route{"POST /v1/admin/mailboxes", s.mailboxCreate},
		route{"GET /v1/admin/mailboxes", s.mailboxList},
		route{"GET /v1/admin/mailboxes/{id}", s.mailboxGet},
		route{"PATCH /v1/admin/mailboxes/{id}", s.mailboxPatch},
		route{"DELETE /v1/admin/mailboxes/{id}", s.mailboxDelete},
		route{"POST /v1/admin/mailboxes/{id}/recalculate", s.mailboxRecalculate},
		route{"POST /v1/admin/mailboxes/{id}/purge", s.mailboxPurge},
		route{"POST /v1/admin/mailboxes/{id}/export", s.mailboxExport},
		route{"POST /v1/admin/mailboxes/{id}/import", s.mailboxImport},
		route{"GET /v1/admin/exports", s.exportList},
		route{"GET /v1/admin/exports/{name}", s.exportDownload},
		route{"POST /v1/admin/mailboxes/{id}/hold", s.mailboxHold},
		route{"DELETE /v1/admin/mailboxes/{id}/hold", s.mailboxRelease},
		route{"GET /v1/admin/holds", s.holdList},
		route{"POST /v1/admin/accounts", s.accountCreate},
		route{"GET /v1/admin/accounts", s.accountList},
		route{"GET /v1/admin/accounts/{id}", s.accountGet},
		route{"PATCH /v1/admin/accounts/{id}", s.accountPatch},
		route{"DELETE /v1/admin/accounts/{id}", s.accountDelete},
		route{"PUT /v1/admin/accounts/{id}/password", s.accountPassword},
		route{"GET /v1/admin/accounts/{id}/passwords", s.accountPasswordList},
		route{"PATCH /v1/admin/accounts/{id}/passwords/{pid}", s.accountPasswordPatch},
		route{"DELETE /v1/admin/accounts/{id}/passwords/{pid}", s.accountPasswordDelete},
		route{"DELETE /v1/admin/accounts/{id}/totp", s.accountTOTPDisable},
		route{"GET /v1/admin/accounts/{id}/sessions", s.accountSessions},
		route{"DELETE /v1/admin/accounts/{id}/sessions/{sid}", s.accountRevokeSession},
		route{"POST /v1/admin/accounts/{id}/tokens", s.tokenCreate},
		route{"GET /v1/admin/accounts/{id}/tokens", s.tokenList},
		route{"DELETE /v1/admin/accounts/{id}/tokens/{tid}", s.tokenDelete},
		route{"POST /v1/admin/aliases", s.aliasCreate},
		route{"GET /v1/admin/aliases", s.aliasList},
		route{"GET /v1/admin/aliases/{address}", s.aliasGet},
		route{"PATCH /v1/admin/aliases/{address}", s.aliasPatch},
		route{"DELETE /v1/admin/aliases/{address}", s.aliasDelete},
		route{"GET /v1/admin/audit", s.auditRead},
		route{"GET /v1/admin/queues/{type}", s.queueStats},
		route{"GET /v1/admin/queues/{type}/dead", s.queueDead},
		route{"POST /v1/admin/queues/{type}/dead/{id}/retry", s.queueRetry},
		route{"POST /v1/admin/keys/rotate", s.keysRotate},
		route{"GET /v1/admin/export", s.exportIdentity},
		route{"POST /v1/admin/import", s.importIdentity},
	)
	return mux
}

// isAdminWrap reports whether wrap is the full-admin wrapper (func values
// cannot be compared, so the registration passes a marker through the
// wrapper identity via reflect-free trick: compare pointers of a sentinel).
func isAdminWrap(wrap, adm func(http.Handler) http.Handler) bool {
	return fmt.Sprintf("%p", wrap) == fmt.Sprintf("%p", adm)
}

type adminTokenRequest struct {
	ReadOnly  bool      `json:"read_only"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
}

func (s *Server) adminTokenCreate(w http.ResponseWriter, r *http.Request) {
	var req adminTokenRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	t, secret, err := s.admin.CreateAdminToken(r.Context(), req.ReadOnly, req.ExpiresAt, req.Comment)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"token": viewToken(t), "secret": secret})
}

func (s *Server) adminTokenList(w http.ResponseWriter, r *http.Request) {
	list, err := s.admin.ListAdminTokens(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := make([]tokenView, 0, len(list))
	for _, t := range list {
		out = append(out, viewToken(t))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) adminTokenDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.admin.DeleteAdminToken(r.Context(), r.PathValue("tid")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// openAPI serves the embedded specification.
func (s *Server) openAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(api.OpenAPI)
}

// root identifies the listener.
func (s *Server) root(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintf(w, "kittiwake %s admin API\nspec: /openapi.yaml\n", Version)
}

// Handler combines both handlers on one mux (tests and single-listener
// deployments). Production runs them on separate listeners.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	account := s.AccountHandler()
	mux.Handle("/v1/oauth/", account)
	mux.Handle("/v1/me", account)
	mux.Handle("/v1/me/", account)
	mux.Handle("/", s.AdminHandler()) // /v1/admin/*, /openapi.yaml, /
	return mux
}

// ---- helpers ----

func (s *Server) clientIP(r *http.Request) string { return httpx.ClientIP(r, s.cfg.TrustedProxies) }

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// fail maps service errors to HTTP problems.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var status int
	detail := err.Error()
	switch {
	case errors.Is(err, identity.ErrNotFound), errors.Is(err, session.ErrNotFound), errors.Is(err, queue.ErrNotFound), errors.Is(err, index.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, identity.ErrExists):
		status = http.StatusConflict
	case errors.Is(err, identity.ErrInUse), errors.Is(err, identity.ErrConflict), errors.Is(err, session.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, identity.ErrInvalid), errors.Is(err, identity.ErrDomainUnknown), errors.Is(err, errBadRequest):
		status = http.StatusBadRequest
	case errors.Is(err, admin.ErrForbidden), errors.Is(err, export.ErrForbidden):
		status = http.StatusForbidden
	case errors.Is(err, export.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, auth.ErrTOTPInvalid), errors.Is(err, auth.ErrScope):
		status = http.StatusBadRequest
	case errors.Is(err, auth.ErrInvalidCredentials), errors.Is(err, auth.ErrInvalidToken), errors.Is(err, auth.ErrTokenExpired), errors.Is(err, auth.ErrTOTPRequired), errors.Is(err, auth.ErrSessionRevoked):
		status = http.StatusUnauthorized
	case errors.Is(err, auth.ErrAccountDisabled), errors.Is(err, auth.ErrServiceDisabled):
		status = http.StatusForbidden
	case errors.Is(err, storage.ErrUnavailable), errors.Is(err, storage.ErrNoPlacement):
		status = http.StatusServiceUnavailable
	default:
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			status, detail = http.StatusRequestEntityTooLarge, "request body too large"
			break
		}
		var jsonErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &jsonErr) || errors.As(err, &typeErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || strings.HasPrefix(detail, "json: ") {
			status, detail = http.StatusBadRequest, "malformed JSON body: "+detail
			break
		}
		s.log.ErrorContext(r.Context(), "rest: internal error", slog.String("path", r.URL.Path), slog.String("error", err.Error()))
		status, detail = http.StatusInternalServerError, "internal error"
	}
	httpx.WriteProblem(w, status, "about:blank", detail)
}

var errBadRequest = errors.New("bad request")

func badRequest(msg string) error { return errors.Join(errBadRequest, errors.New(msg)) }

func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return uuid.Nil, badRequest("invalid " + name + ": not a uuid")
	}
	return id, nil
}

func limitParam(r *http.Request) int {
	n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if n <= 0 {
		return 100
	}
	if n > 1000 {
		return 1000
	}
	return n
}

type listResponse struct {
	Items []string `json:"items"`
	More  bool     `json:"more"`
	Next  string   `json:"next,omitempty"`
}

func listOut(items []string, more bool) listResponse {
	if items == nil {
		items = []string{}
	}
	out := listResponse{Items: items, More: more}
	if more && len(items) > 0 {
		out.Next = items[len(items)-1]
	}
	return out
}

// ---- OAuth ----

type oauthError struct {
	Error       string `json:"error"`
	Description string `json:"error_description,omitempty"`
}

func writeOAuthError(w http.ResponseWriter, status int, code, desc string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(oauthError{Error: code, Description: desc})
}

type tokenRequest struct {
	GrantType    string `json:"grant_type"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Scope        string `json:"scope"`
	ClientName   string `json:"client_name"`
	ClientID     string `json:"client_id"`
	TOTPCode     string `json:"totp_code"`
	RefreshToken string `json:"refresh_token"`
	Token        string `json:"token"`
}

// readTokenRequest accepts form-encoded or JSON bodies.
func readTokenRequest(r *http.Request) (tokenRequest, error) {
	var req tokenRequest
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			return req, err
		}
		return req, nil
	}
	if err := r.ParseForm(); err != nil {
		return req, err
	}
	f := r.PostForm
	req = tokenRequest{
		GrantType: f.Get("grant_type"), Username: f.Get("username"), Password: f.Get("password"), Scope: f.Get("scope"),
		ClientName: f.Get("client_name"), ClientID: f.Get("client_id"), TOTPCode: f.Get("totp_code"),
		RefreshToken: f.Get("refresh_token"), Token: f.Get("token"),
	}
	return req, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	SessionID    string `json:"session_id"`
}

func (s *Server) oauthToken(w http.ResponseWriter, r *http.Request) {
	req, err := readTokenRequest(r)
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "malformed request body")
		return
	}
	var toks auth.Tokens
	switch req.GrantType {
	case "password":
		if req.Username == "" || req.Password == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "username and password are required")
			return
		}
		client := req.ClientName
		if client == "" {
			client = req.ClientID
		}
		toks, err = s.auth.Login(r.Context(), auth.LoginRequest{
			Username: req.Username, Password: req.Password, TOTPCode: req.TOTPCode,
			Scope: strings.Fields(req.Scope), Client: client, ClientIP: s.clientIP(r), Protocol: audit.ProtoREST,
		})
	case "refresh_token":
		if req.RefreshToken == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "refresh_token is required")
			return
		}
		toks, err = s.auth.Refresh(r.Context(), req.RefreshToken, s.clientIP(r), audit.ProtoREST)
	case "":
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "grant_type is required")
		return
	default:
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be password or refresh_token")
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrTOTPRequired):
			writeOAuthError(w, http.StatusBadRequest, "totp_required", "a one-time code is required for this account; resend with totp_code")
		case errors.Is(err, auth.ErrScope):
			writeOAuthError(w, http.StatusBadRequest, "invalid_scope", "requested scope exceeds the account's roles")
		case errors.Is(err, auth.ErrAccountDisabled):
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "account disabled")
		case errors.Is(err, auth.ErrServiceDisabled):
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "JMAP is not enabled for this account")
		case errors.Is(err, auth.ErrInvalidCredentials), errors.Is(err, auth.ErrTOTPInvalid),
			errors.Is(err, auth.ErrSessionRevoked), errors.Is(err, auth.ErrInvalidToken), errors.Is(err, auth.ErrTokenExpired):
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "invalid credentials or token")
		default:
			s.log.ErrorContext(r.Context(), "rest: token endpoint", slog.String("error", err.Error()))
			writeOAuthError(w, http.StatusInternalServerError, "server_error", "internal error")
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, tokenResponse{
		AccessToken: toks.AccessToken, TokenType: toks.TokenType, ExpiresIn: toks.ExpiresIn,
		RefreshToken: toks.RefreshToken, Scope: strings.Join(toks.Scope, " "), SessionID: toks.SessionID,
	})
}

func (s *Server) oauthRevoke(w http.ResponseWriter, r *http.Request) {
	req, err := readTokenRequest(r)
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "malformed request body")
		return
	}
	tok := req.Token
	if tok == "" {
		tok = req.RefreshToken
	}
	if err := s.auth.Revoke(r.Context(), tok, s.clientIP(r), audit.ProtoREST); err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

// ---- self-service ----

type passwordView struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	// Scheme is the hash scheme (ARGON2ID, or a migrated SHA512-CRYPT
	// until the first login). Admin views only: it tells an operator how
	// far a migration got and means nothing to the account holder.
	Scheme    string     `json:"scheme,omitempty"`
	Comment   string     `json:"comment"`
	ExpiresAt *time.Time `json:"expires_at"`
	Created   time.Time  `json:"created"`
}

// passwordViews lists passwords; withScheme adds the hash scheme (admin).
func passwordViews(pw []identity.Password, withScheme bool) []passwordView {
	out := make([]passwordView, 0, len(pw))
	for _, p := range pw {
		v := passwordView{ID: p.ID, Kind: p.Kind, Comment: p.Comment, ExpiresAt: p.ExpiresAt, Created: p.Created}
		if withScheme {
			v.Scheme = cryptoauth.Scheme(p.Hash)
		}
		out = append(out, v)
	}
	return out
}

type meView struct {
	ID          uuid.UUID      `json:"id"`
	Username    string         `json:"username"`
	Name        string         `json:"name"`
	Services    []string       `json:"services"`
	MailboxID   uuid.UUID      `json:"mailbox_id"`
	Roles       []string       `json:"roles"`
	Credential  string         `json:"credential"`
	TOTPEnabled bool           `json:"totp_enabled"`
	Passwords   []passwordView `json:"passwords"`
	TokenID     string         `json:"token_id,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	a := actor.From(r.Context())
	out := meView{ID: a.AccountID, Username: a.Username, MailboxID: a.MailboxID, Roles: a.Roles, Credential: a.Credential, TokenID: a.TokenID, SessionID: a.SessionID, Passwords: []passwordView{}}
	if out.Roles == nil {
		out.Roles = []string{}
	}
	if s.cfg.Identity != nil {
		v, err := s.cfg.Identity.GetAccount(r.Context(), a.AccountID)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		out.Name = v.Value.Name
		out.Services = v.Value.Services
		if out.Services == nil {
			out.Services = []string{}
		}
		out.TOTPEnabled = v.Value.TOTP != nil && v.Value.TOTP.Confirmed
		out.Passwords = passwordViews(v.Value.Passwords, false)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// mePatch changes what an account may change about itself: the display
// name and the services it may log in to (a user who turns JMAP off here
// locks themselves out of this API; an admin can turn it back on).
func (s *Server) mePatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     *string  `json:"name"`
		Services []string `json:"services"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	if req.Name == nil && req.Services == nil {
		s.fail(w, r, badRequest("name or services is required"))
		return
	}
	if req.Name != nil {
		if s.cfg.Submit == nil {
			s.fail(w, r, badRequest("not available on this listener"))
			return
		}
		if _, err := s.cfg.Submit.SetName(r.Context(), *req.Name); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if req.Services != nil {
		if err := s.auth.SetServices(r.Context(), actor.From(r.Context()).AccountID, req.Services); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	s.me(w, r)
}

type sessionView struct {
	ID        string    `json:"id"`
	Client    string    `json:"client"`
	Roles     []string  `json:"roles"`
	Created   time.Time `json:"created"`
	LastUsed  time.Time `json:"last_used"`
	LastIP    string    `json:"last_ip,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

func sessionViews(in []session.Session) []sessionView {
	out := make([]sessionView, 0, len(in))
	for _, x := range in {
		out = append(out, sessionView{ID: x.ID, Client: x.Client, Roles: x.Roles, Created: x.Created, LastUsed: x.LastUsed, LastIP: x.LastIP, ExpiresAt: x.ExpiresAt})
	}
	return out
}

func (s *Server) meSessions(w http.ResponseWriter, r *http.Request) {
	list, err := s.auth.ListSessions(r.Context(), actor.From(r.Context()).AccountID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": sessionViews(list)})
}

func (s *Server) meRevokeSession(w http.ResponseWriter, r *http.Request) {
	if err := s.auth.RevokeSession(r.Context(), actor.From(r.Context()).AccountID, r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type passwordRequest struct {
	Password string `json:"password"`
	// Hash: an existing hash in the form another platform stores it
	// (migrations); exactly one of password and hash. Admin only, primary only.
	Hash      string     `json:"hash,omitempty"`
	Kind      string     `json:"kind,omitempty"`
	Comment   string     `json:"comment,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // nil = never
}

func (s *Server) mePassword(w http.ResponseWriter, r *http.Request) {
	var req passwordRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	comment := req.Comment
	if comment == "" {
		comment = "set by user"
	}
	id, err := s.auth.SetPassword(r.Context(), actor.From(r.Context()).AccountID, req.Password, identity.PasswordPrimary, comment, req.ExpiresAt)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "kind": identity.PasswordPrimary, "comment": comment, "expires_at": req.ExpiresAt})
}

func (s *Server) meAppPassword(w http.ResponseWriter, r *http.Request) {
	var req passwordRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	id, err := s.auth.SetPassword(r.Context(), actor.From(r.Context()).AccountID, req.Password, identity.PasswordApplication, req.Comment, req.ExpiresAt)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"id": id, "kind": identity.PasswordApplication, "comment": req.Comment, "expires_at": req.ExpiresAt})
}

func (s *Server) meDeletePassword(w http.ResponseWriter, r *http.Request) {
	if err := s.auth.RemovePassword(r.Context(), actor.From(r.Context()).AccountID, r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) meTOTPBegin(w http.ResponseWriter, r *http.Request) {
	enr, err := s.auth.BeginTOTP(r.Context(), actor.From(r.Context()).AccountID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"secret": enr.Secret, "uri": enr.URI, "recovery_codes": enr.RecoveryCodes})
}

func (s *Server) meTOTPConfirm(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Code string `json:"code"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.auth.ConfirmTOTP(r.Context(), actor.From(r.Context()).AccountID, req.Code); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) meTOTPDisable(w http.ResponseWriter, r *http.Request) {
	if err := s.auth.DisableTOTP(r.Context(), actor.From(r.Context()).AccountID); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: status ----

func (s *Server) adminStatus(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"version": Version, "time": time.Now().UTC()})
}

// ---- admin: domains ----

type domainRequest struct {
	Name     string            `json:"name,omitempty"`
	Enabled  *bool             `json:"enabled,omitempty"`
	Settings map[string]string `json:"settings,omitempty"`
}

func (s *Server) domainCreate(w http.ResponseWriter, r *http.Request) {
	var req domainRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	d := identity.Domain{Name: req.Name, Enabled: true, Settings: req.Settings}
	if req.Enabled != nil {
		d.Enabled = *req.Enabled
	}
	out, err := s.admin.CreateDomain(r.Context(), d)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, out)
}

func (s *Server) domainList(w http.ResponseWriter, r *http.Request) {
	names, more, err := s.admin.ListDomains(r.Context(), r.URL.Query().Get("after"), limitParam(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listOut(names, more))
}

func (s *Server) domainGet(w http.ResponseWriter, r *http.Request) {
	out, err := s.admin.GetDomain(r.Context(), r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) domainPatch(w http.ResponseWriter, r *http.Request) {
	var req domainRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	out, err := s.admin.UpdateDomain(r.Context(), r.PathValue("name"), func(d *identity.Domain) error {
		if req.Enabled != nil {
			d.Enabled = *req.Enabled
		}
		if req.Settings != nil {
			d.Settings = req.Settings
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) domainDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.admin.DeleteDomain(r.Context(), r.PathValue("name")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: mailboxes ----

type mailboxRequest struct {
	// State: active | suspended (LMTP 4xx, MTA queues) | disabled (LMTP 5xx).
	// A reason is required for suspended and disabled.
	State       *string               `json:"state,omitempty"`
	StateReason string                `json:"state_reason,omitempty"`
	Quota       *identity.QuotaLimits `json:"quota,omitempty"`
	// StorageID pins the Ceph storage on create (default: random among
	// storage entries open for placement). Not changeable afterwards.
	StorageID int64 `json:"storage_id,omitempty"`
}

func (s *Server) mailboxCreate(w http.ResponseWriter, r *http.Request) {
	var req mailboxRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
			s.fail(w, r, err)
			return
		}
	}
	m := identity.Mailbox{State: identity.StateActive, StorageID: req.StorageID}
	if req.State != nil {
		m.State, m.StateReason = *req.State, req.StateReason
	}
	if req.Quota != nil {
		m.Quota = *req.Quota
	}
	out, err := s.admin.CreateMailbox(r.Context(), m)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, out)
}

func (s *Server) mailboxGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	info, err := s.admin.InspectMailbox(r.Context(), id, s.cfg.DefaultQuota)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, info)
}

func (s *Server) mailboxPatch(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req mailboxRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	if req.StorageID != 0 {
		s.fail(w, r, badRequest("storage_id cannot be changed (mailbox moves are a worker task)"))
		return
	}
	var out identity.Mailbox
	if req.Quota != nil {
		out, err = s.admin.UpdateMailbox(r.Context(), id, func(m *identity.Mailbox) error {
			m.Quota = *req.Quota
			return nil
		})
		if err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if req.State != nil {
		out, err = s.admin.SetMailboxState(r.Context(), id, *req.State, req.StateReason)
		if err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if req.Quota == nil && req.State == nil {
		if out, err = s.admin.GetMailbox(r.Context(), id); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) mailboxDelete(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.admin.DeleteMailbox(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// mailboxPurge schedules the removal of everything the account holder
// already deleted (the retention set), ahead of the retention period.
func (s *Server) mailboxPurge(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	res, err := s.admin.PurgeMailbox(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, res)
}

func (s *Server) mailboxRecalculate(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	usage, err := s.admin.RecalculateQuota(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"usage": usage})
}

type holdRequest struct {
	Reason         string `json:"reason"`
	CaseRef        string `json:"case_ref,omitempty"`
	AcceptDelivery *bool  `json:"accept_delivery,omitempty"`
	AllowSend      bool   `json:"allow_send,omitempty"`
	Sealed         bool   `json:"sealed,omitempty"`
}

func (s *Server) mailboxHold(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req holdRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	out, err := s.admin.Hold(r.Context(), id, admin.HoldRequest{Reason: req.Reason, CaseRef: req.CaseRef, AcceptDelivery: req.AcceptDelivery, AllowSend: req.AllowSend, Sealed: req.Sealed})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) mailboxRelease(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req struct {
		Note string `json:"note"`
	}
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
			s.fail(w, r, err)
			return
		}
	}
	out, err := s.admin.Release(r.Context(), id, req.Note)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) holdList(w http.ResponseWriter, r *http.Request) {
	list, err := s.admin.ListHolds(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if list == nil {
		list = []identity.Mailbox{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": list})
}

// ---- admin: accounts ----

type accountView struct {
	ID          uuid.UUID      `json:"id"`
	Username    string         `json:"username"`
	Name        string         `json:"name"`
	Domain      string         `json:"domain"`
	MailboxID   uuid.UUID      `json:"mailbox_id"`
	Roles       []string       `json:"roles"`
	Services    []string       `json:"services"`
	Enabled     bool           `json:"enabled"`
	TOTPEnabled bool           `json:"totp_enabled"`
	Passwords   []passwordView `json:"passwords"`
	Created     time.Time      `json:"created"`
	Updated     time.Time      `json:"updated"`
}

func viewAccount(a identity.Account) accountView {
	roles := a.Roles
	if roles == nil {
		roles = []string{}
	}
	services := a.Services
	if services == nil {
		services = []string{}
	}
	return accountView{
		ID: a.ID, Username: a.Username, Name: a.Name, Domain: a.Domain, MailboxID: a.MailboxID, Roles: roles, Services: services, Enabled: a.Enabled,
		TOTPEnabled: a.TOTP != nil && a.TOTP.Confirmed, Passwords: passwordViews(a.Passwords, true), Created: a.Created, Updated: a.Updated,
	}
}

type accountRequest struct {
	Username          string     `json:"username,omitempty"`
	Name              *string    `json:"name,omitempty"`
	MailboxID         *uuid.UUID `json:"mailbox_id,omitempty"`
	Roles             []string   `json:"roles,omitempty"`
	Services          []string   `json:"services,omitempty"` // nil = unchanged (PATCH) or all (create)
	Password          string     `json:"password,omitempty"`
	PasswordComment   string     `json:"password_comment,omitempty"`
	PasswordExpiresAt *time.Time `json:"password_expires_at,omitempty"`
	Enabled           *bool      `json:"enabled,omitempty"`
}

func (s *Server) accountCreate(w http.ResponseWriter, r *http.Request) {
	var req accountRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	cr := admin.CreateAccountRequest{Username: req.Username, Roles: req.Roles, Services: req.Services, Password: req.Password, PasswordComment: req.PasswordComment, PasswordExpiresAt: req.PasswordExpiresAt, Enabled: req.Enabled}
	if req.Name != nil {
		cr.Name = *req.Name
	}
	if req.MailboxID != nil {
		cr.MailboxID = *req.MailboxID
	}
	out, err := s.admin.CreateAccount(r.Context(), cr)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, viewAccount(out))
}

// mailboxList lists every mailbox with its accounts, enabled flag and hold
// state. RADOS has no ordered listing, so this walks the meta pool; it is an
// operator tool, not a hot path.
func (s *Server) mailboxList(w http.ResponseWriter, r *http.Request) {
	type item struct {
		ID        uuid.UUID            `json:"id"`
		StorageID int64                `json:"storage_id"`
		State     string               `json:"state"`
		Reason    string               `json:"state_reason,omitempty"`
		OnHold    bool                 `json:"on_hold"`
		Quota     identity.QuotaLimits `json:"quota"`
		Accounts  []string             `json:"accounts"`
		Created   time.Time            `json:"created"`
	}
	orphaned := r.URL.Query().Get("orphaned") == "true"
	items := []item{}
	err := s.admin.ListMailboxes(r.Context(), func(id uuid.UUID) error {
		m, err := s.admin.GetMailbox(r.Context(), id)
		if err != nil {
			return nil
		}
		accts, _, _ := s.admin.ListMailboxAccounts(r.Context(), id)
		if accts == nil {
			accts = []string{}
		}
		if orphaned && len(accts) > 0 {
			return nil
		}
		items = append(items, item{ID: id, StorageID: m.StorageID, State: m.State, Reason: m.StateReason, OnHold: m.Hold != nil, Quota: m.Quota, Accounts: accts, Created: m.Created})
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Created.Before(items[j].Created) })
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) accountList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("domain") == "" {
		s.fail(w, r, badRequest("domain query parameter is required"))
		return
	}
	names, more, err := s.admin.ListAccounts(r.Context(), q.Get("domain"), q.Get("prefix"), q.Get("after"), limitParam(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listOut(names, more))
}

// loadAccount resolves {id} as a uuid or a username.
func (s *Server) loadAccount(r *http.Request) (identity.Account, error) {
	v := r.PathValue("id")
	if id, err := uuid.Parse(v); err == nil {
		return s.admin.GetAccount(r.Context(), id)
	}
	return s.admin.GetAccountByUsername(r.Context(), v)
}

func (s *Server) accountGet(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, viewAccount(a))
}

func (s *Server) accountPatch(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req accountRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	if req.Username != "" || req.Password != "" {
		s.fail(w, r, badRequest("username and password cannot be changed here"))
		return
	}
	out := a
	if req.Roles != nil || req.Enabled != nil || req.Name != nil || req.Services != nil {
		out, err = s.admin.UpdateAccount(r.Context(), a.ID, func(x *identity.Account) error {
			if req.Roles != nil {
				x.Roles = req.Roles
			}
			if req.Services != nil {
				x.Services = req.Services
			}
			if req.Enabled != nil {
				x.Enabled = *req.Enabled
			}
			if req.Name != nil {
				x.Name = strings.TrimSpace(*req.Name)
			}
			return nil
		})
		if err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if req.MailboxID != nil && *req.MailboxID != out.MailboxID {
		out, err = s.admin.RelinkAccount(r.Context(), a.ID, *req.MailboxID)
		if err != nil {
			s.fail(w, r, err)
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, viewAccount(out))
}

func (s *Server) accountDelete(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.admin.DeleteAccount(r.Context(), a.ID); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) accountPasswordList(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": passwordViews(a.Passwords, true)})
}

// accountPasswordPatch changes expiry (expires_at: time or null = never)
// and optionally the comment of one password.
func (s *Server) accountPasswordPatch(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req struct {
		ExpiresAt *time.Time `json:"expires_at"`
		Comment   string     `json:"comment"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	p, err := s.auth.UpdatePassword(r.Context(), a.ID, r.PathValue("pid"), req.ExpiresAt, req.Comment)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, passwordViews([]identity.Password{p}, true)[0])
}

func (s *Server) accountPasswordDelete(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.auth.RemovePassword(r.Context(), a.ID, r.PathValue("pid")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) accountPassword(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req passwordRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	if (req.Password == "") == (req.Hash == "") {
		s.fail(w, r, badRequest("exactly one of password and hash is required"))
		return
	}
	kind := req.Kind
	if kind == "" {
		kind = identity.PasswordPrimary
	}
	var id string
	var err2 error
	if req.Hash != "" {
		id, err2 = s.auth.SetPasswordHash(r.Context(), a.ID, req.Hash, kind, req.Comment, req.ExpiresAt)
	} else {
		id, err2 = s.auth.SetPassword(r.Context(), a.ID, req.Password, req.Kind, req.Comment, req.ExpiresAt)
	}
	if err2 != nil {
		s.fail(w, r, err2)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "kind": kind, "comment": req.Comment, "expires_at": req.ExpiresAt})
}

func (s *Server) accountTOTPDisable(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.auth.DisableTOTP(r.Context(), a.ID); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) accountSessions(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	list, err := s.auth.ListSessions(r.Context(), a.ID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": sessionViews(list)})
}

func (s *Server) accountRevokeSession(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.auth.RevokeSession(r.Context(), a.ID, r.PathValue("sid")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: tokens ----

type tokenCreateRequest struct {
	Roles     []string  `json:"roles,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
}

func (s *Server) tokenCreate(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req tokenCreateRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	t, secret, err := s.admin.CreateToken(r.Context(), a.ID, req.Roles, req.ExpiresAt, req.Comment)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"token": viewToken(t), "secret": secret})
}

type tokenView struct {
	ID        string    `json:"id"`
	AccountID uuid.UUID `json:"account_id"`
	Roles     []string  `json:"roles"`
	ExpiresAt time.Time `json:"expires_at"`
	Comment   string    `json:"comment"`
	Created   time.Time `json:"created"`
	LastUsed  time.Time `json:"last_used,omitempty"`
}

func viewToken(t identity.Token) tokenView {
	return tokenView{ID: t.ID, AccountID: t.AccountID, Roles: t.Roles, ExpiresAt: t.ExpiresAt, Comment: t.Comment, Created: t.Created, LastUsed: t.LastUsed}
}

func (s *Server) tokenList(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	list, err := s.admin.ListTokens(r.Context(), a.ID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := make([]tokenView, 0, len(list))
	for _, t := range list {
		out = append(out, viewToken(t))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) tokenDelete(w http.ResponseWriter, r *http.Request) {
	a, err := s.loadAccount(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.admin.DeleteToken(r.Context(), a.ID, r.PathValue("tid")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: aliases ----

type aliasRequest struct {
	Address      string                 `json:"address,omitempty"`
	Enabled      *bool                  `json:"enabled,omitempty"`
	Destinations []identity.Destination `json:"destinations,omitempty"`
}

func (s *Server) aliasCreate(w http.ResponseWriter, r *http.Request) {
	var req aliasRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	al := identity.Alias{Address: req.Address, Enabled: true, Destinations: req.Destinations}
	if req.Enabled != nil {
		al.Enabled = *req.Enabled
	}
	out, err := s.admin.CreateAlias(r.Context(), al)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, out)
}

func (s *Server) aliasList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("domain") == "" {
		s.fail(w, r, badRequest("domain query parameter is required"))
		return
	}
	addrs, more, err := s.admin.ListAliases(r.Context(), q.Get("domain"), q.Get("after"), limitParam(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listOut(addrs, more))
}

func (s *Server) aliasGet(w http.ResponseWriter, r *http.Request) {
	out, err := s.admin.GetAlias(r.Context(), r.PathValue("address"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) aliasPatch(w http.ResponseWriter, r *http.Request) {
	var req aliasRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	out, err := s.admin.UpdateAlias(r.Context(), r.PathValue("address"), func(al *identity.Alias) error {
		if req.Enabled != nil {
			al.Enabled = *req.Enabled
		}
		if req.Destinations != nil {
			al.Destinations = req.Destinations
		}
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) aliasDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.admin.DeleteAlias(r.Context(), r.PathValue("address")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: audit ----

func (s *Server) auditRead(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	aq := audit.Query{Account: q.Get("account"), Action: q.Get("action"), ClientIP: q.Get("client_ip"), After: q.Get("after"), Max: limitParam(r)}
	if m := q.Get("mailbox"); m != "" {
		id, err := uuid.Parse(m)
		if err != nil {
			s.fail(w, r, badRequest("invalid mailbox: not a uuid"))
			return
		}
		aq.Mailbox = id
	}
	for _, p := range []struct {
		name string
		dst  *time.Time
	}{{"from", &aq.From}, {"to", &aq.To}} {
		if v := q.Get(p.name); v != "" {
			t, err := time.Parse("2006-01-02", v)
			if err != nil {
				s.fail(w, r, badRequest("invalid "+p.name+": want YYYY-MM-DD"))
				return
			}
			*p.dst = t
		}
	}
	recs, next, err := s.audit.Read(r.Context(), aq)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if recs == nil {
		recs = []audit.Record{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": recs, "next": next})
}

// ---- admin: queues ----

func (s *Server) queueStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.queue.Stats(r.Context(), r.PathValue("type"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"type": r.PathValue("type"), "pending": st.Pending, "leased": st.Leased, "dead": st.Dead})
}

func (s *Server) queueDead(w http.ResponseWriter, r *http.Request) {
	list, err := s.queue.Dead(r.Context(), r.PathValue("type"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if list == nil {
		list = []queue.Task{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": list})
}

func (s *Server) queueRetry(w http.ResponseWriter, r *http.Request) {
	if err := s.queue.Retry(r.Context(), r.PathValue("type"), r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: keys, export/import ----

func (s *Server) keysRotate(w http.ResponseWriter, r *http.Request) {
	if err := s.auth.RotateKeys(r.Context()); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) exportIdentity(w http.ResponseWriter, r *http.Request) {
	out, err := s.admin.ExportIdentity(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) importIdentity(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	summary, err := s.admin.ImportIdentity(r.Context(), data)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"summary": summary})
}

// ensure context import is used for interface satisfaction in tests.
var _ = context.Background

// ---- admin: storage ----

type storageRequest struct {
	Name      string          `json:"name,omitempty"`
	MonHost   string          `json:"mon_host,omitempty"`
	User      string          `json:"user,omitempty"`
	Key       string          `json:"key,omitempty"`
	Pools     *identity.Pools `json:"pools,omitempty"`
	Placement *bool           `json:"placement,omitempty"`
}

func (r storageRequest) toAdmin() admin.StorageRequest {
	out := admin.StorageRequest{Name: r.Name, MonHost: r.MonHost, User: r.User, Key: r.Key, Placement: r.Placement}
	if r.Pools != nil {
		out.Pools = *r.Pools
	}
	return out
}

// storageView never shows the key.
type storageView struct {
	ID        int64          `json:"id"`
	FSID      uuid.UUID      `json:"fsid"`
	Name      string         `json:"name"`
	MonHost   string         `json:"mon_host"`
	User      string         `json:"user"`
	Pools     identity.Pools `json:"pools"`
	Placement bool           `json:"placement"`
	Created   time.Time      `json:"created"`
	Updated   time.Time      `json:"updated"`
}

func viewStorage(c identity.Storage) storageView {
	return storageView{ID: c.ID, FSID: c.FSID, Name: c.Name, MonHost: c.MonHost, User: c.User, Pools: c.Pools, Placement: c.Placement, Created: c.Created, Updated: c.Updated}
}

func pathStorageID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("sid"), 10, 64)
	if err != nil || id <= 0 {
		return 0, badRequest("storage id must be a positive integer")
	}
	return id, nil
}

func (s *Server) storageAdd(w http.ResponseWriter, r *http.Request) {
	var req storageRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	out, err := s.admin.AddStorage(r.Context(), req.toAdmin())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, viewStorage(out))
}

func (s *Server) storageList(w http.ResponseWriter, r *http.Request) {
	list, err := s.admin.ListStorage(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := make([]storageView, 0, len(list))
	for _, c := range list {
		out = append(out, viewStorage(c))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) storageGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathStorageID(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	c, err := s.admin.GetStorage(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, viewStorage(c))
}

func (s *Server) storagePatch(w http.ResponseWriter, r *http.Request) {
	id, err := pathStorageID(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req storageRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	out, err := s.admin.UpdateStorage(r.Context(), id, req.toAdmin())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, viewStorage(out))
}

func (s *Server) storageDelete(w http.ResponseWriter, r *http.Request) {
	id, err := pathStorageID(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.admin.DeleteStorage(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin: nodes ----

type nodeView struct {
	ID       int64     `json:"id"`
	Mode     string    `json:"mode"`
	Address  string    `json:"address"`
	Host     string    `json:"host"`
	Version  string    `json:"version"`
	Started  time.Time `json:"started"`
	LastSeen time.Time `json:"last_seen"`
	Alive    bool      `json:"alive"`
}

func (s *Server) nodeList(w http.ResponseWriter, r *http.Request) {
	list, err := s.admin.ListNodes(r.Context(), r.URL.Query().Get("mode"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	now := time.Now()
	out := make([]nodeView, 0, len(list))
	for _, n := range list {
		out = append(out, nodeView{ID: n.ID, Mode: n.Mode, Address: n.Address, Host: n.Host, Version: n.Version, Started: n.Started, LastSeen: n.LastSeen, Alive: n.Alive(now)})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}
