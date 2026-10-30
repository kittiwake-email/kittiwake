package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/internal/ttlcache"
	"kittiwake/proto/httpx"
	"kittiwake/store/identity"
)

// HTTP (JMAP, the OAuth endpoints and /v1/me) is reverse-proxied per
// request: the credential names the account (basic auth, a session JWT's
// usr claim, an API token, a refresh token), the account's domain names
// the namespace, and the request goes to a jmap node of that namespace
// or to its ingress. The client address travels in X-Forwarded-For (a
// pooled HTTP connection serves many clients, so PROXY protocol cannot
// carry it); backends trust the proxy's address for it.

type ctxKey int

const (
	ctxTarget ctxKey = iota
)

// httpTarget is what the dialer needs for one backend connection.
type httpTarget struct {
	addr   string            // node address or ingress host:port
	tlsFor *identity.Ingress // nil: plain
	policy string
}

// HTTPHandler returns the JMAP front end.
func (s *Server) HTTPHandler() http.Handler {
	h := &httpProxy{s: s, tokens: &ttlcache.Cache[uuid.UUID]{}}
	tr := &http.Transport{
		DialContext:           h.dial,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		ForceAttemptHTTP2:     false,
	}
	h.rp = &httputil.ReverseProxy{
		Rewrite:       h.rewrite,
		Transport:     tr,
		FlushInterval: -1, // EventSource streams
		ErrorHandler:  h.backendError,
		ErrorLog:      slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
	return h
}

type httpProxy struct {
	s      *Server
	rp     *httputil.ReverseProxy
	tokens *ttlcache.Cache[uuid.UUID] // API token hash -> account
}

func (h *httpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip := httpx.ClientIP(r, h.s.cfg.TrustedProxies)
	username, acct, body, err := h.principal(r)
	info := client{ip: ip}
	if err != nil {
		h.refuse(w, r, "", info, Target{}, err)
		return
	}
	var t Target
	switch {
	case username != "":
		t, err = h.s.Route(r.Context(), username)
	case acct != uuid.Nil:
		t, err = h.s.RouteAccount(r.Context(), acct)
		username = acct.String()
	}
	if err != nil {
		h.refuse(w, r, username, info, t, err)
		return
	}
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	ht, err := h.target(r.Context(), t, username)
	if err != nil {
		h.refuse(w, r, username, info, t, err)
		return
	}
	h.s.log.Debug("proxy: request routed", slog.String("protocol", ProtoJMAP), slog.String("client", ip), slog.String("username", username), slog.String("namespace", t.Namespace), slog.String("backend", ht.addr), slog.String("path", r.URL.Path))
	ctx := context.WithValue(r.Context(), ctxTarget, ht)
	ctx = context.WithValue(ctx, ctxClientIP, ip)
	h.rp.ServeHTTP(w, r.WithContext(ctx))
}

const ctxClientIP ctxKey = 1

// principal extracts who the request is for: a username (basic auth,
// JWT, password grant) or an account id (API token, refresh token). The
// body is returned when it had to be read (token endpoints).
func (h *httpProxy) principal(r *http.Request) (username string, acct uuid.UUID, body []byte, err error) {
	auth := r.Header.Get("Authorization")
	switch {
	case strings.HasPrefix(strings.ToLower(auth), "basic "):
		user, _, ok := r.BasicAuth()
		if !ok {
			return "", uuid.Nil, nil, errUnauthorized
		}
		return user, uuid.Nil, nil, nil
	case strings.HasPrefix(strings.ToLower(auth), "bearer "):
		u, id, err := h.bearer(r.Context(), strings.TrimSpace(auth[7:]))
		return u, id, nil, err
	}
	if r.Method == http.MethodPost && (r.URL.Path == "/v1/oauth/token" || r.URL.Path == "/v1/oauth/revoke") {
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil {
			return "", uuid.Nil, nil, errBadRequest
		}
		req := tokenFields(r.Header.Get("Content-Type"), body)
		switch {
		case req.username != "":
			return req.username, uuid.Nil, body, nil
		case req.refresh != "":
			id, ok := refreshAccount(req.refresh)
			if !ok {
				return "", uuid.Nil, body, errUnauthorized
			}
			return "", id, body, nil
		case req.token != "":
			if id, ok := refreshAccount(req.token); ok {
				return "", id, body, nil
			}
			u, id, err := h.bearer(r.Context(), req.token)
			return u, id, body, err

		}
		return "", uuid.Nil, body, errUnauthorized
	}
	return "", uuid.Nil, nil, errUnauthorized
}

var (
	errUnauthorized = errors.New("proxy: no credential")
	errBadRequest   = errors.New("proxy: malformed request")
)

// bearer resolves an API token (one indexed read, cached) or a session
// JWT (its usr claim, read without verification: the backend verifies).
func (h *httpProxy) bearer(ctx context.Context, token string) (string, uuid.UUID, error) {
	if strings.HasPrefix(token, identity.TokenPrefix) {
		key := identity.HashToken(token)
		now := time.Now()
		if id, err, ok := h.tokens.Get(key, now, h.s.ids.CacheTTL); ok {
			return "", id, err
		}
		v, err := h.s.ids.LookupToken(ctx, token)
		if err != nil {
			if errors.Is(err, identity.ErrNotFound) {
				h.tokens.Put(key, uuid.Nil, errUnauthorized, now)
				return "", uuid.Nil, errUnauthorized
			}
			return "", uuid.Nil, err
		}
		if v.Value.AccountID == uuid.Nil {
			// An admin token has no mailbox; the admin API is not proxied.
			h.tokens.Put(key, uuid.Nil, errUnauthorized, now)
			return "", uuid.Nil, errUnauthorized
		}
		h.tokens.Put(key, v.Value.AccountID, nil, now)
		return "", v.Value.AccountID, nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", uuid.Nil, errUnauthorized
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", uuid.Nil, errUnauthorized
	}
	var claims struct {
		Usr string `json:"usr"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Usr == "" {
		return "", uuid.Nil, errUnauthorized
	}
	return claims.Usr, uuid.Nil, nil
}

// refreshAccount reads the account id out of a refresh token
// ("kwr1.<session>.<account>.<secret>").
func refreshAccount(tok string) (uuid.UUID, bool) {
	parts := strings.SplitN(tok, ".", 4)
	if len(parts) != 4 || parts[0] != "kwr1" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(parts[2])
	return id, err == nil
}

type tokenReq struct{ username, refresh, token string }

// tokenFields reads the routing fields of a token or revoke request,
// form-encoded or JSON, as the backend does.
func tokenFields(contentType string, body []byte) tokenReq {
	if strings.HasPrefix(contentType, "application/json") {
		var j struct {
			Username     string `json:"username"`
			RefreshToken string `json:"refresh_token"`
			Token        string `json:"token"`
		}
		_ = json.Unmarshal(body, &j)
		return tokenReq{j.Username, j.RefreshToken, j.Token}
	}
	f, _ := url.ParseQuery(string(body))
	return tokenReq{f.Get("username"), f.Get("refresh_token"), f.Get("token")}
}

// target picks the backend for one request.
func (h *httpProxy) target(ctx context.Context, t Target, username string) (httpTarget, error) {
	if t.Ingress != nil {
		port := h.s.cfg.IngressPorts[ProtoJMAP]
		policy := t.Ingress.TLS
		if policy == "" {
			policy = h.s.cfg.IngressTLS
		}
		ht := httpTarget{addr: net.JoinHostPort(t.Ingress.Host, strconv.Itoa(port)), policy: policy}
		if policy != identity.IngressTLSOff {
			ht.tlsFor = t.Ingress
		}
		return ht, nil
	}
	addr, err := h.s.dial.pick(ctx, ProtoJMAP, t.Namespace, username)
	if err != nil {
		return httpTarget{}, err
	}
	return httpTarget{addr: addr}, nil
}

func (h *httpProxy) rewrite(pr *httputil.ProxyRequest) {
	ht, _ := pr.In.Context().Value(ctxTarget).(httpTarget)
	ip, _ := pr.In.Context().Value(ctxClientIP).(string)
	pr.Out.URL.Scheme = "http"
	// The pool is keyed by URL host; TLS to an ingress happens in dial.
	pr.Out.URL.Host = ht.addr
	if ht.tlsFor != nil {
		pr.Out.URL.Host = "tls+" + ht.addr
	}
	pr.Out.Host = pr.In.Host
	pr.Out.Header.Set("X-Forwarded-For", ip)
	proto := "http"
	if tlsOf(connOf(pr.In)) || strings.EqualFold(pr.In.Header.Get("X-Forwarded-Proto"), "https") {
		proto = "https"
	}
	pr.Out.Header.Set("X-Forwarded-Proto", proto)
	pr.Out.Header.Set("X-Forwarded-Host", pr.In.Host)
}

// dial opens the backend connection the transport asked for: addr is the
// pool key set by rewrite ("tls+host:port" for an ingress with TLS).
func (h *httpProxy) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	ht, _ := ctx.Value(ctxTarget).(httpTarget)
	useTLS := strings.HasPrefix(addr, "tls+")
	addr = strings.TrimPrefix(addr, "tls+")
	conn, err := h.s.dial.open(ctx, addr)
	if err != nil {
		if ht.tlsFor == nil {
			h.s.dial.markDown(addr)
		}
		return nil, err
	}
	if !useTLS || ht.tlsFor == nil {
		h.s.dial.markUp(addr)
		return conn, nil
	}
	tc := &tls.Config{ServerName: ht.tlsFor.Host, MinVersion: tls.VersionTLS12}
	if ht.policy == identity.IngressTLSInsecure {
		tc.InsecureSkipVerify = true //nolint:gosec // the namespace's operator chose it
	}
	tconn := tls.Client(conn, tc)
	if err := tconn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tconn, nil
}

func (h *httpProxy) backendError(w http.ResponseWriter, r *http.Request, err error) {
	ht, _ := r.Context().Value(ctxTarget).(httpTarget)
	h.s.log.Warn("proxy: backend request failed", slog.String("backend", ht.addr), slog.String("path", r.URL.Path), slog.String("error", err.Error()))
	httpx.WriteProblem(w, http.StatusBadGateway, "about:blank", "the server for this account did not answer, try again later")
}

// refuse answers a request the proxy cannot route, in the backend's
// vocabulary so clients behave the same either way.
func (h *httpProxy) refuse(w http.ResponseWriter, r *http.Request, username string, info client, t Target, err error) {
	switch {
	case errors.Is(err, errUnauthorized), errors.Is(err, ErrUnknownDomain), errors.Is(err, ErrBadCredentialFormat):
		w.Header().Set("WWW-Authenticate", `Bearer realm="kittiwake", Basic realm="kittiwake"`)
		if errors.Is(err, errUnauthorized) && username == "" {
			httpx.WriteProblem(w, http.StatusUnauthorized, "about:blank", "authentication required")
			return
		}
		h.s.result(ProtoJMAP, "unknown-domain", username, info, t, err)
		httpx.WriteProblem(w, http.StatusUnauthorized, "about:blank", "invalid credentials")
	case errors.Is(err, errBadRequest):
		httpx.WriteProblem(w, http.StatusBadRequest, "about:blank", "malformed request body")
	case errors.Is(err, ErrDisabled):
		h.s.result(ProtoJMAP, "disabled", username, info, t, err)
		httpx.WriteProblem(w, http.StatusForbidden, "about:blank", "account disabled")
	case errors.Is(err, ErrNoBackend):
		h.s.result(ProtoJMAP, "no-backend", username, info, t, err)
		httpx.WriteProblem(w, http.StatusServiceUnavailable, "about:blank", "no server available for this account, try again later")
	default:
		h.s.result(ProtoJMAP, "error", username, info, t, err)
		httpx.WriteProblem(w, http.StatusServiceUnavailable, "about:blank", "routing failed, try again later")
	}
}

// connOf returns the request's connection when the server recorded it
// (ConnContext); nil otherwise.
func connOf(r *http.Request) net.Conn {
	c, _ := r.Context().Value(ctxConn).(net.Conn)
	return c
}

const ctxConn ctxKey = 2

// ConnContext is for http.Server.ConnContext on the proxy's HTTP
// listener: it keeps the connection (for its PROXY TLS flag) and records
// the real client IP as httpx does.
func ConnContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(httpx.ConnContext(ctx, c), ctxConn, c)
}
