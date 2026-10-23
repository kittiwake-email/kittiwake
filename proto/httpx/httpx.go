// Package httpx holds HTTP plumbing shared by JMAP and the REST endpoints:
// real client IP (PROXY protocol or trusted X-Forwarded-For), bearer/basic
// authentication into an actor, and the JSON problem error format.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"

	"kittiwake/proto/proxyproto"
	"kittiwake/service/actor"
	authsvc "kittiwake/service/auth"
)

type ctxKey int

const (
	ctxConnIP ctxKey = iota
)

// ConnContext is for http.Server.ConnContext: it records the connection's
// real remote IP (after PROXY protocol) in the request context.
func ConnContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, ctxConnIP, proxyproto.IPOf(c.RemoteAddr()))
}

// ClientIP returns the client IP for a request: the connection address
// (which the PROXY listener already fixed up), or X-Forwarded-For when the
// connection comes from a trusted proxy.
func ClientIP(r *http.Request, trusted []net.IPNet) string {
	ip, _ := r.Context().Value(ctxConnIP).(string)
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
		if ip == "" {
			ip = r.RemoteAddr
		}
	}
	if len(trusted) == 0 {
		return ip
	}
	pip := net.ParseIP(ip)
	if pip == nil {
		return ip
	}
	for _, n := range trusted {
		if n.Contains(pip) {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				first := strings.TrimSpace(strings.Split(xff, ",")[0])
				if net.ParseIP(first) != nil {
					return first
				}
			}
			break
		}
	}
	return ip
}

// ParseTrusted parses CIDRs or bare IPs.
func ParseTrusted(list []string) ([]net.IPNet, error) {
	var out []net.IPNet
	for _, s := range list {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if !strings.Contains(s, "/") {
			ip := net.ParseIP(s)
			if ip == nil {
				return nil, errors.New("httpx: bad trusted address " + s)
			}
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			out = append(out, net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			return nil, err
		}
		out = append(out, *n)
	}
	return out, nil
}

// Problem is the JSON error body (RFC 9457 style).
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// WriteProblem writes a problem response.
func WriteProblem(w http.ResponseWriter, status int, typ, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Problem{Type: typ, Title: http.StatusText(status), Status: status, Detail: detail})
}

// WriteJSON writes a JSON response.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Authenticator turns credentials into an actor.
type Authenticator interface {
	VerifyBearer(ctx context.Context, token string) (actor.Actor, error)
	VerifyPassword(ctx context.Context, username, password, proto, clientIP string) (actor.Actor, error)
}

// Auth returns middleware that authenticates Bearer tokens (session access
// tokens or API tokens) and HTTP basic auth, puts the actor in the context
// and rejects unauthenticated requests with 401. protocol is recorded on
// the actor ("jmap", "rest", "admin").
func Auth(a Authenticator, protocol string, trusted []net.IPNet, realm string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := ClientIP(r, trusted)
			h := r.Header.Get("Authorization")
			var act actor.Actor
			var err error
			switch {
			case strings.HasPrefix(strings.ToLower(h), "bearer "):
				act, err = a.VerifyBearer(r.Context(), strings.TrimSpace(h[7:]))
			case strings.HasPrefix(strings.ToLower(h), "basic "):
				user, pass, ok := r.BasicAuth()
				if !ok {
					err = authsvc.ErrInvalidCredentials
				} else {
					act, err = a.VerifyPassword(r.Context(), user, pass, protocol, ip)
				}
			default:
				w.Header().Set("WWW-Authenticate", `Bearer realm="`+realm+`", Basic realm="`+realm+`"`)
				WriteProblem(w, http.StatusUnauthorized, "about:blank", "authentication required")
				return
			}
			if err != nil {
				status, detail := http.StatusUnauthorized, "invalid credentials"
				switch {
				case errors.Is(err, authsvc.ErrTokenExpired):
					detail = "token expired"
				case errors.Is(err, authsvc.ErrTOTPRequired):
					detail = "this password requires a one-time code; use an application password or a session token"
				case errors.Is(err, authsvc.ErrAccountDisabled):
					status, detail = http.StatusForbidden, "account disabled"
				case errors.Is(err, authsvc.ErrServiceDisabled):
					status, detail = http.StatusForbidden, "this service is not enabled for the account"
				case errors.Is(err, authsvc.ErrInvalidCredentials), errors.Is(err, authsvc.ErrInvalidToken):
				default:
					status, detail = http.StatusInternalServerError, "authentication backend error"
				}
				if status == http.StatusUnauthorized {
					w.Header().Set("WWW-Authenticate", `Bearer realm="`+realm+`", error="invalid_token"`)
				}
				WriteProblem(w, status, "about:blank", detail)
				return
			}
			act.Protocol, act.ClientIP = protocol, ip
			if validator, ok := a.(interface {
				ValidateActor(context.Context, actor.Actor) error
			}); ok {
				if err := validator.ValidateActor(r.Context(), act); err != nil {
					WriteProblem(w, http.StatusForbidden, "about:blank", "account access revoked")
					return
				}
			}
			next.ServeHTTP(w, r.WithContext(actor.With(r.Context(), act)))
		})
	}
}

// RequireRole returns middleware that rejects actors without the role.
func RequireRole(role string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !actor.From(r.Context()).HasRole(role) {
				WriteProblem(w, http.StatusForbidden, "about:blank", "role "+role+" required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequireAnyRole returns middleware that rejects actors lacking all roles.
func RequireAnyRole(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			a := actor.From(r.Context())
			for _, role := range roles {
				if a.HasRole(role) {
					next.ServeHTTP(w, r)
					return
				}
			}
			WriteProblem(w, http.StatusForbidden, "about:blank", "one of roles "+strings.Join(roles, ", ")+" required")
		})
	}
}

// MaxBody limits request bodies.
func MaxBody(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}
