// Package proxy is the proxy mode: IMAP, POP3, SMTP submission and JMAP
// front ends that read a client's login name, find the namespace of its
// domain and hand the connection to a server of that namespace, so that
// the whole platform looks like one cluster from outside while every
// namespace's servers may sit in their own datacenter. A proxy process
// holds no mail state and never talks to Ceph: it reads namespaces,
// domains, nodes and tokens from the database, read-only, and nothing
// else. Credentials pass through untouched; the backend verifies them,
// counts failures and writes the audit record with the real client
// address, which the proxy forwards with PROXY protocol v2. See
// README.md "Proxies".
package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/metrics"
	"kittiwake/store/identity"
)

// Sentinel errors of routing.
var (
	// ErrUnknownDomain: the login name's domain is not hosted here.
	ErrUnknownDomain = errors.New("proxy: unknown domain")
	// ErrDisabled: the domain or its namespace is disabled.
	ErrDisabled = errors.New("proxy: disabled")
	// ErrNoBackend: no live server of the namespace runs the protocol.
	ErrNoBackend = errors.New("proxy: no backend available")
	// ErrBadCredentialFormat: the credential carries no routable name.
	ErrBadCredentialFormat = errors.New("proxy: no routable login name")
)

// Protocols a proxy fronts; the names double as the node registry mode
// the backend runs and as the metrics label.
const (
	ProtoIMAP       = "imap"
	ProtoPOP3       = "pop3"
	ProtoJMAP       = "jmap"
	ProtoSubmission = "submission"
)

// Config of a proxy.
type Config struct {
	// Hostname in greetings.
	Hostname string
	// ConnectTimeout bounds one backend connection attempt.
	ConnectTimeout time.Duration
	// PreAuthTimeout bounds how long a client may take to log in.
	PreAuthTimeout time.Duration
	// NodeTTL: how long the node registry listing is reused.
	NodeTTL time.Duration
	// DownFor: how long a backend that refused a connection is skipped.
	DownFor time.Duration
	// IngressTLS is the policy for ingress hosts whose namespace sets
	// none: strict, insecure or off.
	IngressTLS string
	// IngressPorts: the port per protocol on an ingress host.
	IngressPorts map[string]int
	// MaxMessageSize is advertised to submission clients (SIZE).
	MaxMessageSize int64
	// RequireTLSProxy offers submission AUTH only when the PROXY header
	// says the client spoke TLS to the TLS terminator in front.
	RequireTLSProxy bool
	// TrustedProxies may set X-Forwarded-For on HTTP (the TLS terminator
	// in front, when it does not send PROXY protocol).
	TrustedProxies []net.IPNet
}

// Server is one proxy process: every protocol front end over one router.
type Server struct {
	cfg    Config
	ids    *identity.Store
	log    *slog.Logger
	dial   *dialer
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// New returns a proxy over the identity store (read-only use).
func New(cfg Config, ids *identity.Store, log *slog.Logger) *Server {
	if cfg.Hostname == "" {
		cfg.Hostname = "kittiwake"
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.PreAuthTimeout <= 0 {
		cfg.PreAuthTimeout = time.Minute
	}
	if cfg.NodeTTL <= 0 {
		cfg.NodeTTL = 5 * time.Second
	}
	if cfg.DownFor <= 0 {
		cfg.DownFor = 15 * time.Second
	}
	if cfg.IngressTLS == "" {
		cfg.IngressTLS = identity.IngressTLSStrict
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 50 << 20
	}
	ports := map[string]int{ProtoIMAP: 993, ProtoPOP3: 995, ProtoJMAP: 443, ProtoSubmission: 465}
	for k, v := range cfg.IngressPorts {
		if v > 0 {
			ports[k] = v
		}
	}
	cfg.IngressPorts = ports
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, ids: ids, log: log, conns: map[net.Conn]struct{}{}}
	s.dial = newDialer(cfg, ids, log)
	return s
}

// Target is where a login name's mailbox is served.
type Target struct {
	Namespace string
	Ingress   *identity.Ingress // nil: nodes from the registry
}

// Route resolves a login name (user@domain) to its namespace. Only the
// domain is read; the account itself is the backend's business.
func (s *Server) Route(ctx context.Context, username string) (Target, error) {
	_, domain, ok := strings.Cut(strings.TrimSpace(username), "@")
	if !ok || domain == "" {
		return Target{}, ErrBadCredentialFormat
	}
	return s.routeDomain(ctx, domain)
}

// RouteAccount resolves an account id (from a token) to its namespace.
func (s *Server) RouteAccount(ctx context.Context, id uuid.UUID) (Target, error) {
	domain, err := s.ids.AccountDomainCached(ctx, id)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return Target{}, ErrUnknownDomain
		}
		return Target{}, err
	}
	return s.routeDomain(ctx, domain)
}

func (s *Server) routeDomain(ctx context.Context, domain string) (Target, error) {
	dom, err := s.ids.GetDomainCached(ctx, domain)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) || errors.Is(err, identity.ErrInvalid) {
			return Target{}, ErrUnknownDomain
		}
		return Target{}, err
	}
	if !dom.Enabled {
		return Target{}, ErrDisabled
	}
	ns, err := s.ids.GetNamespaceCached(ctx, dom.Namespace)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return Target{}, ErrDisabled
		}
		return Target{}, err
	}
	if !ns.Enabled {
		return Target{}, ErrDisabled
	}
	return Target{Namespace: ns.Name, Ingress: ns.Ingress}, nil
}

// client describes the connection being proxied, for the PROXY header
// and the log.
type client struct {
	remote net.Addr
	local  net.Addr
	tls    bool
	ip     string
}

// connect opens the backend connection for a target: a node of the
// namespace from the registry, or the namespace's ingress.
func (s *Server) connect(ctx context.Context, proto string, t Target, username string, c client) (net.Conn, error) {
	return s.dial.dial(ctx, proto, t, username, c)
}

// track registers a client connection for Shutdown.
func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Shutdown closes every proxied connection.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return nil
}

// serveTCP accepts connections for a TCP front end.
func (s *Server) serveTCP(l net.Listener, proto string, handle func(net.Conn)) error {
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		if !s.track(c) {
			_ = c.Close()
			return nil
		}
		metrics.Connections.WithLabelValues("proxy-" + proto).Inc()
		go func() {
			defer func() {
				metrics.Connections.WithLabelValues("proxy-" + proto).Dec()
				s.untrack(c)
				_ = c.Close()
			}()
			handle(c)
		}()
	}
}

// result records the outcome of one proxied login in metrics and the log.
func (s *Server) result(proto, outcome, username string, c client, t Target, err error) {
	metrics.ProxySessions.WithLabelValues(proto, outcome).Inc()
	attrs := []any{slog.String("protocol", proto), slog.String("client", c.ip), slog.String("username", username), slog.String("result", outcome)}
	if t.Namespace != "" {
		attrs = append(attrs, slog.String("namespace", t.Namespace))
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	if outcome == "routed" {
		s.log.Info("proxy: session routed", attrs...)
		return
	}
	s.log.Warn("proxy: login refused", attrs...)
}

func clientOf(c net.Conn) client {
	cl := client{remote: c.RemoteAddr(), local: c.LocalAddr(), tls: tlsOf(c)}
	cl.ip = ipOf(cl.remote)
	return cl
}

func fmtErr(prefix string, err error) string { return fmt.Sprintf("%s: %v", prefix, err) }
