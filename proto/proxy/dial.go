package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"kittiwake/proto/proxyproto"
	"kittiwake/store/identity"
)

// dialer opens backend connections: from the node registry (a live node
// of the namespace running the protocol, plain TCP with a PROXY v2
// header) or through the namespace's ingress (the configured port, PROXY
// v2 first, then TLS per policy).
type dialer struct {
	cfg Config
	ids *identity.Store
	log *slog.Logger

	mu      sync.Mutex
	nodes   map[string]nodeCache // by mode
	down    map[string]time.Time // backend address -> skip until
	up      map[string]time.Time // backend address -> last successful connect
	Now     func() time.Time
	DialCtx func(ctx context.Context, network, addr string) (net.Conn, error)
}

type nodeCache struct {
	at    time.Time
	nodes []identity.Node
}

func newDialer(cfg Config, ids *identity.Store, log *slog.Logger) *dialer {
	d := &dialer{cfg: cfg, ids: ids, log: log, nodes: map[string]nodeCache{}, down: map[string]time.Time{}, up: map[string]time.Time{}, Now: time.Now}
	nd := &net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	d.DialCtx = nd.DialContext
	return d
}

// candidates lists the live nodes of a mode serving the namespace,
// ordered by rendezvous hash of the username so one user keeps reaching
// the same node while it lives (its hot index), others spread evenly.
func (d *dialer) candidates(ctx context.Context, mode, namespace, username string) ([]identity.Node, error) {
	now := d.Now()
	d.mu.Lock()
	c, ok := d.nodes[mode]
	d.mu.Unlock()
	if !ok || now.Sub(c.at) >= d.cfg.NodeTTL {
		list, err := d.ids.ListNodes(ctx, mode)
		if err != nil {
			if !ok {
				return nil, err
			}
			d.log.Warn("proxy: node registry unreadable, using the last listing", slog.String("error", err.Error()))
		} else {
			c = nodeCache{at: now, nodes: list}
			d.mu.Lock()
			d.nodes[mode] = c
			d.mu.Unlock()
		}
	}
	type scored struct {
		n     identity.Node
		score uint64
	}
	var out []scored
	d.mu.Lock()
	for _, n := range c.nodes {
		if !n.Alive(now) || !n.Serves(namespace) {
			continue
		}
		if until, bad := d.down[n.Address]; bad && now.Before(until) {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(username))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(n.Address))
		out = append(out, scored{n, h.Sum64()})
	}
	d.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].score > out[j].score })
	nodes := make([]identity.Node, len(out))
	for i := range out {
		nodes[i] = out[i].n
	}
	return nodes, nil
}

func (d *dialer) markDown(addr string) {
	d.mu.Lock()
	d.down[addr] = d.Now().Add(d.cfg.DownFor)
	delete(d.up, addr)
	d.mu.Unlock()
}

func (d *dialer) markUp(addr string) {
	d.mu.Lock()
	d.up[addr] = d.Now()
	d.mu.Unlock()
}

// pick returns the first candidate known to accept connections: one that
// connected within DownFor, else one that answers a probe now. HTTP
// requests cannot fail over inside the transport, so the choice is made
// before the request; the probe costs one connect per node per DownFor.
func (d *dialer) pick(ctx context.Context, mode, namespace, username string) (string, error) {
	nodes, err := d.candidates(ctx, mode, namespace, username)
	if err != nil {
		return "", err
	}
	for _, n := range nodes {
		d.mu.Lock()
		last, ok := d.up[n.Address]
		d.mu.Unlock()
		if ok && d.Now().Sub(last) < d.cfg.DownFor {
			return n.Address, nil
		}
		c, err := d.open(ctx, n.Address)
		if err != nil {
			d.markDown(n.Address)
			d.log.Warn("proxy: backend unreachable", slog.String("mode", mode), slog.String("address", n.Address), slog.String("error", err.Error()))
			continue
		}
		_ = c.Close()
		d.markUp(n.Address)
		return n.Address, nil
	}
	return "", ErrNoBackend
}

// dial connects to the target for proto and sends the PROXY v2 header
// announcing the client.
func (d *dialer) dial(ctx context.Context, proto string, t Target, username string, c client) (net.Conn, error) {
	if t.Ingress != nil {
		return d.dialIngress(ctx, proto, t, c)
	}
	nodes, err := d.candidates(ctx, proto, t.Namespace, username)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, ErrNoBackend
	}
	var last error
	for _, n := range nodes {
		conn, err := d.open(ctx, n.Address)
		if err != nil {
			last = err
			d.markDown(n.Address)
			d.log.Warn("proxy: backend unreachable", slog.String("mode", proto), slog.String("address", n.Address), slog.String("error", err.Error()))
			continue
		}
		if err := d.header(conn, c); err != nil {
			_ = conn.Close()
			return nil, err
		}
		d.markUp(n.Address)
		return conn, nil
	}
	return nil, fmt.Errorf("%w: %v", ErrNoBackend, last)
}

// dialIngress connects to the namespace's ingress host on the protocol's
// port: PROXY v2 goes first (HAProxy accept-proxy reads it before the
// TLS handshake), then TLS per the namespace's policy or the default.
func (d *dialer) dialIngress(ctx context.Context, proto string, t Target, c client) (net.Conn, error) {
	port, ok := d.cfg.IngressPorts[proto]
	if !ok {
		return nil, fmt.Errorf("proxy: no ingress port for %s", proto)
	}
	addr := net.JoinHostPort(t.Ingress.Host, strconv.Itoa(port))
	conn, err := d.open(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("%w: ingress %s: %v", ErrNoBackend, addr, err)
	}
	if err := d.header(conn, c); err != nil {
		_ = conn.Close()
		return nil, err
	}
	policy := t.Ingress.TLS
	if policy == "" {
		policy = d.cfg.IngressTLS
	}
	if policy == identity.IngressTLSOff {
		return conn, nil
	}
	tc := &tls.Config{ServerName: t.Ingress.Host, MinVersion: tls.VersionTLS12}
	if policy == identity.IngressTLSInsecure {
		tc.InsecureSkipVerify = true //nolint:gosec // the namespace's operator chose it
	}
	tconn := tls.Client(conn, tc)
	hctx, cancel := context.WithTimeout(ctx, d.cfg.ConnectTimeout)
	defer cancel()
	if err := tconn.HandshakeContext(hctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: ingress %s tls: %v", ErrNoBackend, addr, err)
	}
	return tconn, nil
}

func (d *dialer) open(ctx context.Context, addr string) (net.Conn, error) {
	dctx, cancel := context.WithTimeout(ctx, d.cfg.ConnectTimeout)
	defer cancel()
	return d.DialCtx(dctx, "tcp", addr)
}

// header sends the PROXY v2 header with the client's addresses and TLS flag.
func (d *dialer) header(conn net.Conn, c client) error {
	_ = conn.SetWriteDeadline(d.Now().Add(d.cfg.ConnectTimeout))
	defer conn.SetWriteDeadline(time.Time{})
	if _, err := conn.Write(proxyproto.HeaderV2(c.remote, c.local, c.tls)); err != nil {
		return fmt.Errorf("%w: proxy header: %v", ErrNoBackend, err)
	}
	return nil
}

var errClosed = errors.New("proxy: connection closed")
