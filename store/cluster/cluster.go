// Package cluster maps mailboxes to the Ceph cluster that holds their data.
//
// Every data store (index, messages, attachments, audit) asks a Resolver
// for the pool context of a mailbox instead of holding one pool. Two
// resolvers exist: Fixed, one cluster for everything (memory backend,
// tests), and Registry, which connects to every cluster in the identity
// store's registry, keeps the connections, and places new mailboxes.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
	"dovetail/store/identity"
)

// Role names a data pool of a cluster.
type Role int

// Pool roles.
const (
	RoleIndex Role = iota
	RoleMessages
	RoleAttachments
)

func (r Role) String() string {
	switch r {
	case RoleIndex:
		return "index"
	case RoleMessages:
		return "messages"
	case RoleAttachments:
		return "attachments"
	}
	return "?"
}

// ErrUnavailable: the mailbox's cluster is not connected from this process
// (unknown id, registry not yet reloaded, or connection failed). Callers
// answer with a temporary failure.
var ErrUnavailable = errors.New("cluster: unavailable")

// Resolver finds the pool contexts of a mailbox's cluster.
type Resolver interface {
	// Pool returns the cluster id and the pool context of role for the
	// mailbox. A cluster id placed in ctx with WithID wins over the lookup
	// (deleted mailboxes, sweeps).
	Pool(ctx context.Context, mailbox uuid.UUID, role Role) (int64, rados.IOContext, error)
	// PoolOf returns the pool context of role on the given cluster.
	PoolOf(ctx context.Context, clusterID int64, role Role) (rados.IOContext, error)
	// Each calls fn with the pool context of role on every connected
	// cluster. Used by sweeps and garbage collection.
	Each(ctx context.Context, role Role, fn func(clusterID int64, io rados.IOContext) error) error
}

type ctxKey struct{}

// WithID pins the cluster for mailboxes touched through ctx.
func WithID(ctx context.Context, clusterID int64) context.Context {
	return context.WithValue(ctx, ctxKey{}, clusterID)
}

// IDFrom returns the cluster pinned in ctx, if any.
func IDFrom(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(ctxKey{}).(int64)
	return v, ok
}

// ---- Fixed: one cluster ----

// Fixed resolves every mailbox to one client (memory backend, tests).
type Fixed struct {
	pools [3]rados.IOContext
}

// NewFixed returns a resolver on the given client and pool names.
func NewFixed(client rados.Client, index, messages, attachments string) (*Fixed, error) {
	f := &Fixed{}
	for i, name := range []string{index, messages, attachments} {
		io, err := client.Pool(name)
		if err != nil {
			return nil, fmt.Errorf("pool %s: %w", name, err)
		}
		f.pools[i] = io
	}
	return f, nil
}

// FixedPools returns a resolver on already opened pool contexts.
func FixedPools(index, messages, attachments rados.IOContext) *Fixed {
	return &Fixed{pools: [3]rados.IOContext{index, messages, attachments}}
}

func (f *Fixed) Pool(_ context.Context, _ uuid.UUID, role Role) (int64, rados.IOContext, error) {
	return 0, f.pools[role], nil
}

func (f *Fixed) PoolOf(_ context.Context, _ int64, role Role) (rados.IOContext, error) {
	return f.pools[role], nil
}

func (f *Fixed) Each(_ context.Context, role Role, fn func(int64, rados.IOContext) error) error {
	return fn(0, f.pools[role])
}

// ---- Registry: every cluster in the identity store ----

// Connector opens a client for a cluster record. Production uses librados;
// tests hand out memory clients.
type Connector func(c identity.Cluster) (rados.Client, error)

// Registry keeps a connection per registered cluster and resolves
// mailboxes to them through the identity store (cached reads).
type Registry struct {
	ids     *identity.Store
	connect Connector
	log     *slog.Logger
	Now     func() time.Time
	// ReloadMinInterval bounds on-demand reloads triggered by an unknown
	// cluster id (a mailbox placed on a cluster added since the last reload).
	ReloadMinInterval time.Duration

	mu       sync.RWMutex
	conns    map[int64]*conn
	lastLoad time.Time
}

type conn struct {
	rec    identity.Cluster
	client rados.Client
	pools  [3]rados.IOContext
	err    error // why the cluster is not connected
}

// Status describes one cluster for health output.
type Status struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	FSID      uuid.UUID `json:"fsid"`
	Placement bool      `json:"placement"`
	Connected bool      `json:"connected"`
	Error     string    `json:"error,omitempty"`
}

// NewRegistry returns a registry; call Reload before use.
func NewRegistry(ids *identity.Store, connect Connector, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{ids: ids, connect: connect, log: log, Now: time.Now, ReloadMinInterval: 10 * time.Second, conns: map[int64]*conn{}}
}

// Reload reads the registry, connects to new clusters (verifying the fsid
// when the record has one, recording it when it has none), applies
// changed placement flags, and closes removed clusters. Connection
// failures are kept as the cluster's status, not returned.
func (r *Registry) Reload(ctx context.Context) error {
	recs, err := r.ids.ListClusters(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastLoad = r.Now()
	seen := map[int64]bool{}
	for _, rec := range recs {
		seen[rec.ID] = true
		c, ok := r.conns[rec.ID]
		if ok && c.client != nil && c.rec.MonHost == rec.MonHost && c.rec.User == rec.User && c.rec.Key == rec.Key && c.rec.Pools == rec.Pools {
			c.rec = rec // placement/name changes need no reconnect
			continue
		}
		if ok && c.client != nil {
			_ = c.client.Close()
		}
		r.conns[rec.ID] = r.open(ctx, rec)
	}
	for id, c := range r.conns {
		if !seen[id] {
			if c.client != nil {
				_ = c.client.Close()
			}
			delete(r.conns, id)
			r.log.Info("cluster removed from registry", slog.Int64("cluster", id), slog.String("name", c.rec.Name))
		}
	}
	return nil
}

func (r *Registry) open(ctx context.Context, rec identity.Cluster) *conn {
	c := &conn{rec: rec}
	client, err := r.connect(rec)
	if err != nil {
		c.err = err
		r.log.Error("cluster connect failed", slog.Int64("cluster", rec.ID), slog.String("name", rec.Name), slog.String("error", err.Error()))
		return c
	}
	fsid, err := client.FSID()
	if err != nil {
		_ = client.Close()
		c.err = fmt.Errorf("fsid: %w", err)
		return c
	}
	got, err := uuid.Parse(fsid)
	if err != nil {
		_ = client.Close()
		c.err = fmt.Errorf("fsid %q: %w", fsid, err)
		return c
	}
	switch {
	case rec.FSID == uuid.Nil:
		// First contact: record the fsid so later reloads verify it.
		if upd, err := r.ids.UpdateCluster(ctx, rec.ID, func(x *identity.Cluster) error {
			if x.FSID != uuid.Nil && x.FSID != got {
				return fmt.Errorf("%w: cluster %s now reports fsid %s, registry has %s", identity.ErrConflict, x.Name, got, x.FSID)
			}
			x.FSID = got
			return nil
		}); err != nil {
			_ = client.Close()
			c.err = err
			return c
		} else {
			c.rec = upd
		}
	case rec.FSID != got:
		_ = client.Close()
		c.err = fmt.Errorf("fsid mismatch: registry says %s, cluster reports %s", rec.FSID, got)
		r.log.Error("cluster refused", slog.Int64("cluster", rec.ID), slog.String("name", rec.Name), slog.String("error", c.err.Error()))
		return c
	}
	for i, name := range []string{rec.Pools.Index, rec.Pools.Messages, rec.Pools.Attachments} {
		io, err := client.Pool(name)
		if err != nil {
			_ = client.Close()
			c.err = fmt.Errorf("pool %s: %w", name, err)
			return c
		}
		c.pools[i] = io
	}
	c.client = client
	r.log.Info("cluster connected", slog.Int64("cluster", rec.ID), slog.String("name", rec.Name), slog.String("fsid", got.String()), slog.Bool("placement", rec.Placement))
	return c
}

// Run reloads the registry every interval until ctx ends.
func (r *Registry) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Reload(ctx); err != nil {
				r.log.Error("cluster registry reload failed", slog.String("error", err.Error()))
			}
		}
	}
}

// Close closes every connection.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, c := range r.conns {
		if c.client != nil {
			_ = c.client.Close()
		}
		delete(r.conns, id)
	}
}

func (r *Registry) get(ctx context.Context, id int64) (*conn, error) {
	r.mu.RLock()
	c, ok := r.conns[id]
	last := r.lastLoad
	r.mu.RUnlock()
	if !ok && r.Now().Sub(last) >= r.ReloadMinInterval {
		// A cluster added since the last reload: look again, bounded.
		if err := r.Reload(ctx); err == nil {
			r.mu.RLock()
			c, ok = r.conns[id]
			r.mu.RUnlock()
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: cluster %d is not in this process's registry", ErrUnavailable, id)
	}
	if c.client == nil {
		return nil, fmt.Errorf("%w: cluster %d (%s): %v", ErrUnavailable, id, c.rec.Name, c.err)
	}
	return c, nil
}

// ClusterOf returns the cluster id of a mailbox: the one pinned in ctx, or
// the mailbox record's (cached).
func (r *Registry) ClusterOf(ctx context.Context, mailbox uuid.UUID) (int64, error) {
	if id, ok := IDFrom(ctx); ok {
		return id, nil
	}
	m, err := r.ids.GetMailboxCached(ctx, mailbox)
	if err != nil {
		return 0, err
	}
	if m.ClusterID == 0 {
		return 0, fmt.Errorf("%w: mailbox %s has no cluster", ErrUnavailable, mailbox)
	}
	return m.ClusterID, nil
}

func (r *Registry) Pool(ctx context.Context, mailbox uuid.UUID, role Role) (int64, rados.IOContext, error) {
	id, err := r.ClusterOf(ctx, mailbox)
	if err != nil {
		return 0, nil, err
	}
	c, err := r.get(ctx, id)
	if err != nil {
		return 0, nil, err
	}
	return id, c.pools[role], nil
}

func (r *Registry) PoolOf(ctx context.Context, clusterID int64, role Role) (rados.IOContext, error) {
	c, err := r.get(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	return c.pools[role], nil
}

func (r *Registry) Each(ctx context.Context, role Role, fn func(int64, rados.IOContext) error) error {
	r.mu.RLock()
	var ids []int64
	for id, c := range r.conns {
		if c.client != nil {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()
	for _, id := range ids {
		r.mu.RLock()
		c, ok := r.conns[id]
		r.mu.RUnlock()
		if !ok || c.client == nil {
			continue
		}
		if err := fn(id, c.pools[role]); err != nil {
			return err
		}
	}
	return nil
}

// ErrNoPlacement: no connected cluster accepts new mailboxes.
var ErrNoPlacement = errors.New("cluster: no cluster accepts new mailboxes")

// Pick chooses a cluster for a new mailbox: random among connected
// clusters with placement enabled.
func (r *Registry) Pick(ctx context.Context) (int64, error) {
	r.mu.RLock()
	var ids []int64
	for id, c := range r.conns {
		if c.client != nil && c.rec.Placement {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()
	if len(ids) == 0 {
		return 0, ErrNoPlacement
	}
	return ids[rand.IntN(len(ids))], nil
}

// Statuses lists every registered cluster with its connection state.
func (r *Registry) Statuses() []Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Status
	for id, c := range r.conns {
		st := Status{ID: id, Name: c.rec.Name, FSID: c.rec.FSID, Placement: c.rec.Placement, Connected: c.client != nil}
		if c.err != nil {
			st.Error = c.err.Error()
		}
		out = append(out, st)
	}
	sortStatuses(out)
	return out
}

func sortStatuses(s []Status) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].ID < s[j-1].ID; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// Verify connects to a cluster record once, returns its fsid and closes
// the connection. Used when a cluster is added through the admin API.
func Verify(connect Connector, rec identity.Cluster) (uuid.UUID, error) {
	client, err := connect(rec)
	if err != nil {
		return uuid.Nil, err
	}
	defer client.Close()
	fsid, err := client.FSID()
	if err != nil {
		return uuid.Nil, err
	}
	got, err := uuid.Parse(fsid)
	if err != nil {
		return uuid.Nil, fmt.Errorf("fsid %q: %w", fsid, err)
	}
	for _, name := range []string{rec.Pools.Index, rec.Pools.Messages, rec.Pools.Attachments} {
		if _, err := client.Pool(name); err != nil {
			return uuid.Nil, fmt.Errorf("pool %s: %w", name, err)
		}
	}
	return got, nil
}
