// Package storage maps mailboxes to the Ceph storage that holds their data.
//
// Every data store (index, messages, attachments, audit) asks a Resolver
// for the pool context of a mailbox instead of holding one pool. Two
// resolvers exist: Fixed, one storage for everything (memory backend,
// tests), and Registry, which connects to every storage in the identity
// store's registry, keeps the connections, and places new mailboxes.
package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/store/identity"
)

// Role names a data pool of a storage.
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

// ErrUnavailable: the mailbox's storage is not connected from this process
// (unknown id, registry not yet reloaded, or connection failed). Callers
// answer with a temporary failure.
var ErrUnavailable = errors.New("storage: unavailable")

// Resolver finds the pool contexts of a mailbox's storage.
type Resolver interface {
	// Pool returns the storage id and the pool context of role for the
	// mailbox. A storage id placed in ctx with WithID wins over the lookup
	// (deleted mailboxes, sweeps).
	Pool(ctx context.Context, mailbox uuid.UUID, role Role) (int64, rados.IOContext, error)
	// PoolOf returns the pool context of role on the given storage.
	PoolOf(ctx context.Context, storageID int64, role Role) (rados.IOContext, error)
	// Each calls fn with the pool context of role on every connected
	// storage. Used by sweeps and garbage collection.
	Each(ctx context.Context, role Role, fn func(storageID int64, io rados.IOContext) error) error
}

type ctxKey struct{}

// WithID pins the storage for mailboxes touched through ctx.
func WithID(ctx context.Context, storageID int64) context.Context {
	return context.WithValue(ctx, ctxKey{}, storageID)
}

// IDFrom returns the storage pinned in ctx, if any.
func IDFrom(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(ctxKey{}).(int64)
	return v, ok
}

// ---- Fixed: one storage ----

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

// ---- Registry: every storage in the identity store ----

// Connector opens a client for a storage record. Production uses librados;
// tests hand out memory clients.
type Connector func(c identity.Storage) (rados.Client, error)

// Registry keeps a connection per registered storage and resolves
// mailboxes to them through the identity store (cached reads).
type Registry struct {
	// PlacementReserve excludes raw cluster bytes reserved for headroom.
	PlacementReserve float64
	ids              *identity.Store
	connect          Connector
	log              *slog.Logger
	Now              func() time.Time
	// ReloadMinInterval bounds on-demand reloads triggered by an unknown
	// storage id (a mailbox placed on a storage added since the last reload).
	ReloadMinInterval time.Duration
	// RetryInterval: how long a storage whose connection failed is left
	// alone before the next attempt.
	RetryInterval time.Duration

	mu       sync.RWMutex
	conns    map[int64]*conn
	lastLoad time.Time
}

type conn struct {
	capacity    rados.Capacity
	capacityAt  time.Time
	capacityErr error
	rec         identity.Storage
	client      rados.Client
	pools       [3]rados.IOContext
	err         error     // why the storage is not connected
	tried       time.Time // last connection attempt
}

// Status describes one storage for health output.
type Status struct {
	CapacityTotalBytes     uint64    `json:"capacity_total_bytes,omitempty"`
	CapacityAvailableBytes uint64    `json:"capacity_available_bytes,omitempty"`
	PlacementEligible      bool      `json:"placement_eligible"`
	CapacityError          string    `json:"capacity_error,omitempty"`
	ID                     int64     `json:"id"`
	Name                   string    `json:"name"`
	FSID                   uuid.UUID `json:"fsid"`
	Placement              bool      `json:"placement"`
	Connected              bool      `json:"connected"`
	Error                  string    `json:"error,omitempty"`
}

// NewRegistry returns a registry; call Reload before use.
func NewRegistry(ids *identity.Store, connect Connector, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{PlacementReserve: 0.15, ids: ids, connect: connect, log: log, Now: time.Now, ReloadMinInterval: 10 * time.Second, RetryInterval: time.Minute, conns: map[int64]*conn{}}
}

// Reload reads the registry, connects to new storage entries (verifying the fsid
// when the record has one, recording it when it has none), applies
// changed placement flags, and closes removed storage entries. Connecting
// happens outside the lock, so a slow or unreachable storage never stalls
// lookups for the others; a storage whose last attempt failed is retried
// no more often than RetryInterval. Connection failures are kept as the
// storage's status, not returned.
func (r *Registry) Reload(ctx context.Context) error {
	recs, err := r.ids.ListStorage(ctx)
	if err != nil {
		return err
	}
	now := r.Now()
	// 1. Decide under the lock.
	r.mu.Lock()
	r.lastLoad = now
	seen := map[int64]bool{}
	var todo []identity.Storage
	var closing []rados.Client
	for _, rec := range recs {
		seen[rec.ID] = true
		c, ok := r.conns[rec.ID]
		switch {
		case ok && c.client != nil && sameConn(c.rec, rec):
			c.rec = rec // placement/name changes need no reconnect
		case ok && c.client == nil && sameConn(c.rec, rec) && now.Sub(c.tried) < r.RetryInterval:
			c.rec = rec // failed recently with the same details: wait
		default:
			if ok && c.client != nil {
				closing = append(closing, c.client)
			}
			todo = append(todo, rec)
			r.conns[rec.ID] = &conn{rec: rec, tried: now, err: errConnecting}
		}
	}
	for id, c := range r.conns {
		if !seen[id] {
			if c.client != nil {
				closing = append(closing, c.client)
			}
			delete(r.conns, id)
			r.log.Info("storage removed from registry", slog.Int64("storage", id), slog.String("name", c.rec.Name))
		}
	}
	r.mu.Unlock()
	for _, cl := range closing {
		_ = cl.Close()
	}
	// 2. Connect outside the lock, all candidates in parallel.
	results := make([]*conn, len(todo))
	var wg sync.WaitGroup
	for i, rec := range todo {
		wg.Add(1)
		go func(i int, rec identity.Storage) {
			defer wg.Done()
			results[i] = r.open(ctx, rec)
		}(i, rec)
	}
	wg.Wait()
	// 3. Install.
	r.mu.Lock()
	for i, rec := range todo {
		c := results[i]
		c.tried = now
		if cur, ok := r.conns[rec.ID]; !ok || cur.err != errConnecting {
			// Removed or replaced while we were connecting.
			if c.client != nil {
				_ = c.client.Close()
			}
			continue
		}
		r.conns[rec.ID] = c
	}
	r.mu.Unlock()
	r.refreshCapacity()
	return nil
}

var errConnecting = errors.New("connecting")

func sameConn(a, b identity.Storage) bool {
	return a.MonHost == b.MonHost && a.User == b.User && a.Key == b.Key && a.Pools == b.Pools
}

func (r *Registry) open(ctx context.Context, rec identity.Storage) *conn {
	c := &conn{rec: rec}
	client, err := r.connect(rec)
	if err != nil {
		c.err = err
		r.log.Error("storage connect failed", slog.Int64("storage", rec.ID), slog.String("name", rec.Name), slog.String("error", err.Error()))
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
		upd, err := r.ids.UpdateStorage(ctx, rec.ID, func(x *identity.Storage) error {
			if x.FSID != uuid.Nil && x.FSID != got {
				return fmt.Errorf("%w: storage %s now reports fsid %s, registry has %s", identity.ErrConflict, x.Name, got, x.FSID)
			}
			x.FSID = got
			return nil
		})
		if err != nil {
			_ = client.Close()
			c.err = err
			return c
		}
		c.rec = upd
	case rec.FSID != got:
		_ = client.Close()
		c.err = fmt.Errorf("fsid mismatch: registry says %s, storage reports %s", rec.FSID, got)
		r.log.Error("storage refused", slog.Int64("storage", rec.ID), slog.String("name", rec.Name), slog.String("error", c.err.Error()))
		return c
	}
	for i, name := range []string{rec.Pools.Index, rec.Pools.Messages, rec.Pools.Attachments} {
		io, err := client.Pool(name)
		if err != nil {
			_ = client.Close()
			c.err = fmt.Errorf("pool %s: %w", name, err)
			return c
		}
		if err := probePool(ctx, io, Role(i)); err != nil {
			_ = client.Close()
			c.err = fmt.Errorf("pool %s capabilities: %w", name, err)
			return c
		}
		c.pools[i] = io
	}
	c.client = client
	r.log.Info("storage connected", slog.Int64("storage", rec.ID), slog.String("name", rec.Name), slog.String("fsid", got.String()), slog.Bool("placement", rec.Placement))
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
				r.log.Error("storage registry reload failed", slog.String("error", err.Error()))
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
		// A storage added since the last reload: look again, bounded.
		if err := r.Reload(ctx); err == nil {
			r.mu.RLock()
			c, ok = r.conns[id]
			r.mu.RUnlock()
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: storage %d is not in this process's registry", ErrUnavailable, id)
	}
	if c.client == nil {
		return nil, fmt.Errorf("%w: storage %d (%s): %v", ErrUnavailable, id, c.rec.Name, c.err)
	}
	return c, nil
}

// StorageOf returns the storage id of a mailbox: the one pinned in ctx, or
// the mailbox record's (cached).
func (r *Registry) StorageOf(ctx context.Context, mailbox uuid.UUID) (int64, error) {
	if id, ok := IDFrom(ctx); ok {
		return id, nil
	}
	m, err := r.ids.GetMailboxCached(ctx, mailbox)
	if err != nil {
		return 0, err
	}
	if m.StorageID == 0 {
		return 0, fmt.Errorf("%w: mailbox %s has no storage", ErrUnavailable, mailbox)
	}
	return m.StorageID, nil
}

func (r *Registry) Pool(ctx context.Context, mailbox uuid.UUID, role Role) (int64, rados.IOContext, error) {
	id, err := r.StorageOf(ctx, mailbox)
	if err != nil {
		return 0, nil, err
	}
	c, err := r.get(ctx, id)
	if err != nil {
		return 0, nil, err
	}
	return id, c.pools[role], nil
}

func (r *Registry) PoolOf(ctx context.Context, storageID int64, role Role) (rados.IOContext, error) {
	c, err := r.get(ctx, storageID)
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

// ErrNoPlacement: no connected storage accepts new mailboxes.
var ErrNoPlacement = errors.New("storage: no storage accepts new mailboxes")

// Pick chooses a connected storage weighted by measured headroom above the
// placement reserve. Failed or stale capacity measurements exclude new placement.
func (r *Registry) Pick(ctx context.Context) (int64, error) {
	ids := r.placeable()
	if len(ids) == 0 {
		// A storage registered since the last reload: look again, bounded.
		r.mu.RLock()
		last := r.lastLoad
		r.mu.RUnlock()
		if r.Now().Sub(last) >= r.ReloadMinInterval {
			if err := r.Reload(ctx); err == nil {
				ids = r.placeable()
			}
		}
	}
	if len(ids) == 0 {
		return 0, ErrNoPlacement
	}
	return r.pickWeighted(ids)
}

func (r *Registry) placeable() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ids []int64
	for id, c := range r.conns {
		if r.placementWeight(c) > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// Statuses lists every registered storage with its connection state.
func (r *Registry) Statuses() []Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Status
	for id, c := range r.conns {
		st := Status{ID: id, Name: c.rec.Name, FSID: c.rec.FSID, Placement: c.rec.Placement, Connected: c.client != nil, CapacityTotalBytes: c.capacity.TotalBytes, CapacityAvailableBytes: c.capacity.AvailableBytes, PlacementEligible: r.placementWeight(c) > 0}
		if c.capacityErr != nil {
			st.CapacityError = c.capacityErr.Error()
		}
		if c.err != nil {
			st.Error = c.err.Error()
		}
		if c.err == errConnecting {
			st.Error = "connecting"
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

// Verify connects to a storage record once, returns its fsid and closes
// the connection. Used when a storage is added through the admin API.
func Verify(connect Connector, rec identity.Storage) (uuid.UUID, error) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i, name := range []string{rec.Pools.Index, rec.Pools.Messages, rec.Pools.Attachments} {
		io, err := client.Pool(name)
		if err == nil {
			err = probePool(ctx, io, Role(i))
		}
		if err != nil {
			return uuid.Nil, fmt.Errorf("pool %s capabilities: %w", name, err)
		}
	}
	return got, nil
}

// Opening an IOContext does not establish that a pool supports the operations
// required by its role. Probe only a unique private object, never operator data.
func probePool(ctx context.Context, io rados.IOContext, role Role) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	oid := "capability-probe/" + uuid.NewString()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := io.Delete(cleanup, oid); e != nil && !errors.Is(e, rados.ErrNotFound) && err == nil {
			err = fmt.Errorf("probe cleanup: %w", e)
		}
	}()
	tx := rados.NewWriteTx().CreateExclusive().WriteFull([]byte("probe")).SetXattr("probe", []byte("1"))
	// Production attachment candidates live in SQL; only index pools need OMAP.
	if role == RoleIndex {
		tx.OmapSet(map[string][]byte{"probe": []byte("1")})
	}
	version, err := io.Write(ctx, oid, tx)
	if err != nil {
		return err
	}
	if role == RoleIndex {
		values, err := io.OmapGet(ctx, oid, []string{"probe"})
		if err != nil {
			return err
		}
		if string(values["probe"]) != "1" {
			return errors.New("OMAP probe read mismatch")
		}
	}
	_, err = io.Write(ctx, oid, rados.NewWriteTx().AssertVersion(version).WriteFull([]byte("verified")))
	return err
}
