package rados

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryClient is an in-memory implementation of Client used in tests. It
// models the semantics the stores rely on: per-object versions, atomic
// transactions with assertions, sorted omap, namespaces, locks with expiry
// and watch/notify.
type MemoryClient struct {
	mu    sync.Mutex
	pools map[string]*memPool
	// Now is the clock used for mtimes and lock expiry; tests may override it.
	Now func() time.Time
	// Fsid identifies the fake cluster (random per client; tests may set it).
	Fsid string
}

// NewMemoryClient returns an empty in-memory cluster.
func NewMemoryClient() *MemoryClient {
	return &MemoryClient{pools: map[string]*memPool{}, Now: time.Now, Fsid: newFSID()}
}

func newFSID() string {
	var b [16]byte
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> (uint(i%8) * 8))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 36)
	j := 0
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[j] = '-'
			j++
		}
		out[j], out[j+1] = hexdigits[c>>4], hexdigits[c&0x0f]
		j += 2
	}
	return string(out)
}

// FSID implements Client.
func (c *MemoryClient) FSID() (string, error) { return c.Fsid, nil }

type memPool struct {
	mu    sync.Mutex
	objs  map[string]*memObject // key: namespace + "\x00" + oid
	seq   uint64
	reads atomic.Int64 // read operations (Get, Stat, GetXattrs, OmapGet, OmapList); tests count I/O with it
}

// Reads returns the number of read operations the pool has served. Tests
// use it to assert that a cache keeps a hot path off RADOS.
func (io *memIOContext) Reads() int { return int(io.pool.reads.Load()) }

type memObject struct {
	data     []byte
	xattrs   map[string][]byte
	omap     map[string][]byte
	version  uint64
	mtime    time.Time
	locks    map[string]memLock // lock name -> holder
	watchers map[*memWatcher]struct{}
}

type memLock struct {
	cookie  string
	expires time.Time // zero = never
}

type memWatcher struct {
	events chan []byte
	errs   chan error
	pool   *memPool
	key    string
	once   sync.Once
}

func (w *memWatcher) Events() <-chan []byte { return w.events }
func (w *memWatcher) Errors() <-chan error  { return w.errs }
func (w *memWatcher) Close() error {
	w.once.Do(func() {
		w.pool.mu.Lock()
		if o, ok := w.pool.objs[w.key]; ok {
			delete(o.watchers, w)
		}
		w.pool.mu.Unlock()
		close(w.events)
		close(w.errs)
	})
	return nil
}

// Pool implements Client.
func (c *MemoryClient) Pool(name string) (IOContext, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pools[name]
	if !ok {
		p = &memPool{objs: map[string]*memObject{}}
		c.pools[name] = p
	}
	return &memIOContext{client: c, pool: p, ns: ""}, nil
}

// Close implements Client.
func (c *MemoryClient) Close() error { return nil }

// Objects returns the oids present in pool/namespace, sorted. Test helper.
func (c *MemoryClient) Objects(pool, ns string) []string {
	c.mu.Lock()
	p, ok := c.pools[pool]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	prefix := ns + "\x00"
	for k := range p.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, strings.TrimPrefix(k, prefix))
		}
	}
	sort.Strings(out)
	return out
}

type memIOContext struct {
	client *MemoryClient
	pool   *memPool
	ns     string
}

func (io *memIOContext) key(oid string) string { return io.ns + "\x00" + oid }

func (io *memIOContext) Namespace(ns string) IOContext {
	return &memIOContext{client: io.client, pool: io.pool, ns: ns}
}

func (io *memIOContext) get(oid string) (*memObject, bool) {
	o, ok := io.pool.objs[io.key(oid)]
	return o, ok
}

func (io *memIOContext) Get(ctx context.Context, oid string) ([]byte, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	io.pool.reads.Add(1)
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return nil, 0, ErrNotFound
	}
	return append([]byte(nil), o.data...), o.version, nil
}

func (io *memIOContext) Stat(ctx context.Context, oid string) (Stat, error) {
	if err := ctx.Err(); err != nil {
		return Stat{}, err
	}
	io.pool.reads.Add(1)
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return Stat{}, ErrNotFound
	}
	return Stat{Size: uint64(len(o.data)), ModTime: o.mtime, Version: o.version}, nil
}

func (io *memIOContext) GetXattrs(ctx context.Context, oid string) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	io.pool.reads.Add(1)
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return nil, ErrNotFound
	}
	out := make(map[string][]byte, len(o.xattrs))
	for k, v := range o.xattrs {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}

func (io *memIOContext) OmapGet(ctx context.Context, oid string, keys []string) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	io.pool.reads.Add(1)
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return nil, ErrNotFound
	}
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := o.omap[k]; ok {
			out[k] = append([]byte(nil), v...)
		}
	}
	return out, nil
}

func (io *memIOContext) OmapList(ctx context.Context, oid, prefix, after string, max int) ([]KV, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	io.pool.reads.Add(1)
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return nil, false, ErrNotFound
	}
	keys := make([]string, 0, len(o.omap))
	for k := range o.omap {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	more := false
	if max > 0 && len(keys) > max {
		keys = keys[:max]
		more = true
	}
	out := make([]KV, 0, len(keys))
	for _, k := range keys {
		out = append(out, KV{Key: k, Value: append([]byte(nil), o.omap[k]...)})
	}
	return out, more, nil
}

func (io *memIOContext) Write(ctx context.Context, oid string, tx *WriteTx) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	k := io.key(oid)
	cur, exists := io.pool.objs[k]

	// Validate all assertions first so the transaction is atomic.
	for _, s := range tx.steps {
		switch s.kind {
		case txCreateExclusive:
			if exists {
				return 0, ErrExists
			}
		case txAssertExists:
			if !exists {
				return 0, ErrNotFound
			}
		case txAssertVersion:
			if !exists {
				return 0, ErrNotFound
			}
			if cur.version != s.version {
				return 0, ErrVersionMismatch
			}
		case txRemove, txSetXattr, txRmXattr, txOmapSet, txOmapRemove, txOmapClear:
			// Mutations on a missing object (without a write) create it in
			// librados for xattr/omap ops; Remove on missing is ENOENT.
			if s.kind == txRemove && !exists {
				return 0, ErrNotFound
			}
		}
	}

	// Apply.
	obj := cur
	if obj == nil {
		obj = &memObject{xattrs: map[string][]byte{}, omap: map[string][]byte{}, locks: map[string]memLock{}, watchers: map[*memWatcher]struct{}{}}
	}
	removed := false
	for _, s := range tx.steps {
		switch s.kind {
		case txWriteFull:
			obj.data = append([]byte(nil), s.data...)
		case txSetXattr:
			obj.xattrs[s.name] = append([]byte(nil), s.data...)
		case txRmXattr:
			delete(obj.xattrs, s.name)
		case txOmapSet:
			for kk, vv := range s.kv {
				obj.omap[kk] = append([]byte(nil), vv...)
			}
		case txOmapRemove:
			for _, kk := range s.keys {
				delete(obj.omap, kk)
			}
		case txOmapClear:
			obj.omap = map[string][]byte{}
		case txRemove:
			removed = true
		}
	}
	if removed {
		delete(io.pool.objs, k)
		return 0, nil
	}
	io.pool.seq++
	obj.version = io.pool.seq
	obj.mtime = io.client.Now()
	io.pool.objs[k] = obj
	return obj.version, nil
}

func (io *memIOContext) Delete(ctx context.Context, oid string) error {
	_, err := io.Write(ctx, oid, NewWriteTx().Remove())
	return err
}

func (io *memIOContext) List(ctx context.Context, fn func(oid string) error) error {
	io.pool.mu.Lock()
	prefix := io.ns + "\x00"
	var oids []string
	for k := range io.pool.objs {
		if strings.HasPrefix(k, prefix) {
			oids = append(oids, strings.TrimPrefix(k, prefix))
		}
	}
	io.pool.mu.Unlock()
	sort.Strings(oids)
	for _, oid := range oids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(oid); err != nil {
			return err
		}
	}
	return nil
}

func (io *memIOContext) Lock(ctx context.Context, oid, name, cookie string, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		// librados creates the object when locking a missing one.
		o = &memObject{xattrs: map[string][]byte{}, omap: map[string][]byte{}, locks: map[string]memLock{}, watchers: map[*memWatcher]struct{}{}}
		io.pool.seq++
		o.version = io.pool.seq
		o.mtime = io.client.Now()
		io.pool.objs[io.key(oid)] = o
	}
	now := io.client.Now()
	if l, held := o.locks[name]; held && l.cookie != cookie {
		if l.expires.IsZero() || now.Before(l.expires) {
			return ErrLocked
		}
	}
	var exp time.Time
	if duration > 0 {
		exp = now.Add(duration)
	}
	o.locks[name] = memLock{cookie: cookie, expires: exp}
	return nil
}

func (io *memIOContext) Unlock(ctx context.Context, oid, name, cookie string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return ErrNotLocked
	}
	l, held := o.locks[name]
	if !held || l.cookie != cookie {
		return ErrNotLocked
	}
	delete(o.locks, name)
	return nil
}

func (io *memIOContext) Watch(ctx context.Context, oid string) (Watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	io.pool.mu.Lock()
	defer io.pool.mu.Unlock()
	o, ok := io.get(oid)
	if !ok {
		return nil, ErrNotFound
	}
	w := &memWatcher{events: make(chan []byte, 64), errs: make(chan error, 1), pool: io.pool, key: io.key(oid)}
	o.watchers[w] = struct{}{}
	return w, nil
}

func (io *memIOContext) Notify(ctx context.Context, oid string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	io.pool.mu.Lock()
	o, ok := io.get(oid)
	if !ok {
		io.pool.mu.Unlock()
		return ErrNotFound
	}
	ws := make([]*memWatcher, 0, len(o.watchers))
	for w := range o.watchers {
		ws = append(ws, w)
	}
	io.pool.mu.Unlock()
	for _, w := range ws {
		select {
		case w.events <- append([]byte(nil), payload...):
		default:
			// Slow watcher: drop, like a notify timeout would.
		}
	}
	return nil
}

var _ Client = (*MemoryClient)(nil)
var _ IOContext = (*memIOContext)(nil)
