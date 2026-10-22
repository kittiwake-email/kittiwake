package rados

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	ceph "github.com/ceph/go-ceph/rados"
)

// Config describes how to reach a Ceph cluster.
type Config struct {
	// ConfigFile is an optional ceph.conf to read first.
	ConfigFile string
	// Monitors is the mon_host value, e.g. "[2001:db8::1]:6789,[2001:db8::2]:6789".
	Monitors string
	// User is the CephX user without the "client." prefix, e.g. "dovetail".
	User string
	// Key is the CephX secret key (base64). Alternatively use Keyring.
	Key string
	// Keyring is a path to a keyring file.
	Keyring string
	// Options are extra ceph config options applied verbatim.
	Options map[string]string
}

// Connect opens a connection to the cluster described by cfg.
func Connect(cfg Config) (Client, error) {
	var conn *ceph.Conn
	var err error
	if cfg.User != "" {
		conn, err = ceph.NewConnWithUser(cfg.User)
	} else {
		conn, err = ceph.NewConn()
	}
	if err != nil {
		return nil, fmt.Errorf("rados: new conn: %w", err)
	}
	if cfg.ConfigFile != "" {
		if err := conn.ReadConfigFile(cfg.ConfigFile); err != nil {
			return nil, fmt.Errorf("rados: read config %s: %w", cfg.ConfigFile, err)
		}
	}
	set := func(k, v string) error {
		if v == "" {
			return nil
		}
		if err := conn.SetConfigOption(k, v); err != nil {
			return fmt.Errorf("rados: set %s: %w", k, err)
		}
		return nil
	}
	if err := set("mon_host", cfg.Monitors); err != nil {
		return nil, err
	}
	if err := set("key", cfg.Key); err != nil {
		return nil, err
	}
	if err := set("keyring", cfg.Keyring); err != nil {
		return nil, err
	}
	for k, v := range cfg.Options {
		if err := set(k, v); err != nil {
			return nil, err
		}
	}
	if err := conn.Connect(); err != nil {
		return nil, fmt.Errorf("rados: connect: %w", err)
	}
	return &libradosClient{conn: conn}, nil
}

type libradosClient struct {
	conn *ceph.Conn
}

func (c *libradosClient) Pool(name string) (IOContext, error) {
	// Probe once so a bad pool name fails early.
	probe, err := c.conn.OpenIOContext(name)
	if err != nil {
		return nil, mapErr(err)
	}
	probe.Destroy()
	return &libradosIOContext{conn: c.conn, pool: name, ns: "", handles: &sync.Pool{}}, nil
}

func (c *libradosClient) Close() error {
	c.conn.Shutdown()
	return nil
}

// libradosIOContext keeps a pool of go-ceph IOContext handles. librados tracks
// "last version" per handle, so a goroutine must own a handle for the duration
// of an operation + GetLastVersion. A sync.Pool gives us that cheaply.
type libradosIOContext struct {
	conn    *ceph.Conn
	pool    string
	ns      string
	handles *sync.Pool
}

func (io *libradosIOContext) acquire() (*ceph.IOContext, error) {
	if h, ok := io.handles.Get().(*ceph.IOContext); ok && h != nil {
		return h, nil
	}
	h, err := io.conn.OpenIOContext(io.pool)
	if err != nil {
		return nil, mapErr(err)
	}
	h.SetNamespace(io.ns)
	return h, nil
}

func (io *libradosIOContext) release(h *ceph.IOContext) { io.handles.Put(h) }

func (io *libradosIOContext) Namespace(ns string) IOContext {
	return &libradosIOContext{conn: io.conn, pool: io.pool, ns: ns, handles: &sync.Pool{}}
}

func (io *libradosIOContext) Get(ctx context.Context, oid string) ([]byte, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	h, err := io.acquire()
	if err != nil {
		return nil, 0, err
	}
	defer io.release(h)
	st, err := h.Stat(oid)
	if err != nil {
		return nil, 0, mapErr(err)
	}
	buf := make([]byte, st.Size)
	n := 0
	for uint64(n) < st.Size {
		m, err := h.Read(oid, buf[n:], uint64(n))
		if err != nil {
			return nil, 0, mapErr(err)
		}
		if m == 0 {
			break
		}
		n += m
	}
	ver, err := h.GetLastVersion()
	if err != nil {
		return nil, 0, mapErr(err)
	}
	return buf[:n], ver, nil
}

func (io *libradosIOContext) Stat(ctx context.Context, oid string) (Stat, error) {
	if err := ctx.Err(); err != nil {
		return Stat{}, err
	}
	h, err := io.acquire()
	if err != nil {
		return Stat{}, err
	}
	defer io.release(h)
	st, err := h.Stat(oid)
	if err != nil {
		return Stat{}, mapErr(err)
	}
	ver, err := h.GetLastVersion()
	if err != nil {
		return Stat{}, mapErr(err)
	}
	return Stat{Size: st.Size, ModTime: st.ModTime, Version: ver}, nil
}

func (io *libradosIOContext) GetXattrs(ctx context.Context, oid string) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h, err := io.acquire()
	if err != nil {
		return nil, err
	}
	defer io.release(h)
	m, err := h.ListXattrs(oid)
	if err != nil {
		return nil, mapErr(err)
	}
	return m, nil
}

func (io *libradosIOContext) OmapGet(ctx context.Context, oid string, keys []string) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	h, err := io.acquire()
	if err != nil {
		return nil, err
	}
	defer io.release(h)
	op := ceph.CreateReadOp()
	defer op.Release()
	op.AssertExists()
	step := op.GetOmapValuesByKeys(keys)
	if err := op.Operate(h, oid, ceph.OperationNoFlag); err != nil {
		return nil, mapErr(err)
	}
	out := map[string][]byte{}
	for {
		kv, err := step.Next()
		if err != nil {
			return nil, mapErr(err)
		}
		if kv == nil {
			break
		}
		out[kv.Key] = kv.Value
	}
	return out, nil
}

func (io *libradosIOContext) OmapList(ctx context.Context, oid, prefix, after string, max int) ([]KV, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	h, err := io.acquire()
	if err != nil {
		return nil, false, err
	}
	defer io.release(h)
	// An OSD returns at most osd_max_omap_entries_per_request (default
	// 1024) entries per call, so page through until max is reached or the
	// object is exhausted.
	const chunk = 1024
	var out []KV
	cursor := after
	for {
		want := uint64(chunk)
		if max > 0 && max-len(out)+1 < chunk {
			want = uint64(max - len(out) + 1)
		}
		op := ceph.CreateReadOp()
		op.AssertExists()
		step := op.GetOmapValues(cursor, prefix, want)
		err := op.Operate(h, oid, ceph.OperationNoFlag)
		if err != nil {
			op.Release()
			return nil, false, mapErr(err)
		}
		n := 0
		for {
			kv, err := step.Next()
			if err != nil {
				op.Release()
				return nil, false, mapErr(err)
			}
			if kv == nil {
				break
			}
			n++
			cursor = kv.Key
			out = append(out, KV{Key: kv.Key, Value: kv.Value})
			if max > 0 && len(out) > max {
				break
			}
		}
		more := step.More()
		op.Release()
		if max > 0 && len(out) > max {
			return out[:max], true, nil
		}
		if n == 0 || !more {
			return out, false, nil
		}
	}
}

func (io *libradosIOContext) Write(ctx context.Context, oid string, tx *WriteTx) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	h, err := io.acquire()
	if err != nil {
		return 0, err
	}
	defer io.release(h)
	op := ceph.CreateWriteOp()
	defer op.Release()
	var rmXattrs []string
	removed := false
	truncate := false
	for _, s := range tx.steps {
		switch s.kind {
		case txCreateExclusive:
			op.Create(ceph.CreateExclusive)
		case txAssertExists:
			op.AssertExists()
		case txAssertVersion:
			op.AssertVersion(s.version)
		case txWriteFull:
			if len(s.data) == 0 {
				// go-ceph rejects zero-length write steps; create the
				// object in the transaction and truncate it afterwards.
				op.Create(ceph.CreateIdempotent)
				truncate = true
			} else {
				op.WriteFull(s.data)
			}
		case txSetXattr:
			op.SetXattr(s.name, s.data)
		case txRmXattr:
			rmXattrs = append(rmXattrs, s.name)
		case txOmapSet:
			op.SetOmap(s.kv)
		case txOmapRemove:
			op.RmOmapKeys(s.keys)
		case txOmapClear:
			op.CleanOmap()
		case txRemove:
			op.Remove()
			removed = true
		}
	}
	if err := op.Operate(h, oid, ceph.OperationNoFlag); err != nil {
		return 0, mapErr(err)
	}
	if truncate && !removed {
		if err := h.Truncate(oid, 0); err != nil {
			return 0, mapErr(err)
		}
	}
	// librados has no rmxattr write-op step; apply separately.
	for _, n := range rmXattrs {
		if err := h.RmXattr(oid, n); err != nil && !errors.Is(mapErr(err), ErrNotFound) {
			return 0, mapErr(err)
		}
	}
	if removed {
		return 0, nil
	}
	ver, err := h.GetLastVersion()
	if err != nil {
		return 0, mapErr(err)
	}
	return ver, nil
}

func (io *libradosIOContext) Delete(ctx context.Context, oid string) error {
	_, err := io.Write(ctx, oid, NewWriteTx().Remove())
	return err
}

func (io *libradosIOContext) List(ctx context.Context, fn func(oid string) error) error {
	h, err := io.acquire()
	if err != nil {
		return err
	}
	defer io.release(h)
	it, err := h.Iter()
	if err != nil {
		return mapErr(err)
	}
	defer it.Close()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if it.Namespace() != io.ns {
			continue
		}
		if err := fn(it.Value()); err != nil {
			return err
		}
	}
	return mapErr(it.Err())
}

func (io *libradosIOContext) Lock(ctx context.Context, oid, name, cookie string, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := io.acquire()
	if err != nil {
		return err
	}
	defer io.release(h)
	// LIBRADOS_LOCK_FLAG_RENEW lets the same cookie re-lock.
	flags := byte(1)
	ret, err := h.LockExclusive(oid, name, cookie, "dovetail", duration, &flags)
	if err != nil {
		return mapErr(err)
	}
	switch ret {
	case 0:
		return nil
	case -int(syscall.EBUSY), -int(syscall.EEXIST):
		return ErrLocked
	default:
		return fmt.Errorf("rados: lock returned %d", ret)
	}
}

func (io *libradosIOContext) Unlock(ctx context.Context, oid, name, cookie string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := io.acquire()
	if err != nil {
		return err
	}
	defer io.release(h)
	ret, err := h.Unlock(oid, name, cookie)
	if err != nil {
		return mapErr(err)
	}
	switch ret {
	case 0:
		return nil
	case -int(syscall.ENOENT):
		return ErrNotLocked
	default:
		return fmt.Errorf("rados: unlock returned %d", ret)
	}
}

type libradosWatcher struct {
	w      *ceph.Watcher
	events chan []byte
	errs   chan error
	done   chan struct{}
	once   sync.Once
}

func (w *libradosWatcher) Events() <-chan []byte { return w.events }
func (w *libradosWatcher) Errors() <-chan error  { return w.errs }
func (w *libradosWatcher) Close() error {
	var err error
	w.once.Do(func() {
		close(w.done)
		err = w.w.Delete()
	})
	return err
}

func (io *libradosIOContext) Watch(ctx context.Context, oid string) (Watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A watch handle must stay alive for the watcher's lifetime, so it gets
	// its own IOContext rather than a pooled one.
	h, err := io.conn.OpenIOContext(io.pool)
	if err != nil {
		return nil, mapErr(err)
	}
	h.SetNamespace(io.ns)
	cw, err := h.Watch(oid)
	if err != nil {
		h.Destroy()
		return nil, mapErr(err)
	}
	w := &libradosWatcher{w: cw, events: make(chan []byte, 64), errs: make(chan error, 1), done: make(chan struct{})}
	go func() {
		defer h.Destroy()
		defer close(w.events)
		defer close(w.errs)
		for {
			select {
			case <-w.done:
				return
			case ev, ok := <-cw.Events():
				if !ok {
					return
				}
				_ = ev.Ack(nil)
				select {
				case w.events <- ev.Data:
				default:
				}
			case err, ok := <-cw.Errors():
				if !ok {
					return
				}
				select {
				case w.errs <- mapErr(err):
				default:
				}
				return
			}
		}
	}()
	return w, nil
}

func (io *libradosIOContext) Notify(ctx context.Context, oid string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := io.acquire()
	if err != nil {
		return err
	}
	defer io.release(h)
	_, _, err = h.NotifyWithTimeout(oid, payload, 5*time.Second)
	return mapErr(err)
}

// mapErr converts go-ceph errors to this package's sentinels.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var coded interface{ ErrorCode() int }
	if errors.As(err, &coded) {
		switch coded.ErrorCode() {
		case -int(syscall.ENOENT):
			return ErrNotFound
		case -int(syscall.EEXIST):
			return ErrExists
		case -int(syscall.ERANGE), -int(syscall.EOVERFLOW):
			// assert_version: ERANGE when the object is newer than
			// asserted, EOVERFLOW when the asserted version is ahead.
			return ErrVersionMismatch
		case -int(syscall.EBUSY):
			return ErrLocked
		}
	}
	if errors.Is(err, ceph.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, ceph.ErrObjectExists) {
		return ErrExists
	}
	return err
}

var _ Client = (*libradosClient)(nil)
var _ IOContext = (*libradosIOContext)(nil)
