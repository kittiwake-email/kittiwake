// Package rados is Kittiwake's narrow abstraction over Ceph RADOS.
//
// Everything above this package (stores, services, protocols) talks to RADOS
// only through the Client and IOContext interfaces. There are two
// implementations: the real one backed by librados (see librados.go) and an
// in-memory one used by every unit test (see memory.go).
//
// The surface is deliberately small: whole-object reads and writes, xattrs,
// omap (the per-object sorted key/value map), atomic write transactions with
// assertions, exclusive locks, watch/notify and object listing. That is the
// subset the design in README.md relies on.
package rados

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors. Implementations map backend-specific errors onto these so
// callers can use errors.Is.
var (
	// ErrNotFound: the object (or a required omap key / xattr) does not exist.
	ErrNotFound = errors.New("rados: not found")
	// ErrExists: an exclusive create found the object already present.
	ErrExists = errors.New("rados: object exists")
	// ErrVersionMismatch: an AssertVersion step failed, i.e. somebody else
	// modified the object since it was read.
	ErrVersionMismatch = errors.New("rados: version mismatch")
	// ErrLocked: an exclusive lock is held by somebody else.
	ErrLocked = errors.New("rados: locked")
	// ErrNotLocked: unlock of a lock that is not held with this cookie.
	ErrNotLocked = errors.New("rados: not locked")
)

// Client is a connection to a Ceph storage.
type Client interface {
	// Pool returns an IOContext for the named pool (default namespace).
	Pool(name string) (IOContext, error)
	// FSID returns the storage's fsid (its identity across reconnects and
	// config changes).
	FSID() (string, error)
	// Close releases the connection. IOContexts obtained from it become invalid.
	Close() error
}

// Stat describes an object.
type Stat struct {
	Size    uint64
	ModTime time.Time
	// Version is the object's version as tracked by RADOS. It changes on
	// every write and is what AssertVersion compares against.
	Version uint64
}

// KV is one omap entry.
type KV struct {
	Key   string
	Value []byte
}

// Watcher delivers notifications sent to a watched object.
type Watcher interface {
	// Events yields the payload of each Notify on the watched object.
	Events() <-chan []byte
	// Errors yields watch errors (e.g. the watch was lost). After an error
	// the watcher is dead and must be recreated.
	Errors() <-chan error
	// Close stops watching.
	Close() error
}

// IOContext performs operations within one pool and one namespace.
type IOContext interface {
	// Namespace returns a derived IOContext bound to the given RADOS
	// namespace within the same pool. The receiver is unchanged.
	Namespace(ns string) IOContext

	// Get reads the whole object and returns its data and version.
	Get(ctx context.Context, oid string) (data []byte, version uint64, err error)
	// Stat returns size, mtime and version. ErrNotFound if absent.
	Stat(ctx context.Context, oid string) (Stat, error)
	// GetXattrs returns all xattrs of the object. ErrNotFound if absent.
	GetXattrs(ctx context.Context, oid string) (map[string][]byte, error)

	// OmapGet returns the values for the requested keys. Missing keys are
	// simply absent from the result. ErrNotFound if the object is absent.
	OmapGet(ctx context.Context, oid string, keys []string) (map[string][]byte, error)
	// OmapList returns up to max entries whose key starts with prefix and
	// sorts after `after` (exclusive), in key order. more reports whether
	// further entries exist. ErrNotFound if the object is absent.
	OmapList(ctx context.Context, oid, prefix, after string, max int) (entries []KV, more bool, err error)

	// Write applies the transaction atomically and returns the object's new
	// version. If any assertion fails nothing is applied and the matching
	// sentinel error is returned.
	Write(ctx context.Context, oid string, tx *WriteTx) (version uint64, err error)
	// Delete removes the object. ErrNotFound if absent.
	Delete(ctx context.Context, oid string) error

	// List calls fn for every object in the namespace. The order is
	// unspecified. Returning an error from fn stops the listing.
	List(ctx context.Context, fn func(oid string) error) error

	// Lock takes an exclusive lock called name on the object, identified by
	// cookie, expiring after duration (0 = never). ErrLocked if held by
	// another cookie. Re-locking with the same cookie renews it.
	Lock(ctx context.Context, oid, name, cookie string, duration time.Duration) error
	// Unlock releases the lock. ErrNotLocked if not held with this cookie.
	Unlock(ctx context.Context, oid, name, cookie string) error

	// Watch registers for notifications on the object. The object must exist.
	Watch(ctx context.Context, oid string) (Watcher, error)
	// Notify sends payload to all watchers of the object and waits for them
	// to acknowledge (or time out). It is a no-op if there are no watchers.
	Notify(ctx context.Context, oid string, payload []byte) error
}
