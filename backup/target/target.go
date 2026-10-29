// Package target abstracts where backups are written: an S3-compatible
// object store (production, any provider), a directory (lab, small
// installations) or memory (tests). The surface is the handful of
// operations the backup design needs: whole-object put, ranged get, head,
// delete and prefix listing. Nothing in the daemon lists a bucket on the
// hot path; listing exists for verification and catalog rebuilds.
package target

import (
	"context"
	"errors"
	"io"
	"time"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("backup target: not found")
	// ErrLockUnsupported: the target cannot retain objects (no Object Lock).
	ErrLockUnsupported = errors.New("backup target: retention lock not supported")
)

// Object describes a stored object.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
	// ETag as the provider reports it (opaque; compared for equality).
	ETag string
}

// PutOptions tune a write.
type PutOptions struct {
	ContentType string
	// RetainUntil, when set and the target supports it, locks the object
	// against deletion until then (S3 Object Lock, compliance mode).
	RetainUntil time.Time
}

// Target is a backup store.
type Target interface {
	// Put writes an object of the given size from r.
	Put(ctx context.Context, key string, r io.Reader, size int64, opts PutOptions) error
	// Get reads length bytes from offset (length < 0: to the end).
	Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	// Head returns the object's metadata.
	Head(ctx context.Context, key string) (Object, error)
	// Delete removes an object; absent objects are not an error.
	Delete(ctx context.Context, key string) error
	// List calls fn for every object under prefix in key order, with
	// bounded pages. Returning an error from fn stops the listing.
	List(ctx context.Context, prefix string, fn func(Object) error) error
	// Locking reports whether RetainUntil is honoured.
	Locking() bool
}

// ReadAll fetches a whole object.
func ReadAll(ctx context.Context, t Target, key string) ([]byte, error) {
	rc, err := t.Get(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ReadRange fetches length bytes at offset and insists on getting them all.
func ReadRange(ctx context.Context, t Target, key string, offset, length int64) ([]byte, error) {
	rc, err := t.Get(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	buf := make([]byte, length)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Presigner is implemented by targets that can hand out time-limited
// download links without credentials (S3 query-string signing).
type Presigner interface {
	PresignGet(key string, validity time.Duration) (string, error)
}
