package target

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FS stores objects as files under a directory: key "a/b/c" is the file
// <root>/a/b/c. Writes go to a temporary file and are renamed into place,
// so a reader never sees a partial object. No retention lock.
type FS struct {
	root string
}

// NewFS returns a directory target, creating the directory.
func NewFS(root string) (*FS, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	return &FS{root: root}, nil
}

func (f *FS) path(key string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(key))
	if key == "" || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("backup target: bad key %q", key)
	}
	return filepath.Join(f.root, clean), nil
}

func (f *FS) Put(_ context.Context, key string, r io.Reader, size int64, _ PutOptions) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".put-*")
	if err != nil {
		return err
	}
	n, err := io.Copy(tmp, r)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil && size >= 0 && n != size {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (f *FS) Get(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	fh, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if offset > 0 {
		if _, err := fh.Seek(offset, io.SeekStart); err != nil {
			_ = fh.Close()
			return nil, err
		}
	}
	if length < 0 {
		return fh, nil
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(fh, length), fh}, nil
}

func (f *FS) Head(_ context.Context, key string) (Object, error) {
	p, err := f.path(key)
	if err != nil {
		return Object{}, err
	}
	st, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Object{}, ErrNotFound
		}
		return Object{}, err
	}
	return f.describe(key, p, st)
}

func (f *FS) describe(key, p string, st fs.FileInfo) (Object, error) {
	fh, err := os.Open(p)
	if err != nil {
		return Object{}, err
	}
	defer fh.Close()
	h := md5.New()
	if _, err := io.Copy(h, fh); err != nil {
		return Object{}, err
	}
	return Object{Key: key, Size: st.Size(), Modified: st.ModTime().UTC(), ETag: hex.EncodeToString(h.Sum(nil))}, nil
}

func (f *FS) Delete(_ context.Context, key string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	// Prune now-empty parents up to the root.
	for d := filepath.Dir(p); d != f.root && strings.HasPrefix(d, f.root); d = filepath.Dir(d) {
		if os.Remove(d) != nil {
			break
		}
	}
	return nil
}

func (f *FS) List(ctx context.Context, prefix string, fn func(Object) error) error {
	var keys []string
	err := filepath.WalkDir(f.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".put-") {
			return nil
		}
		rel, err := filepath.Rel(f.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, _ := f.path(k)
		st, err := os.Stat(p)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		o, err := f.describe(k, p, st)
		if err != nil {
			return err
		}
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

func (f *FS) Locking() bool { return false }
