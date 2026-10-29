package target

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-process target for tests.
type Memory struct {
	mu      sync.Mutex
	objects map[string]memObject
	Now     func() time.Time
	// Lock makes the target claim Object Lock support.
	Lock bool
	// Fail, when set, is returned by every operation (tests).
	Fail error
}

type memObject struct {
	data     []byte
	modified time.Time
	retain   time.Time
}

// NewMemory returns an empty memory target.
func NewMemory() *Memory { return &Memory{objects: map[string]memObject{}, Now: time.Now} }

func (m *Memory) Put(_ context.Context, key string, r io.Reader, size int64, opts PutOptions) error {
	if m.Fail != nil {
		return m.Fail
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(data)) != size {
		return io.ErrUnexpectedEOF
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := memObject{data: data, modified: m.Now().UTC()}
	if m.Lock {
		o.retain = opts.RetainUntil
	}
	m.objects[key] = o
	return nil
}

func (m *Memory) Get(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if m.Fail != nil {
		return nil, m.Fail
	}
	m.mu.Lock()
	o, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	if offset > int64(len(o.data)) {
		return nil, io.ErrUnexpectedEOF
	}
	data := o.data[offset:]
	if length >= 0 && length < int64(len(data)) {
		data = data[:length]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *Memory) Head(_ context.Context, key string) (Object, error) {
	if m.Fail != nil {
		return Object{}, m.Fail
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	return m.describe(key, o), nil
}

func (m *Memory) describe(key string, o memObject) Object {
	sum := md5.Sum(o.data)
	return Object{Key: key, Size: int64(len(o.data)), Modified: o.modified, ETag: hex.EncodeToString(sum[:])}
}

func (m *Memory) Delete(_ context.Context, key string) error {
	if m.Fail != nil {
		return m.Fail
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok := m.objects[key]; ok && !o.retain.IsZero() && m.Now().Before(o.retain) {
		return ErrLocked
	}
	delete(m.objects, key)
	return nil
}

func (m *Memory) List(_ context.Context, prefix string, fn func(Object) error) error {
	if m.Fail != nil {
		return m.Fail
	}
	m.mu.Lock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	objs := make([]Object, 0, len(keys))
	for _, k := range keys {
		objs = append(objs, m.describe(k, m.objects[k]))
	}
	m.mu.Unlock()
	for _, o := range objs {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

func (m *Memory) Locking() bool { return m.Lock }

// Keys returns every key (tests).
func (m *Memory) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objects))
	for k := range m.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Corrupt flips a byte of an object (tests).
func (m *Memory) Corrupt(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok := m.objects[key]; ok && len(o.data) > 0 {
		d := append([]byte(nil), o.data...)
		d[len(d)/2] ^= 0x55
		o.data = d
		m.objects[key] = o
	}
}
