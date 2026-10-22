package rados

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryConformance(t *testing.T) {
	RunConformance(t, func(t *testing.T) IOContext {
		c := NewMemoryClient()
		io, _ := c.Pool("p")
		return io
	})
}

func TestMemoryBasicWriteGet(t *testing.T) {
	c := NewMemoryClient()
	io, _ := c.Pool("p")
	ctx := context.Background()

	v1, err := io.Write(ctx, "a", NewWriteTx().CreateExclusive().WriteFull([]byte("hello")).SetXattr("x", []byte("1")))
	if err != nil {
		t.Fatal(err)
	}
	data, v, err := io.Get(ctx, "a")
	if err != nil || string(data) != "hello" || v != v1 {
		t.Fatalf("get: %q %d %v", data, v, err)
	}
	if _, err := io.Write(ctx, "a", NewWriteTx().CreateExclusive()); !errors.Is(err, ErrExists) {
		t.Fatalf("expected ErrExists, got %v", err)
	}
	if _, _, err := io.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	x, err := io.GetXattrs(ctx, "a")
	if err != nil || string(x["x"]) != "1" {
		t.Fatalf("xattrs: %v %v", x, err)
	}
}

func TestMemoryAssertVersion(t *testing.T) {
	c := NewMemoryClient()
	io, _ := c.Pool("p")
	ctx := context.Background()
	v1, _ := io.Write(ctx, "a", NewWriteTx().WriteFull([]byte("1")))
	v2, err := io.Write(ctx, "a", NewWriteTx().AssertVersion(v1).WriteFull([]byte("2")))
	if err != nil || v2 == v1 {
		t.Fatalf("v2 %d err %v", v2, err)
	}
	_, err = io.Write(ctx, "a", NewWriteTx().AssertVersion(v1).WriteFull([]byte("3")))
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected mismatch, got %v", err)
	}
	data, _, _ := io.Get(ctx, "a")
	if string(data) != "2" {
		t.Fatalf("atomicity violated: %q", data)
	}
}

func TestMemoryNamespacesAndOmap(t *testing.T) {
	c := NewMemoryClient()
	io, _ := c.Pool("p")
	ctx := context.Background()
	a := io.Namespace("a")
	b := io.Namespace("b")
	_, _ = a.Write(ctx, "o", NewWriteTx().OmapSet(map[string][]byte{"k3": []byte("3"), "k1": []byte("1"), "k2": []byte("2"), "z": []byte("z")}))
	if _, _, err := b.Get(ctx, "o"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("namespace leak: %v", err)
	}
	kv, more, err := a.OmapList(ctx, "o", "k", "", 2)
	if err != nil || !more || len(kv) != 2 || kv[0].Key != "k1" || kv[1].Key != "k2" {
		t.Fatalf("list: %v %v %v", kv, more, err)
	}
	kv, more, _ = a.OmapList(ctx, "o", "k", "k2", 10)
	if more || len(kv) != 1 || kv[0].Key != "k3" {
		t.Fatalf("list after: %v %v", kv, more)
	}
	m, _ := a.OmapGet(ctx, "o", []string{"k1", "missing"})
	if len(m) != 1 || string(m["k1"]) != "1" {
		t.Fatalf("omapget: %v", m)
	}
	_, _ = a.Write(ctx, "o", NewWriteTx().OmapRemove("k1", "k2"))
	kv, _, _ = a.OmapList(ctx, "o", "", "", 0)
	if len(kv) != 2 {
		t.Fatalf("after remove: %v", kv)
	}
	if objs := c.Objects("p", "a"); len(objs) != 1 || objs[0] != "o" {
		t.Fatalf("objects: %v", objs)
	}
}

func TestMemoryLocks(t *testing.T) {
	c := NewMemoryClient()
	now := time.Unix(1000, 0)
	c.Now = func() time.Time { return now }
	io, _ := c.Pool("p")
	ctx := context.Background()
	if err := io.Lock(ctx, "l", "lock", "c1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := io.Lock(ctx, "l", "lock", "c2", time.Minute); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected locked, got %v", err)
	}
	if err := io.Lock(ctx, "l", "lock", "c1", time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := io.Unlock(ctx, "l", "lock", "c2"); !errors.Is(err, ErrNotLocked) {
		t.Fatalf("expected not locked, got %v", err)
	}
	now = now.Add(2 * time.Minute)
	if err := io.Lock(ctx, "l", "lock", "c2", time.Minute); err != nil {
		t.Fatalf("expired lock should be takeable: %v", err)
	}
	if err := io.Unlock(ctx, "l", "lock", "c2"); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryWatchNotify(t *testing.T) {
	c := NewMemoryClient()
	io, _ := c.Pool("p")
	ctx := context.Background()
	_, _ = io.Write(ctx, "w", NewWriteTx().WriteFull(nil))
	w, err := io.Watch(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := io.Notify(ctx, "w", []byte("ping")); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		if string(ev) != "ping" {
			t.Fatalf("got %q", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("no event")
	}
}

func TestMemoryDeleteAndList(t *testing.T) {
	c := NewMemoryClient()
	io, _ := c.Pool("p")
	ctx := context.Background()
	_, _ = io.Write(ctx, "b", NewWriteTx().WriteFull(nil))
	_, _ = io.Write(ctx, "a", NewWriteTx().WriteFull(nil))
	var seen []string
	_ = io.List(ctx, func(oid string) error { seen = append(seen, oid); return nil })
	if len(seen) != 2 || seen[0] != "a" {
		t.Fatalf("list: %v", seen)
	}
	if err := io.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := io.Delete(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}
