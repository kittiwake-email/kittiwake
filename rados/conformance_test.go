package rados

import (
	"context"
	"errors"
	"testing"
	"time"
)

// RunConformance exercises the semantics every IOContext implementation
// must provide. The memory backend runs it in the normal unit tests; the
// librados backend runs it against a real storage with -tags integration.
func RunConformance(t *testing.T, newCtx func(t *testing.T) IOContext) {
	t.Run("WriteGetXattr", func(t *testing.T) { confWriteGet(t, newCtx(t)) })
	t.Run("AssertVersion", func(t *testing.T) { confAssertVersion(t, newCtx(t)) })
	t.Run("NamespacesAndOmap", func(t *testing.T) { confOmap(t, newCtx(t)) })
	t.Run("Locks", func(t *testing.T) { confLocks(t, newCtx(t)) })
	t.Run("WatchNotify", func(t *testing.T) { confWatchNotify(t, newCtx(t)) })
	t.Run("DeleteAndList", func(t *testing.T) { confDeleteList(t, newCtx(t)) })
}

func confWriteGet(t *testing.T, io IOContext) {
	ctx := context.Background()
	v1, err := io.Write(ctx, "a", NewWriteTx().CreateExclusive().WriteFull([]byte("hello")).SetXattr("x", []byte("1")))
	if err != nil {
		t.Fatal(err)
	}
	data, v, err := io.Get(ctx, "a")
	if err != nil || string(data) != "hello" || v != v1 {
		t.Fatalf("get: %q %d (want %d) %v", data, v, v1, err)
	}
	if _, err := io.Write(ctx, "a", NewWriteTx().CreateExclusive()); !errors.Is(err, ErrExists) {
		t.Fatalf("expected ErrExists, got %v", err)
	}
	if _, _, err := io.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, err := io.Stat(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat missing: %v", err)
	}
	st, err := io.Stat(ctx, "a")
	if err != nil || st.Size != 5 || st.Version != v1 {
		t.Fatalf("stat: %+v %v", st, err)
	}
	x, err := io.GetXattrs(ctx, "a")
	if err != nil || string(x["x"]) != "1" {
		t.Fatalf("xattrs: %v %v", x, err)
	}
	if _, err := io.Write(ctx, "a", NewWriteTx().RmXattr("x").SetXattr("y", []byte("2"))); err != nil {
		t.Fatal(err)
	}
	x, _ = io.GetXattrs(ctx, "a")
	if _, ok := x["x"]; ok || string(x["y"]) != "2" {
		t.Fatalf("xattrs after rm: %v", x)
	}
	// Large object round trip (crosses typical read chunk sizes).
	big := make([]byte, 5<<20)
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := io.Write(ctx, "big", NewWriteTx().WriteFull(big)); err != nil {
		t.Fatal(err)
	}
	got, _, err := io.Get(ctx, "big")
	if err != nil || len(got) != len(big) || got[len(got)-1] != big[len(big)-1] || got[12345] != big[12345] {
		t.Fatalf("big round trip: len %d err %v", len(got), err)
	}
}

func confAssertVersion(t *testing.T, io IOContext) {
	ctx := context.Background()
	v1, err := io.Write(ctx, "a", NewWriteTx().WriteFull([]byte("1")))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := io.Write(ctx, "a", NewWriteTx().AssertVersion(v1).WriteFull([]byte("2")))
	if err != nil || v2 == v1 {
		t.Fatalf("v2 %d v1 %d err %v", v2, v1, err)
	}
	_, err = io.Write(ctx, "a", NewWriteTx().AssertVersion(v1).WriteFull([]byte("3")))
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected mismatch, got %v", err)
	}
	data, v, _ := io.Get(ctx, "a")
	if string(data) != "2" || v != v2 {
		t.Fatalf("atomicity violated: %q v=%d", data, v)
	}
	if _, err := io.Write(ctx, "missing", NewWriteTx().AssertExists().WriteFull([]byte("x"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("assert exists on missing: %v", err)
	}
}

func confOmap(t *testing.T, io IOContext) {
	ctx := context.Background()
	a := io.Namespace("conf-a")
	b := io.Namespace("conf-b")
	if _, err := a.Write(ctx, "o", NewWriteTx().OmapSet(map[string][]byte{"k3": []byte("3"), "k1": []byte("1"), "k2": []byte("2"), "z": []byte("z")})); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, "o"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("namespace leak: %v", err)
	}
	kv, more, err := a.OmapList(ctx, "o", "k", "", 2)
	if err != nil || !more || len(kv) != 2 || kv[0].Key != "k1" || kv[1].Key != "k2" || string(kv[1].Value) != "2" {
		t.Fatalf("list: %v %v %v", kv, more, err)
	}
	kv, more, _ = a.OmapList(ctx, "o", "k", "k2", 10)
	if more || len(kv) != 1 || kv[0].Key != "k3" {
		t.Fatalf("list after: %v %v", kv, more)
	}
	kv, _, _ = a.OmapList(ctx, "o", "", "", 0)
	if len(kv) != 4 {
		t.Fatalf("list all: %v", kv)
	}
	m, err := a.OmapGet(ctx, "o", []string{"k1", "missing"})
	if err != nil || len(m) != 1 || string(m["k1"]) != "1" {
		t.Fatalf("omapget: %v %v", m, err)
	}
	if _, err := a.OmapGet(ctx, "nope", []string{"k"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("omapget missing object: %v", err)
	}
	if _, _, err := a.OmapList(ctx, "nope", "", "", 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("omaplist missing object: %v", err)
	}
	// Atomic mixed transaction: remove + set in one op.
	if _, err := a.Write(ctx, "o", NewWriteTx().OmapRemove("k1", "k2").OmapSet(map[string][]byte{"k9": []byte("9")})); err != nil {
		t.Fatal(err)
	}
	kv, _, _ = a.OmapList(ctx, "o", "", "", 0)
	if len(kv) != 3 || kv[0].Key != "k3" || kv[1].Key != "k9" {
		t.Fatalf("after remove+set: %v", kv)
	}
	if _, err := a.Write(ctx, "o", NewWriteTx().OmapClear()); err != nil {
		t.Fatal(err)
	}
	kv, _, _ = a.OmapList(ctx, "o", "", "", 0)
	if len(kv) != 0 {
		t.Fatalf("after clear: %v", kv)
	}
	// Many keys, paged.
	batch := map[string][]byte{}
	for i := 0; i < 2500; i++ {
		batch[keyf("p/%06d", i)] = []byte("v")
	}
	if _, err := a.Write(ctx, "many", NewWriteTx().OmapSet(batch)); err != nil {
		t.Fatal(err)
	}
	after, total := "", 0
	for {
		kv, more, err := a.OmapList(ctx, "many", "p/", after, 1000)
		if err != nil {
			t.Fatal(err)
		}
		total += len(kv)
		if !more {
			break
		}
		after = kv[len(kv)-1].Key
	}
	if total != 2500 {
		t.Fatalf("paged total %d", total)
	}
	// Unbounded listing must return everything, past the OSD's 1024-entry cap.
	all, more, err := a.OmapList(ctx, "many", "p/", "", 0)
	if err != nil || more || len(all) != 2500 {
		t.Fatalf("unbounded list: %d more=%v err=%v", len(all), more, err)
	}
	// A bound above the cap is honoured exactly.
	part, more, err := a.OmapList(ctx, "many", "p/", "", 1500)
	if err != nil || !more || len(part) != 1500 || part[1499].Key != keyf("p/%06d", 1499) {
		t.Fatalf("bounded list: %d more=%v err=%v", len(part), more, err)
	}
}

func keyf(format string, i int) string { return sprintf(format, i) }

func confLocks(t *testing.T, io IOContext) {
	ctx := context.Background()
	if err := io.Lock(ctx, "l", "lock", "c1", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := io.Lock(ctx, "l", "lock", "c2", time.Second); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected locked, got %v", err)
	}
	if err := io.Lock(ctx, "l", "lock", "c1", 2*time.Second); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := io.Unlock(ctx, "l", "lock", "c2"); !errors.Is(err, ErrNotLocked) {
		t.Fatalf("expected not locked, got %v", err)
	}
	if err := io.Unlock(ctx, "l", "lock", "c1"); err != nil {
		t.Fatal(err)
	}
	if err := io.Lock(ctx, "l", "lock", "c2", time.Second); err != nil {
		t.Fatalf("after unlock: %v", err)
	}
	// Expiry: a dead holder's lock becomes takeable.
	time.Sleep(1500 * time.Millisecond)
	if err := io.Lock(ctx, "l", "lock", "c3", time.Second); err != nil {
		t.Fatalf("expired lock should be takeable: %v", err)
	}
	_ = io.Unlock(ctx, "l", "lock", "c3")
}

func confWatchNotify(t *testing.T, io IOContext) {
	ctx := context.Background()
	if _, err := io.Write(ctx, "w", NewWriteTx().WriteFull([]byte("x"))); err != nil {
		t.Fatal(err)
	}
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
	case err := <-w.Errors():
		t.Fatalf("watch error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no event")
	}
	if _, err := io.Watch(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("watch missing: %v", err)
	}
}

func confDeleteList(t *testing.T, io IOContext) {
	ctx := context.Background()
	ns := io.Namespace("conf-list")
	_, _ = ns.Write(ctx, "b", NewWriteTx().WriteFull(nil))
	_, _ = ns.Write(ctx, "a", NewWriteTx().WriteFull(nil))
	seen := map[string]bool{}
	if err := ns.List(ctx, func(oid string) error { seen[oid] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || !seen["a"] || !seen["b"] {
		t.Fatalf("list: %v", seen)
	}
	if err := ns.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := ns.Delete(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := ns.Write(ctx, "b", NewWriteTx().AssertVersion(999999999).Remove()); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("remove with wrong version: %v", err)
	}
}
