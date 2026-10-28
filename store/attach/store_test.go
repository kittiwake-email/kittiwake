package attach

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
)

func TestDedupRefsAndGC(t *testing.T) {
	c := rados.NewMemoryClient()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	c.Now = func() time.Time { return now }
	io, _ := c.Pool("att")
	s := New(io)
	s.Now = func() time.Time { return now }
	ctx := context.Background()
	data := []byte("%PDF-1.4 hello")
	h := Hash(data)
	mb := uuid.New()
	m1, m2 := uuid.New(), uuid.New()

	if err := s.Put(ctx, "deadbeef", data, "application/pdf", mb, m1); !errors.Is(err, ErrHash) {
		t.Fatalf("expected hash error, got %v", err)
	}
	if err := s.Put(ctx, h, data, "application/pdf", mb, m1); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, h, data, "application/pdf", mb, m2); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, h, data, "application/pdf", mb, m2); err != nil {
		t.Fatal("idempotent ref failed:", err)
	}
	got, err := s.Get(ctx, mb, h)
	if err != nil || string(got) != string(data) {
		t.Fatal(err)
	}
	info, err := s.Stat(ctx, mb, h)
	if err != nil || info.Refs != 2 || info.ContentType != "application/pdf" || info.Size != uint64(len(data)) {
		t.Fatalf("stat: %+v %v", info, err)
	}
	if err := s.AddRef(ctx, "0000000000000000000000000000000000000000000000000000000000000000", mb, m1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("addref on missing: %v", err)
	}
	if deleted, _ := s.DeleteIfUnreferenced(ctx, 0, counterID(h, mb), time.Hour); deleted {
		t.Fatal("deleted while referenced")
	}
	_ = s.RemoveRef(ctx, h, mb, m1)
	_ = s.RemoveRef(ctx, h, mb, m2)
	_ = s.RemoveRef(ctx, h, mb, m2) // idempotent
	if deleted, _ := s.DeleteIfUnreferenced(ctx, 0, counterID(h, mb), time.Hour); deleted {
		t.Fatal("deleted inside grace")
	}
	now = now.Add(2 * time.Hour)
	deleted, err := s.DeleteIfUnreferenced(ctx, 0, counterID(h, mb), time.Hour)
	if err != nil || !deleted {
		t.Fatalf("gc: %v %v", deleted, err)
	}
	if _, err := s.Get(ctx, mb, h); !errors.Is(err, ErrNotFound) {
		t.Fatal("still there")
	}
	if deleted, err := s.DeleteIfUnreferenced(ctx, 0, counterID(h, mb), time.Hour); deleted || err != nil {
		t.Fatal("gc on missing")
	}
	if err := s.RemoveRef(ctx, h, mb, m1); err != nil {
		t.Fatal("removeref on missing should be nil")
	}
	var hashes []string
	_ = s.Put(ctx, Hash([]byte("x")), []byte("x"), "text/plain", mb, m1)
	_ = s.List(ctx, func(_ int64, h string) error { hashes = append(hashes, h); return nil })
	if len(hashes) != 1 {
		t.Fatalf("list: %v", hashes)
	}
}
