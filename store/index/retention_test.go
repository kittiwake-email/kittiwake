package index

import (
	"context"
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"
)

func TestRetentionRangeBoundaryAndMembership(t *testing.T) {
	s, mb := newStore(t)
	testRetentionRanges(t, s, mb)
}
func testRetentionRanges(t *testing.T, s *Store, mb uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	root, _ := s.GetRoot(ctx, mb)
	fid := root.Folders[0].ID
	_, err := s.updateRoot(ctx, mb, func(r *Root) error { r.RetentionNext = 65535; return nil })
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Append(ctx, mb, fid, rec(1, "a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Append(ctx, mb, fid, rec(1, "b"), nil)
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(-time.Hour)
	if _, err := s.Remove(ctx, mb, fid, a.UID, "test", due); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Remove(ctx, mb, fid, b.UID, "test", due); err != nil {
		t.Fatal(err)
	}
	entries, keys, more, err := s.ListRetention(ctx, mb, time.Now(), 1)
	if err != nil || len(entries) != 1 || !more || !strings.HasPrefix(keys[0], "s/0000000000000000/") {
		t.Fatalf("first page: %v %v %v %v", entries, keys, more, err)
	}
	if ok, err := s.IsRetained(ctx, mb, a.MessageID); err != nil || !ok {
		t.Fatalf("membership: %v %v", ok, err)
	}
	if err := s.DropRetention(ctx, mb, keys...); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.IsRetained(ctx, mb, a.MessageID); err != nil || ok {
		t.Fatalf("removed membership: %v %v", ok, err)
	}
	entries, keys, more, err = s.ListRetention(ctx, mb, time.Now(), 1)
	if err != nil || len(entries) != 1 || more || entries[0].MessageID != b.MessageID || !strings.HasPrefix(keys[0], "s/0000000000000001/") {
		t.Fatalf("second page: %v %v %v %v", entries, keys, more, err)
	}
	root, _ = s.GetRoot(ctx, mb)
	if root.RetentionFirst != 1 {
		t.Fatalf("low watermark: %d", root.RetentionFirst)
	}
	if err := s.DropRetention(ctx, mb, keys...); err != nil {
		t.Fatal(err)
	}
	if err := s.DropRetention(ctx, mb, keys...); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestRetentionFutureRange(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	root, _ := s.GetRoot(ctx, mb)
	fid := root.Folders[0].ID
	future, err := s.Append(ctx, mb, fid, rec(1, "future"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Remove(ctx, mb, fid, future.UID, "test", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	entries, _, _, err := s.ListRetention(ctx, mb, time.Now(), 10)
	if err != nil || len(entries) != 0 {
		t.Fatalf("future entry must stay: %v %v", entries, err)
	}
	if ok, err := s.IsRetained(ctx, mb, future.MessageID); err != nil || !ok {
		t.Fatalf("future membership: %v %v", ok, err)
	}
}

func TestNextRetentionChecksEveryRange(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	root, _ := s.GetRoot(ctx, mb)
	fid := root.Folders[0].ID
	if _, err := s.updateRoot(ctx, mb, func(r *Root) error { r.RetentionNext = 65535; return nil }); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(48 * time.Hour)
	earlier := later.Add(-24 * time.Hour)
	for _, at := range []time.Time{later, earlier} {
		r, err := s.Append(ctx, mb, fid, rec(1, ""), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Remove(ctx, mb, fid, r.UID, "test", at); err != nil {
			t.Fatal(err)
		}
	}
	next, err := s.NextRetention(ctx, mb)
	if err != nil || !next.Equal(earlier) {
		t.Fatalf("next: %v %v", next, err)
	}
}
