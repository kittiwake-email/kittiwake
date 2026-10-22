package message

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"kittiwake/rados"
)

func TestPutGetDeleteList(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("messages")
	s := New(io)
	ctx := context.Background()
	mb, msg := uuid.New(), uuid.New()
	if err := s.Put(ctx, mb, msg, []byte(`{"v":1}`), "f1/7"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, mb, msg, nil, ""); !errors.Is(err, ErrExists) {
		t.Fatalf("expected exists, got %v", err)
	}
	doc, err := s.GetDoc(ctx, mb, msg)
	if err != nil || string(doc) != `{"v":1}` {
		t.Fatal(err)
	}
	if _, err := s.GetRaw(ctx, mb, msg); !errors.Is(err, ErrNotFound) {
		t.Fatal("raw should not exist unless kept")
	}
	if err := s.PutRaw(ctx, mb, msg, []byte("original")); err != nil {
		t.Fatal(err)
	}
	raw, _ := s.GetRaw(ctx, mb, msg)
	if string(raw) != "original" {
		t.Fatal("raw")
	}
	loc, err := s.Location(ctx, mb, msg)
	if err != nil || loc != "f1/7" {
		t.Fatalf("loc %q %v", loc, err)
	}
	if err := s.SetLocation(ctx, mb, msg, "f2/1"); err != nil {
		t.Fatal(err)
	}
	if loc, _ = s.Location(ctx, mb, msg); loc != "f2/1" {
		t.Fatal("set location")
	}
	if err := s.SetLocation(ctx, mb, uuid.New(), "x"); !errors.Is(err, ErrNotFound) {
		t.Fatal("set location on missing")
	}
	var seen []Info
	_ = s.List(ctx, func(i Info) error { seen = append(seen, i); return nil })
	if len(seen) != 1 || seen[0].Mailbox != mb || seen[0].Message != msg || seen[0].Size != 7 || seen[0].Created.IsZero() || seen[0].Location != "f2/1" {
		t.Fatalf("list: %+v", seen)
	}
	if err := s.Delete(ctx, mb, msg); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDoc(ctx, mb, msg); !errors.Is(err, ErrNotFound) {
		t.Fatal("not deleted")
	}
	if _, err := s.GetRaw(ctx, mb, msg); !errors.Is(err, ErrNotFound) {
		t.Fatal("raw not deleted")
	}
	if err := s.Delete(ctx, mb, msg); err != nil {
		t.Fatal("double delete should be nil")
	}
}
