package message

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"dovetail/rados"
)

func TestPutGetDeleteList(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("messages")
	s := New(io)
	ctx := context.Background()
	mb, msg := uuid.New(), uuid.New()
	if err := s.Put(ctx, mb, msg, []byte(`{"v":1}`), []byte("DVSK1raw")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, mb, msg, nil, nil); !errors.Is(err, ErrExists) {
		t.Fatalf("expected exists, got %v", err)
	}
	doc, err := s.GetDoc(ctx, mb, msg)
	if err != nil || string(doc) != `{"v":1}` {
		t.Fatal(err)
	}
	raw, err := s.GetRaw(ctx, mb, msg)
	if err != nil || string(raw) != "DVSK1raw" {
		t.Fatal(err)
	}
	var seen []Info
	_ = s.List(ctx, func(i Info) error { seen = append(seen, i); return nil })
	if len(seen) != 1 || seen[0].Mailbox != mb || seen[0].Message != msg || seen[0].Size != 7 || seen[0].Created.IsZero() {
		t.Fatalf("list: %+v", seen)
	}
	if err := s.Delete(ctx, mb, msg); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDoc(ctx, mb, msg); !errors.Is(err, ErrNotFound) {
		t.Fatal("not deleted")
	}
	if err := s.Delete(ctx, mb, msg); err != nil {
		t.Fatal("double delete should be nil")
	}
}
