package mariadb_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
	"kittiwake/store/attach"
)

func TestAttachmentGCRegistryFencesConcurrentEnqueue(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	db.ConfigurePool(1, 1, time.Minute)
	if err := db.EnqueueAttachmentGC(ctx, 1, "hash"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := db.AttachmentGCCandidates(ctx, 1, func(id string) (bool, error) {
		calls++
		return true, db.EnqueueAttachmentGC(ctx, 1, id)
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls: %d", calls)
	}
	calls = 0
	if err := db.AttachmentGCCandidates(ctx, 1, func(string) (bool, error) { calls++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("concurrent enqueue was lost")
	}
	if err := db.AttachmentGCCandidates(ctx, 1, func(string) (bool, error) { t.Error("completed candidate remains"); return true, nil }); err != nil {
		t.Fatal(err)
	}
}
func TestSQLAttachmentCandidatesUseNoOMAP(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("attachments")
	s := attach.New(io)
	s.SetCandidateQueue(db)
	mb, msg := uuid.New(), uuid.New()
	data := []byte("content")
	hash := attach.Hash(data)
	if err := s.Put(ctx, hash, data, "text/plain", mb, msg); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(ctx, hash, mb, msg); err != nil {
		t.Fatal(err)
	}
	if err := io.List(ctx, func(oid string) error {
		keys, _, err := io.OmapList(ctx, oid, "", "", 1)
		if len(keys) > 0 {
			t.Errorf("unexpected OMAP on %s", oid)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := s.Candidates(ctx, func(int64, string) (bool, error) { calls++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("candidate count: %d", calls)
	}
}
