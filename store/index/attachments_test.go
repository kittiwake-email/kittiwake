package index

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"kittiwake/rados"
	"testing"
	"time"
)

func TestAttachmentCountsRecoverWithMailboxJournal(t *testing.T) {
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("index")
	broken := &failIndexWrite{IOContext: io}
	s := New(broken)
	mb := uuid.New()
	root, err := s.EnsureRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	hash := "test-hash"
	ref := AttachmentRef{Hash: hash, Generation: uuid.NewString()}
	if _, err := s.ReserveAttachment(ctx, mb, hash, root.AttachmentEpoch, Attachment{Generation: ref.Generation}); err != nil {
		t.Fatal(err)
	}
	fid := root.Folders[0].ID
	broken.oid = folderOID(mb, fid, 1)
	broken.fail.Store(true)
	_, err = s.Append(ctx, mb, fid, Record{MessageID: uuid.New(), Attachments: []AttachmentRef{ref}}, nil)
	if !errors.Is(err, ErrPending) {
		t.Fatalf("append: %v", err)
	}
	other := New(io)
	a, _, err := other.Attachment(ctx, mb, hash)
	if err != nil || a.Count != 1 {
		t.Fatalf("recovered count: %+v %v", a, err)
	}
	if _, err = other.Remove(ctx, mb, fid, 1, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	_, keys, _, err := other.ListRetention(ctx, mb, time.Now().Add(time.Hour), 10)
	if err != nil || len(keys) != 1 {
		t.Fatalf("retention: %v %v", keys, err)
	}
	oid, _, err := retentionTarget(mb, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	broken.oid = oid
	broken.fail.Store(true)
	err = s.DropRetention(ctx, mb, keys...)
	if !errors.Is(err, ErrPending) {
		t.Fatalf("drop: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := other.DropRetention(ctx, mb, keys...); err != nil {
			t.Fatal(err)
		}
	}
	a, _, err = other.Attachment(ctx, mb, hash)
	if err != nil || a.Count != 0 {
		t.Fatalf("retry count: %+v %v", a, err)
	}
}
