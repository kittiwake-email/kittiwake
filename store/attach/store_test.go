package attach

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
	"kittiwake/store/index"
)

func TestDedupRefsAndGC(t *testing.T) {
	io := counterTestPool(t)
	s := New(io)
	now := time.Now().UTC()
	s.Now = func() time.Time { return now }
	ctx := context.Background()
	mb := uuid.New()
	data := []byte("%PDF-1.4 hello")
	h := Hash(data)
	if _, err := s.Upload(ctx, "deadbeef", data, "application/pdf", mb); !errors.Is(err, ErrHash) {
		t.Fatal(err)
	}
	ref, err := s.Upload(ctx, h, data, "application/pdf", mb)
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.idx.GetRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	fid := root.Folders[0].ID
	for i := 0; i < 2; i++ {
		again, err := s.Upload(ctx, h, data, "application/pdf", mb)
		if err != nil || again != ref {
			t.Fatalf("dedup: %+v %v", again, err)
		}
		rec, err := s.idx.Append(ctx, mb, fid, index.Record{MessageID: uuid.New(), Size: 20, Attachments: []index.AttachmentRef{ref}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.idx.Remove(ctx, mb, fid, rec.UID, "test", now); err != nil {
			t.Fatal(err)
		}
	}
	info, err := s.Stat(ctx, mb, h)
	if err != nil || info.Refs != 2 {
		t.Fatalf("retained refs: %+v %v", info, err)
	}
	if yes, err := s.HasMailboxRef(ctx, h, uuid.New()); err != nil || yes {
		t.Fatalf("foreign mailbox: %v %v", yes, err)
	}
	if deleted, err := s.DeleteIfUnreferenced(ctx, 0, payloadID(mb, ref), 0); err != nil || deleted {
		t.Fatalf("live GC: %v %v", deleted, err)
	}
	got, err := s.Get(ctx, mb, h)
	if err != nil || string(got) != string(data) {
		t.Fatal("payload", err)
	}
	_, keys, _, err := s.idx.ListRetention(ctx, mb, now.Add(time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.idx.DropRetention(ctx, mb, keys...); err != nil {
			t.Fatal(err)
		}
	}
	info, err = s.Stat(ctx, mb, h)
	if err != nil || info.Refs != 0 {
		t.Fatalf("retry decremented twice: %+v %v", info, err)
	}
	if deleted, err := s.DeleteIfUnreferenced(ctx, 0, payloadID(mb, ref), time.Hour); err != nil || deleted {
		t.Fatalf("grace: %v %v", deleted, err)
	}
	now = now.Add(2 * time.Hour)
	if deleted, err := s.DeleteIfUnreferenced(ctx, 0, payloadID(mb, ref), time.Hour); err != nil || !deleted {
		t.Fatalf("GC: %v %v", deleted, err)
	}
	if _, err := s.Get(ctx, mb, h); !errors.Is(err, ErrNotFound) {
		t.Fatal("counter survives", err)
	}
	if err := io.List(ctx, func(oid string) error {
		if strings.HasPrefix(oid, "receipt/") || strings.HasPrefix(oid, "owner/") || strings.HasPrefix(oid, "v3/") {
			t.Errorf("unexpected object %s", oid)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorFencesUnpublishedUpload(t *testing.T) {
	s := New(counterTestPool(t))
	ctx := context.Background()
	mb := uuid.New()
	root, err := s.idx.EnsureRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("late upload")
	h := Hash(data)
	_, epoch, _ := s.idx.Attachment(ctx, mb, h)
	ref := index.AttachmentRef{Hash: h, Generation: uuid.NewString()}
	// Candidate exists before the counter reservation or payload.
	if err := s.MarkGC(ctx, mb, []index.AttachmentRef{ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteIfUnreferenced(ctx, 0, payloadID(mb, ref), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.idx.ReserveAttachment(ctx, mb, h, epoch, index.Attachment{Generation: ref.Generation}); !errors.Is(err, index.ErrAttachmentRetired) {
		t.Fatalf("late reservation: %v", err)
	}
	ref, err = s.Upload(ctx, h, data, "text/plain", mb)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := s.DeleteIfUnreferenced(ctx, 0, payloadID(mb, ref), 0); err != nil || !deleted {
		t.Fatalf("retire: %v %v", deleted, err)
	}
	if _, err := s.idx.Append(ctx, mb, root.Folders[0].ID, index.Record{MessageID: uuid.New(), Attachments: []index.AttachmentRef{ref}}, nil); !errors.Is(err, index.ErrAttachmentRetired) {
		t.Fatalf("late publication: %v", err)
	}
}

type delayedPayloadDelete struct {
	rados.IOContext
	once            sync.Once
	entered, resume chan struct{}
}

func (io *delayedPayloadDelete) Delete(ctx context.Context, oid string) error {
	block := false
	if strings.HasPrefix(oid, "v3/") {
		io.once.Do(func() { block = true })
	}
	if block {
		close(io.entered)
		<-io.resume
	}
	return io.IOContext.Delete(ctx, oid)
}
func TestCollectorCannotDeleteNewGeneration(t *testing.T) {
	io := &delayedPayloadDelete{IOContext: counterTestPool(t), entered: make(chan struct{}), resume: make(chan struct{})}
	s := New(io)
	ctx := context.Background()
	mb := uuid.New()
	data := []byte("generation")
	h := Hash(data)
	ref, err := s.Upload(ctx, h, data, "text/plain", mb)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.DeleteIfUnreferenced(ctx, 0, payloadID(mb, ref), 0); done <- err }()
	<-io.entered
	fresh, err := s.Upload(ctx, h, data, "text/plain", mb)
	close(io.resume)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if err != nil || fresh.Generation == ref.Generation {
		t.Fatalf("generation: %+v %v", fresh, err)
	}
	if got, err := s.Get(ctx, mb, h); err != nil || string(got) != string(data) {
		t.Fatalf("replacement lost: %v", err)
	}
}
