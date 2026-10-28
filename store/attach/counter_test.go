package attach

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

type failReceipt struct {
	rados.IOContext
	fail bool
}

func (io *failReceipt) Write(ctx context.Context, oid string, tx *rados.WriteTx) (uint64, error) {
	if strings.HasPrefix(oid, "receipt/") && io.fail {
		io.fail = false
		return 0, errors.New("injected receipt failure")
	}
	return io.IOContext.Write(ctx, oid, tx)
}
func TestCounterRecoversAcceptedReference(t *testing.T) {
	io := counterTestPool(t)
	broken := &failReceipt{IOContext: io, fail: true}
	s := New(broken)
	ctx := context.Background()
	mb, msg := uuid.New(), uuid.New()
	data := []byte("payload")
	h := Hash(data)
	if err := s.Put(ctx, h, data, "text/plain", mb, msg); err == nil {
		t.Fatal("failure not injected")
	}
	var prepared counterState
	if _, err := readJSON(ctx, io, counterOID(counterID(h, mb)), &prepared); err != nil {
		t.Fatal(err)
	}
	recovered := New(io)
	if info, err := recovered.Stat(ctx, mb, h); err != nil || info.Refs != 1 {
		t.Fatalf("recovery: %+v %v", info, err)
	}
	if err := recovered.Put(ctx, h, data, "text/plain", mb, msg); err != nil {
		t.Fatal(err)
	}
	if info, _ := recovered.Stat(ctx, mb, h); info.Refs != 1 {
		t.Fatal("retry double-counted")
	}
	if err := recovered.RemoveRef(ctx, h, mb, msg); err != nil {
		t.Fatal(err)
	}
	if err := recovered.RemoveRef(ctx, h, mb, msg); err != nil {
		t.Fatal(err)
	}
	if info, _ := recovered.Stat(ctx, mb, h); info.Refs != 0 {
		t.Fatal("retry double-decremented")
	}
	if err := recovered.recoverCounter(ctx, io, counterID(h, mb), prepared.Pending); err != nil {
		t.Fatal(err)
	}
	if info, _ := recovered.Stat(ctx, mb, h); info.Refs != 0 {
		t.Fatal("delayed helper revived a released reference")
	}
	if err := recovered.Put(ctx, h, data, "text/plain", mb, msg); err == nil {
		t.Fatal("released reference revived")
	}
}
func TestCounterConcurrentReferencesAndAuthorization(t *testing.T) {
	io := counterTestPool(t)
	s := New(io)
	ctx := context.Background()
	mb := uuid.New()
	other := uuid.New()
	other[0] = mb[0] // same dedup partition
	data := []byte("shared")
	h := Hash(data)
	const n = 32
	ids := make([]uuid.UUID, n)
	var wg sync.WaitGroup
	for i := range ids {
		ids[i] = uuid.New()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Put(ctx, h, data, "text/plain", mb, ids[i]); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if info, err := s.Stat(ctx, mb, h); err != nil || info.Refs != n {
		t.Fatalf("count: %+v %v", info, err)
	}
	if ok, err := s.HasMailboxRef(ctx, h, other); err != nil || ok {
		t.Fatalf("cross-mailbox access: %v %v", ok, err)
	}
	if ok, err := s.HasMailboxRef(ctx, h, mb); err != nil || !ok {
		t.Fatalf("own access: %v %v", ok, err)
	}
	if err := io.List(ctx, func(oid string) error {
		kv, _, err := io.OmapList(ctx, oid, "", "", 1)
		if err == nil && len(kv) != 0 {
			t.Errorf("reference OMAP on %s", oid)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, msg := range ids {
		wg.Add(1)
		go func(msg uuid.UUID) {
			defer wg.Done()
			if err := s.RemoveRef(ctx, h, mb, msg); err != nil {
				t.Error(err)
			}
		}(msg)
	}
	wg.Wait()
	if info, err := s.Stat(ctx, mb, h); err != nil || info.Refs != 0 {
		t.Fatalf("released count: %+v %v", info, err)
	}
	s.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if deleted, err := s.DeleteIfUnreferenced(ctx, 0, counterID(h, mb), time.Hour); err != nil || !deleted {
		t.Fatalf("collect: %v %v", deleted, err)
	}
	if err := s.Put(ctx, h, data, "text/plain", mb, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, mb, h); err != nil || string(got) != string(data) {
		t.Fatalf("new generation: %q %v", got, err)
	}
}
func TestLegacyAttachmentRemainsReadable(t *testing.T) {
	io := counterTestPool(t)
	s := New(io)
	ctx := context.Background()
	mb, msg := uuid.New(), uuid.New()
	data := []byte("legacy")
	h := Hash(data)
	_, err := io.Write(ctx, h, rados.NewWriteTx().WriteFull(data).OmapSet(map[string][]byte{refKey(mb, msg): nil}))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, mb, h); err != nil || string(got) != string(data) {
		t.Fatal(err)
	}
	if ok, err := s.HasMailboxRef(ctx, h, mb); err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.RemoveRef(ctx, h, mb, msg); err != nil {
		t.Fatal(err)
	}
	if info, err := s.Stat(ctx, mb, h); err != nil || info.Refs != 0 {
		t.Fatalf("legacy release: %+v %v", info, err)
	}
}

func counterTestPool(t *testing.T) rados.IOContext {
	t.Helper()
	conf := os.Getenv("KITTIWAKE_TEST_CEPH_CONF")
	if conf == "" {
		c := rados.NewMemoryClient()
		io, _ := c.Pool("attachments")
		return io
	}
	c, err := rados.Connect(rados.Config{ConfigFile: conf, User: "kittiwake"})
	if err != nil {
		t.Fatal(err)
	}
	base, err := c.Pool("kittiwake.attachments")
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	io := base.Namespace(fmt.Sprintf("counter-test-%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		_ = io.List(context.Background(), func(oid string) error { return io.Delete(context.Background(), oid) })
		c.Close()
	})
	return io
}

type delayedPayloadDelete struct {
	rados.IOContext
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (io *delayedPayloadDelete) Delete(ctx context.Context, oid string) error {
	block := false
	if strings.Contains(oid, "/data/") {
		io.once.Do(func() { block = true })
	}
	if block {
		close(io.entered)
		<-io.resume
	}
	return io.IOContext.Delete(ctx, oid)
}
func TestCounterRetirementCannotDeleteNewGeneration(t *testing.T) {
	io := counterTestPool(t)
	ctx := context.Background()
	s := New(io)
	mb, msg := uuid.New(), uuid.New()
	data := []byte("generation")
	h := Hash(data)
	if err := s.Put(ctx, h, data, "text/plain", mb, msg); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(ctx, h, mb, msg); err != nil {
		t.Fatal(err)
	}
	delayed := &delayedPayloadDelete{IOContext: io, entered: make(chan struct{}), resume: make(chan struct{})}
	collector := New(delayed)
	done := make(chan error, 1)
	go func() { _, err := collector.DeleteIfUnreferenced(ctx, 0, counterID(h, mb), 0); done <- err }()
	<-delayed.entered
	if err := s.Put(ctx, h, data, "text/plain", mb, uuid.New()); err != nil {
		close(delayed.resume)
		<-done
		t.Fatal(err)
	}
	close(delayed.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, mb, h); err != nil || string(got) != string(data) {
		t.Fatalf("new generation removed: %q %v", got, err)
	}
	if info, err := s.Stat(ctx, mb, h); err != nil || info.Refs != 1 {
		t.Fatalf("new count corrupted: %+v %v", info, err)
	}
}
