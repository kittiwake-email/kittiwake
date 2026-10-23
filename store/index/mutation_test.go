package index

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"kittiwake/rados"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type failIndexWrite struct {
	rados.IOContext
	oid  string
	fail atomic.Bool
}

func (p *failIndexWrite) Write(ctx context.Context, oid string, tx *rados.WriteTx) (uint64, error) {
	if oid == p.oid && p.fail.CompareAndSwap(true, false) {
		return 0, errors.New("injected index write failure")
	}
	return p.IOContext.Write(ctx, oid, tx)
}
func TestAppendPublishesOnlyCommittedChanges(t *testing.T) {
	s, mb := newStore(t)
	s.Now = time.Now
	ctx := context.Background()
	root, _ := s.GetRoot(ctx, mb)
	fid := root.Folders[0].ID
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := s.Append(ctx, mb, fid, rec(7, ""), func(uint32, uint64) error { close(started); <-release; return nil })
		done <- err
	}()
	<-started
	changes, _, current, err := s.Changes(ctx, mb, root.ModSeq, 10)
	if err != nil || len(changes) != 0 || current != root.ModSeq {
		t.Fatalf("uncommitted change: %v %d %v", changes, current, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	changes, _, _, err = s.Changes(ctx, mb, current, 10)
	if err != nil || len(changes) != 1 {
		t.Fatalf("missing committed change: %v %v", changes, err)
	}
}
func TestPreparedAppendRecoveredByAnotherStore(t *testing.T) {
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("index")
	p := &failIndexWrite{IOContext: io}
	s := New(p)
	mb := uuid.New()
	root, _ := s.EnsureRoot(ctx, mb)
	fid := root.Folders[0].ID
	p.oid = folderOID(mb, fid, 1)
	p.fail.Store(true)
	message := rec(9, "")
	_, err := s.Append(ctx, mb, fid, message, nil)
	if !errors.Is(err, ErrPending) {
		t.Fatalf("want durable pending operation: %v", err)
	}
	other := New(io)
	after, err := other.GetRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	got, err := other.GetRecord(ctx, mb, fid, 1)
	if err != nil || got.MessageID != message.MessageID || after.Usage.Messages != 1 {
		t.Fatalf("recovery: %+v %+v %v", after, got, err)
	}
	changes, _, _, err := other.Changes(ctx, mb, root.ModSeq, 10)
	if err != nil || len(changes) != 1 {
		t.Fatalf("log: %v %v", changes, err)
	}
}
func TestConcurrentFlagUpdatesAcrossStores(t *testing.T) {
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("index")
	s := New(io)
	mb := uuid.New()
	root, _ := s.EnsureRoot(ctx, mb)
	fid := root.Folders[0].ID
	r, err := s.Append(ctx, mb, fid, rec(10, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			other := New(io)
			_, err := other.SetFlags(ctx, mb, fid, r.UID, func(flags []string) []string { return append(flags, FlagSeen, strings.Repeat("k", i+1)) })
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	final, err := s.GetRecord(ctx, mb, fid, r.UID)
	if err != nil {
		t.Fatal(err)
	}
	root, _ = s.GetRoot(ctx, mb)
	if root.FolderByID(fid).Unseen != 0 || len(final.Flags) != 13 {
		t.Fatalf("lost updates: unseen=%d flags=%v", root.FolderByID(fid).Unseen, final.Flags)
	}
}
func TestConcurrentHardQuota(t *testing.T) {
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("index")
	s := New(io)
	mb := uuid.New()
	root, _ := s.EnsureRoot(ctx, mb)
	ctx = WithQuota(ctx, 0, 1)
	var wg sync.WaitGroup
	var success atomic.Int32
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := New(io).Append(ctx, mb, root.Folders[0].ID, rec(10, ""), nil)
			if err == nil {
				success.Add(1)
			} else if !errors.Is(err, ErrOverQuota) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if success.Load() != 1 {
		t.Fatalf("quota allowed %d messages", success.Load())
	}
}
func TestInterruptedMoveAndRemoveRecover(t *testing.T) {
	for _, op := range []string{"move", "remove"} {
		t.Run(op, func(t *testing.T) {
			ctx := context.Background()
			client := rados.NewMemoryClient()
			io, _ := client.Pool("index")
			p := &failIndexWrite{IOContext: io}
			s := New(p)
			mb := uuid.New()
			root, _ := s.EnsureRoot(ctx, mb)
			from, to := root.Folders[0].ID, root.Folders[1].ID
			r, err := s.Append(ctx, mb, from, rec(10, ""), nil)
			if err != nil {
				t.Fatal(err)
			}
			p.oid = folderOID(mb, from, r.UID)
			p.fail.Store(true)
			if op == "move" {
				_, err = s.Move(ctx, mb, from, r.UID, to, nil)
			} else {
				_, err = s.Remove(ctx, mb, from, r.UID, "test", time.Now().Add(time.Hour))
			}
			if !errors.Is(err, ErrPending) {
				t.Fatalf("want pending: %v", err)
			}
			other := New(io)
			root, err = other.GetRoot(ctx, mb)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = other.GetRecord(ctx, mb, from, r.UID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("source remains: %v", err)
			}
			loc, known, err := other.Location(ctx, mb, r.MessageID)
			if err != nil || !known {
				t.Fatalf("location: %v %v", known, err)
			}
			if op == "move" {
				if loc.FolderID != to || root.Usage.Messages != 1 {
					t.Fatalf("move: %+v %+v", loc, root.Usage)
				}
			} else {
				entries, _, _, err := other.ListRetention(ctx, mb, time.Time{}, 0)
				if err != nil || len(entries) != 1 || root.Usage.Messages != 0 || loc.FolderID != "" {
					t.Fatalf("remove: %v %+v %+v %v", entries, loc, root.Usage, err)
				}
			}
		})
	}
}

// A helper paused before its target write must not overwrite a newer operation.
func TestDelayedRecoveryHelperIsFenced(t *testing.T) {
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("index")
	p := &failIndexWrite{IOContext: io}
	s := New(p)
	mb := uuid.New()
	root, err := s.EnsureRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	fid := root.Folders[0].ID
	p.oid = folderOID(mb, fid, 1)
	p.fail.Store(true)
	_, err = s.Append(ctx, mb, fid, rec(1, ""), nil)
	if !errors.Is(err, ErrPending) {
		t.Fatal(err)
	}
	prepared, err := s.readRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	delayed := prepared.root.Mutation
	other := New(io)
	if _, err := other.GetRoot(ctx, mb); err != nil {
		t.Fatal(err)
	}
	if _, err := other.SetFlags(ctx, mb, fid, 1, func([]string) []string { return []string{FlagSeen, "newer"} }); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverMutation(ctx, mb, delayed, prepared); err != nil {
		t.Fatal(err)
	}
	got, err := other.GetRecord(ctx, mb, fid, 1)
	if err != nil || len(got.Flags) != 2 {
		t.Fatalf("stale helper overwrote flags: %+v %v", got, err)
	}
	if _, err := other.Remove(ctx, mb, fid, 1, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverMutation(ctx, mb, delayed, prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := other.GetRecord(ctx, mb, fid, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale helper resurrected message: %v", err)
	}
}

func TestStaleCachedRootCannotRejectNewFolder(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	if _, err := s.GetRoot(ctx, mb); err != nil {
		t.Fatal(err)
	}
	other := New(s.pool(ctx, mb))
	folder, err := other.CreateFolder(ctx, mb, "new elsewhere", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, mb, folder.ID, rec(1, ""), nil); err != nil {
		t.Fatalf("stale root rejected new folder: %v", err)
	}
}
