package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

func testSegments(t *testing.T, io rados.IOContext) {
	t.Helper()
	ctx := context.Background()
	mb := uuid.New()
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	base := mailboxOID(mb, day(now))
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			kv := map[string][]byte{}
			for i := 0; i < 600; i++ {
				kv[fmt.Sprintf("%03d-%06d", w, i)] = encoded(Record{Time: now, Mailbox: mb, Action: "fetch"})
			}
			if err := appendSegmented(ctx, io, base, kv); err != nil {
				t.Error(err)
			}
		}(w)
	}
	wg.Wait()
	var h dayHead
	if _, err := loadJSON(ctx, io, base, &h); err != nil {
		t.Fatal(err)
	}
	if h.Last == 0 {
		t.Fatal("did not rotate")
	}
	total := 0
	for seg := uint64(0); seg <= h.Last; seg++ {
		kv, _, err := io.OmapList(ctx, segmentOID(base, seg), "", "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(kv) > segmentKeys {
			t.Fatalf("unbounded segment: %d", len(kv))
		}
		size := 0
		for _, v := range kv {
			size += len(v.Key) + len(v.Value)
		}
		if size > segmentBytes {
			t.Fatalf("unbounded bytes: %d", size)
		}
		total += len(kv)
	}
	if total != 4800 {
		t.Fatalf("lost concurrent audit records: %d", total)
	}
	s := New(io)
	s.Now = func() time.Time { return now }
	cursor := ""
	seen := 0
	for {
		recs, next, err := s.Read(ctx, Query{Mailbox: mb, From: now, Max: 317, After: cursor})
		if err != nil {
			t.Fatal(err)
		}
		seen += len(recs)
		if next == "" {
			break
		}
		cursor = next
	}
	if seen != total {
		t.Fatalf("paged records: %d", seen)
	}
	// A long hold must not strand data outside a sixty-day lookback window.
	n, err := s.PurgeBefore(ctx, mb, now.AddDate(2, 0, 0), 1)
	if err != nil || n != 1 {
		t.Fatalf("old audit purge: %d %v", n, err)
	}
	for seg := uint64(0); seg <= h.Last; seg++ {
		kv, _, err := io.OmapList(ctx, segmentOID(base, seg), "", "", 0)
		if err != nil || len(kv) != 0 {
			t.Fatalf("retired segment: %d %v", len(kv), err)
		}
	}
	if err := appendSegmented(ctx, io, base, map[string][]byte{"late": json.RawMessage(`{}`)}); !errors.Is(err, ErrExpired) {
		t.Fatalf("late writer: %v", err)
	}
	recs, _, err := s.Read(ctx, Query{Mailbox: mb, From: now, Max: 10})
	if err != nil || len(recs) != 0 {
		t.Fatalf("retired read: %v %v", recs, err)
	}
}
func TestSegmentBoundsConcurrencyAndRetirement(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	testSegments(t, io)
}

type delayedSegment struct {
	rados.IOContext
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (d *delayedSegment) Write(ctx context.Context, oid string, tx *rados.WriteTx) (uint64, error) {
	if strings.HasSuffix(oid, "/0000000000000000") {
		pause := false
		d.once.Do(func() { pause = true })
		if pause {
			close(d.entered)
			<-d.resume
		}
	}
	return d.IOContext.Write(ctx, oid, tx)
}
func TestAuditLateCreateCannotReviveRetiredData(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	d := &delayedSegment{IOContext: io, entered: make(chan struct{}), resume: make(chan struct{})}
	ctx := context.Background()
	base := mailboxOID(uuid.New(), "2024-01-01")
	done := make(chan error, 1)
	go func() { done <- appendSegmented(ctx, d, base, map[string][]byte{"a": []byte(`{}`)}) }()
	<-d.entered
	if _, err := retireDay(ctx, io, base); err != nil {
		t.Fatal(err)
	}
	close(d.resume)
	if err := <-done; !errors.Is(err, ErrExpired) {
		t.Fatalf("late create: %v", err)
	}
	entries, _, err := io.OmapList(ctx, segmentOID(base, 0), "", "", 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("revived audit data: %v %v", entries, err)
	}
}
func TestAuditSegmentByteLimit(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	base := mailboxOID(uuid.New(), "2024-01-01")
	kv := map[string][]byte{"a": make([]byte, segmentBytes/2), "b": make([]byte, segmentBytes/2), "c": make([]byte, segmentBytes/2)}
	if err := appendSegmented(context.Background(), io, base, kv); err != nil {
		t.Fatal(err)
	}
	var h dayHead
	if _, err := loadJSON(context.Background(), io, base, &h); err != nil {
		t.Fatal(err)
	}
	if h.Last != 2 {
		t.Fatalf("large records must rotate: %d", h.Last)
	}
}
