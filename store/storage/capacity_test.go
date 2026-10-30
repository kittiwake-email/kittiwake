package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"kittiwake/rados"
	"kittiwake/store/identity"
	"kittiwake/store/storage"
)

type measuredClient struct {
	*rados.MemoryClient
	capacity rados.Capacity
	err      error
}

func (c *measuredClient) Capacity() (rados.Capacity, error) { return c.capacity, c.err }
func TestPlacementUsesFreshHeadroom(t *testing.T) {
	ctx := context.Background()
	meta := rados.NewMemoryClient()
	io, _ := meta.Pool("meta")
	ids := identity.New(io)
	a := &measuredClient{MemoryClient: rados.NewMemoryClient(), capacity: rados.Capacity{TotalBytes: 1000, AvailableBytes: 500}}
	b := &measuredClient{MemoryClient: rados.NewMemoryClient(), capacity: rados.Capacity{TotalBytes: 1000, AvailableBytes: 200}}
	pools := identity.Pools{Index: "index", Messages: "messages", Attachments: "attachments"}
	first, err := ids.CreateStorage(ctx, identity.Storage{Name: "a", MonHost: "a", Pools: pools, Placement: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ids.CreateStorage(ctx, identity.Storage{Name: "b", MonHost: "b", Pools: pools, Placement: true})
	if err != nil {
		t.Fatal(err)
	}
	reg := storage.NewRegistry(ids, func(s identity.Storage) (rados.Client, error) {
		if s.Name == "a" {
			return a, nil
		}
		return b, nil
	}, nil)
	now := time.Now()
	reg.Now = func() time.Time { return now }
	reg.ReloadMinInterval = time.Hour
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	counts := map[int64]int{}
	for i := 0; i < 2000; i++ {
		id, err := reg.Pick(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		counts[id]++
	}
	if counts[first.ID] < 1500 || counts[first.ID] > 1950 {
		t.Fatalf("headroom weighting: %v", counts)
	}
	a.capacity.AvailableBytes = 100
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		id, err := reg.Pick(ctx, "")
		if err != nil || id != second.ID {
			t.Fatalf("reserve exclusion: %d %v", id, err)
		}
	}
	b.err = errors.New("monitor unavailable")
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Pick(ctx, ""); !errors.Is(err, storage.ErrNoPlacement) {
		t.Fatalf("unknown capacity: %v", err)
	}
	if _, err := reg.PoolOf(ctx, first.ID, storage.RoleMessages); err != nil {
		t.Fatalf("existing mail must remain readable: %v", err)
	}
	b.err = nil
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Minute)
	if _, err := reg.Pick(ctx, ""); !errors.Is(err, storage.ErrNoPlacement) {
		t.Fatalf("stale capacity: %v", err)
	}
}
