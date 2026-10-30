package storage_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/store/identity"
	"kittiwake/store/storage"
)

// Two fake storage entries behind a connector keyed by mon_host.
func fixture(t *testing.T) (*identity.Store, *storage.Registry, map[string]*rados.MemoryClient) {
	t.Helper()
	meta := rados.NewMemoryClient()
	mp, _ := meta.Pool("meta")
	ids := identity.New(mp)
	ids.CacheTTL = time.Minute
	clients := map[string]*rados.MemoryClient{"mon-a": rados.NewMemoryClient(), "mon-b": rados.NewMemoryClient()}
	connect := func(c identity.Storage) (rados.Client, error) {
		cl, ok := clients[c.MonHost]
		if !ok {
			return nil, errors.New("no route to " + c.MonHost)
		}
		return cl, nil
	}
	reg := storage.NewRegistry(ids, connect, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg.ReloadMinInterval = 0
	return ids, reg, clients
}

func TestRegistry(t *testing.T) {
	ids, reg, clients := fixture(t)
	ctx := context.Background()
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	a, _ := ids.CreateStorage(ctx, identity.Storage{Name: "a", MonHost: "mon-a", Pools: pools, Placement: true})
	b, _ := ids.CreateStorage(ctx, identity.Storage{Name: "b", MonHost: "mon-b", Pools: pools, Placement: true})
	down, _ := ids.CreateStorage(ctx, identity.Storage{Name: "down", MonHost: "mon-x", Pools: pools, Placement: true})
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	// First contact records the fsid.
	if got, _ := ids.GetStorage(ctx, a.ID); got.FSID.String() != clients["mon-a"].Fsid {
		t.Fatalf("fsid not recorded: %+v", got)
	}
	st := reg.Statuses()
	if len(st) != 3 || !st[0].Connected || !st[1].Connected || st[2].Connected || st[2].Error == "" {
		t.Fatalf("statuses: %+v", st)
	}
	// Placement never picks the unreachable storage.
	seen := map[int64]int{}
	for i := 0; i < 200; i++ {
		id, err := reg.Pick(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		seen[id]++
	}
	if seen[a.ID] == 0 || seen[b.ID] == 0 || seen[down.ID] != 0 {
		t.Fatalf("placement: %v", seen)
	}
	// A mailbox resolves to its storage's pools.
	mbA, _ := ids.CreateMailbox(ctx, identity.Mailbox{StorageID: a.ID})
	mbB, _ := ids.CreateMailbox(ctx, identity.Mailbox{StorageID: b.ID})
	cid, ioA, err := reg.Pool(ctx, mbA.ID, storage.RoleIndex)
	if err != nil || cid != a.ID {
		t.Fatalf("pool a: %d %v", cid, err)
	}
	_, ioB, _ := reg.Pool(ctx, mbB.ID, storage.RoleIndex)
	if _, err := ioA.Write(ctx, "x", rados.NewWriteTx().WriteFull([]byte("1"))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ioB.Get(ctx, "x"); !errors.Is(err, rados.ErrNotFound) {
		t.Fatal("object leaked across storage entries")
	}
	if objs := clients["mon-a"].Objects("i", ""); len(objs) != 1 {
		t.Fatalf("object not on storage a's index pool: %v", objs)
	}
	// Unreachable storage and unplaced mailbox are unavailable, not errors elsewhere.
	mbDown, _ := ids.CreateMailbox(ctx, identity.Mailbox{StorageID: down.ID})
	if _, _, err := reg.Pool(ctx, mbDown.ID, storage.RoleMessages); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("down storage: %v", err)
	}
	if _, _, err := reg.Pool(ctx, uuid.New(), storage.RoleMessages); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("unknown mailbox: %v", err)
	}
	// A pinned storage id wins over the lookup (deleted mailboxes).
	if cid, _, err := reg.Pool(storage.WithID(ctx, b.ID), uuid.New(), storage.RoleMessages); err != nil || cid != b.ID {
		t.Fatalf("pinned: %d %v", cid, err)
	}
	// Each visits connected storage entries only.
	n := 0
	_ = reg.Each(ctx, storage.RoleAttachments, func(int64, rados.IOContext) error { n++; return nil })
	if n != 2 {
		t.Fatalf("each: %d", n)
	}
	// Disabling placement takes effect on reload; the storage still serves.
	_, _ = ids.UpdateStorage(ctx, a.ID, func(c *identity.Storage) error { c.Placement = false; return nil })
	_ = reg.Reload(ctx)
	for i := 0; i < 50; i++ {
		if id, _ := reg.Pick(ctx, ""); id == a.ID {
			t.Fatal("disabled storage picked")
		}
	}
	if _, _, err := reg.Pool(ctx, mbA.ID, storage.RoleIndex); err != nil {
		t.Fatalf("disabled storage must still serve: %v", err)
	}
	// A storage added since the last reload is found on demand.
	clients["mon-c"] = rados.NewMemoryClient()
	c, _ := ids.CreateStorage(ctx, identity.Storage{Name: "c", MonHost: "mon-c", Pools: pools, Placement: true})
	mbC, _ := ids.CreateMailbox(ctx, identity.Mailbox{StorageID: c.ID})
	if cid, _, err := reg.Pool(ctx, mbC.ID, storage.RoleIndex); err != nil || cid != c.ID {
		t.Fatalf("on-demand reload: %d %v", cid, err)
	}
	// An fsid mismatch refuses the storage.
	clients["mon-b"] = rados.NewMemoryClient()                                                             // "someone pointed mon-b at another storage"
	_, _ = ids.UpdateStorage(ctx, b.ID, func(c *identity.Storage) error { c.Key = "rotated"; return nil }) // forces a reconnect
	_ = reg.Reload(ctx)
	for _, s := range reg.Statuses() {
		if s.ID == b.ID && (s.Connected || s.Error == "") {
			t.Fatalf("fsid mismatch not refused: %+v", s)
		}
	}
	// Verify reads the fsid without registering.
	if fsid, err := storage.Verify(func(identity.Storage) (rados.Client, error) { return clients["mon-a"], nil }, identity.Storage{Pools: pools}); err != nil || fsid.String() != clients["mon-a"].Fsid {
		t.Fatalf("verify: %s %v", fsid, err)
	}
	reg.Close()
}

func TestReloadDoesNotBlockLookups(t *testing.T) {
	meta := rados.NewMemoryClient()
	mp, _ := meta.Pool("meta")
	ids := identity.New(mp)
	good := rados.NewMemoryClient()
	release := make(chan struct{})
	connect := func(c identity.Storage) (rados.Client, error) {
		if c.MonHost == "slow" {
			<-release // hangs until the test lets it go
			return nil, errors.New("timed out")
		}
		return good, nil
	}
	reg := storage.NewRegistry(ids, connect, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	g, _ := ids.CreateStorage(ctx, identity.Storage{Name: "good", MonHost: "ok", Pools: pools, Placement: true})
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = ids.CreateStorage(ctx, identity.Storage{Name: "slow", MonHost: "slow", Pools: pools, Placement: true})
	done := make(chan struct{})
	go func() { _ = reg.Reload(ctx); close(done) }()
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{StorageID: g.ID})
	// While the slow storage is still connecting, the good one serves and placement works.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("lookup blocked by a connecting storage")
		default:
		}
		if cid, _, err := reg.Pool(ctx, mb.ID, storage.RoleIndex); err == nil && cid == g.ID {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if id, err := reg.Pick(ctx, ""); err != nil || id != g.ID {
		t.Fatalf("pick during connect: %d %v", id, err)
	}
	for _, s := range reg.Statuses() {
		if s.Name == "slow" && s.Error != "connecting" {
			t.Fatalf("slow storage status: %+v", s)
		}
	}
	close(release)
	<-done
	for _, s := range reg.Statuses() {
		if s.Name == "slow" && (s.Connected || s.Error != "timed out") {
			t.Fatalf("slow storage after failure: %+v", s)
		}
	}
	// A failed storage is not retried before RetryInterval.
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestRegistryServesSubset: a registry restricted to one namespace never
// connects the others' storage and reports why a mailbox there is unavailable.
func TestRegistryServesSubset(t *testing.T) {
	ids, reg, _ := fixture(t)
	ctx := context.Background()
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	if _, err := ids.CreateNamespace(ctx, identity.Namespace{Name: "acme", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	def, _ := ids.CreateStorage(ctx, identity.Storage{Name: "def", MonHost: "mon-a", Pools: pools, Placement: true})
	acme, _ := ids.CreateStorage(ctx, identity.Storage{Name: "acme", Namespace: "acme", MonHost: "mon-b", Pools: pools, Placement: true})
	reg.Serve = []string{"acme"}
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	st := reg.Statuses()
	if len(st) != 1 || st[0].ID != acme.ID || st[0].Namespace != "acme" || !st[0].Connected {
		t.Fatalf("only the served namespace connects: %+v", st)
	}
	if _, err := reg.Pick(ctx, "default"); !errors.Is(err, storage.ErrNoPlacement) {
		t.Fatalf("placement in an unserved namespace: %v", err)
	}
	if id, err := reg.Pick(ctx, "acme"); err != nil || id != acme.ID {
		t.Fatalf("placement in the served namespace: %d %v", id, err)
	}
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{StorageID: def.ID})
	_, _, err := reg.Pool(ctx, mb.ID, storage.RoleIndex)
	if !errors.Is(err, storage.ErrUnavailable) || err == nil || !containsAll(err.Error(), "namespace default", "does not serve") {
		t.Fatalf("mailbox of an unserved namespace: %v", err)
	}
	// Each walks only the served storage, and only one namespace when asked.
	var walked []int64
	_ = reg.Each(ctx, storage.RoleIndex, func(id int64, _ rados.IOContext) error { walked = append(walked, id); return nil })
	if len(walked) != 1 || walked[0] != acme.ID {
		t.Fatalf("each: %v", walked)
	}
	walked = nil
	_ = reg.Each(storage.WithNamespace(ctx, "default"), storage.RoleIndex, func(id int64, _ rados.IOContext) error { walked = append(walked, id); return nil })
	if len(walked) != 0 {
		t.Fatalf("each confined to another namespace: %v", walked)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
