package cluster_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
	"dovetail/store/cluster"
	"dovetail/store/identity"
)

// Two fake clusters behind a connector keyed by mon_host.
func fixture(t *testing.T) (*identity.Store, *cluster.Registry, map[string]*rados.MemoryClient) {
	t.Helper()
	meta := rados.NewMemoryClient()
	mp, _ := meta.Pool("meta")
	ids := identity.New(mp)
	ids.CacheTTL = time.Minute
	clients := map[string]*rados.MemoryClient{"mon-a": rados.NewMemoryClient(), "mon-b": rados.NewMemoryClient()}
	connect := func(c identity.Cluster) (rados.Client, error) {
		cl, ok := clients[c.MonHost]
		if !ok {
			return nil, errors.New("no route to " + c.MonHost)
		}
		return cl, nil
	}
	reg := cluster.NewRegistry(ids, connect, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg.ReloadMinInterval = 0
	return ids, reg, clients
}

func TestRegistry(t *testing.T) {
	ids, reg, clients := fixture(t)
	ctx := context.Background()
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	a, _ := ids.CreateCluster(ctx, identity.Cluster{Name: "a", MonHost: "mon-a", Pools: pools, Placement: true})
	b, _ := ids.CreateCluster(ctx, identity.Cluster{Name: "b", MonHost: "mon-b", Pools: pools, Placement: true})
	down, _ := ids.CreateCluster(ctx, identity.Cluster{Name: "down", MonHost: "mon-x", Pools: pools, Placement: true})
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	// First contact records the fsid.
	if got, _ := ids.GetCluster(ctx, a.ID); got.FSID.String() != clients["mon-a"].Fsid {
		t.Fatalf("fsid not recorded: %+v", got)
	}
	st := reg.Statuses()
	if len(st) != 3 || !st[0].Connected || !st[1].Connected || st[2].Connected || st[2].Error == "" {
		t.Fatalf("statuses: %+v", st)
	}
	// Placement never picks the unreachable cluster.
	seen := map[int64]int{}
	for i := 0; i < 200; i++ {
		id, err := reg.Pick(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen[id]++
	}
	if seen[a.ID] == 0 || seen[b.ID] == 0 || seen[down.ID] != 0 {
		t.Fatalf("placement: %v", seen)
	}
	// A mailbox resolves to its cluster's pools.
	mbA, _ := ids.CreateMailbox(ctx, identity.Mailbox{ClusterID: a.ID})
	mbB, _ := ids.CreateMailbox(ctx, identity.Mailbox{ClusterID: b.ID})
	cid, ioA, err := reg.Pool(ctx, mbA.ID, cluster.RoleIndex)
	if err != nil || cid != a.ID {
		t.Fatalf("pool a: %d %v", cid, err)
	}
	_, ioB, _ := reg.Pool(ctx, mbB.ID, cluster.RoleIndex)
	if _, err := ioA.Write(ctx, "x", rados.NewWriteTx().WriteFull([]byte("1"))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ioB.Get(ctx, "x"); !errors.Is(err, rados.ErrNotFound) {
		t.Fatal("object leaked across clusters")
	}
	if objs := clients["mon-a"].Objects("i", ""); len(objs) != 1 {
		t.Fatalf("object not on cluster a's index pool: %v", objs)
	}
	// Unreachable cluster and unplaced mailbox are unavailable, not errors elsewhere.
	mbDown, _ := ids.CreateMailbox(ctx, identity.Mailbox{ClusterID: down.ID})
	if _, _, err := reg.Pool(ctx, mbDown.ID, cluster.RoleMessages); !errors.Is(err, cluster.ErrUnavailable) {
		t.Fatalf("down cluster: %v", err)
	}
	if _, _, err := reg.Pool(ctx, uuid.New(), cluster.RoleMessages); !errors.Is(err, identity.ErrNotFound) {
		t.Fatalf("unknown mailbox: %v", err)
	}
	// A pinned cluster id wins over the lookup (deleted mailboxes).
	if cid, _, err := reg.Pool(cluster.WithID(ctx, b.ID), uuid.New(), cluster.RoleMessages); err != nil || cid != b.ID {
		t.Fatalf("pinned: %d %v", cid, err)
	}
	// Each visits connected clusters only.
	n := 0
	_ = reg.Each(ctx, cluster.RoleAttachments, func(int64, rados.IOContext) error { n++; return nil })
	if n != 2 {
		t.Fatalf("each: %d", n)
	}
	// Disabling placement takes effect on reload; the cluster still serves.
	_, _ = ids.UpdateCluster(ctx, a.ID, func(c *identity.Cluster) error { c.Placement = false; return nil })
	_ = reg.Reload(ctx)
	for i := 0; i < 50; i++ {
		if id, _ := reg.Pick(ctx); id == a.ID {
			t.Fatal("disabled cluster picked")
		}
	}
	if _, _, err := reg.Pool(ctx, mbA.ID, cluster.RoleIndex); err != nil {
		t.Fatalf("disabled cluster must still serve: %v", err)
	}
	// A cluster added since the last reload is found on demand.
	clients["mon-c"] = rados.NewMemoryClient()
	c, _ := ids.CreateCluster(ctx, identity.Cluster{Name: "c", MonHost: "mon-c", Pools: pools, Placement: true})
	mbC, _ := ids.CreateMailbox(ctx, identity.Mailbox{ClusterID: c.ID})
	if cid, _, err := reg.Pool(ctx, mbC.ID, cluster.RoleIndex); err != nil || cid != c.ID {
		t.Fatalf("on-demand reload: %d %v", cid, err)
	}
	// An fsid mismatch refuses the cluster.
	clients["mon-b"] = rados.NewMemoryClient()                                                             // "someone pointed mon-b at another cluster"
	_, _ = ids.UpdateCluster(ctx, b.ID, func(c *identity.Cluster) error { c.Key = "rotated"; return nil }) // forces a reconnect
	_ = reg.Reload(ctx)
	for _, s := range reg.Statuses() {
		if s.ID == b.ID && (s.Connected || s.Error == "") {
			t.Fatalf("fsid mismatch not refused: %+v", s)
		}
	}
	// Verify reads the fsid without registering.
	if fsid, err := cluster.Verify(func(identity.Cluster) (rados.Client, error) { return clients["mon-a"], nil }, identity.Cluster{Pools: pools}); err != nil || fsid.String() != clients["mon-a"].Fsid {
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
	connect := func(c identity.Cluster) (rados.Client, error) {
		if c.MonHost == "slow" {
			<-release // hangs until the test lets it go
			return nil, errors.New("timed out")
		}
		return good, nil
	}
	reg := cluster.NewRegistry(ids, connect, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()
	pools := identity.Pools{Messages: "m", Attachments: "a", Index: "i"}
	g, _ := ids.CreateCluster(ctx, identity.Cluster{Name: "good", MonHost: "ok", Pools: pools, Placement: true})
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = ids.CreateCluster(ctx, identity.Cluster{Name: "slow", MonHost: "slow", Pools: pools, Placement: true})
	done := make(chan struct{})
	go func() { _ = reg.Reload(ctx); close(done) }()
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{ClusterID: g.ID})
	// While the slow cluster is still connecting, the good one serves and placement works.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("lookup blocked by a connecting cluster")
		default:
		}
		if cid, _, err := reg.Pool(ctx, mb.ID, cluster.RoleIndex); err == nil && cid == g.ID {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if id, err := reg.Pick(ctx); err != nil || id != g.ID {
		t.Fatalf("pick during connect: %d %v", id, err)
	}
	for _, s := range reg.Statuses() {
		if s.Name == "slow" && s.Error != "connecting" {
			t.Fatalf("slow cluster status: %+v", s)
		}
	}
	close(release)
	<-done
	for _, s := range reg.Statuses() {
		if s.Name == "slow" && (s.Connected || s.Error != "timed out") {
			t.Fatalf("slow cluster after failure: %+v", s)
		}
	}
	// A failed cluster is not retried before RetryInterval.
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
}
