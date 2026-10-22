//go:build integration

package rados

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestLibradosConformance runs the conformance suite against a real Ceph
// storage. Configure with:
//
//	KITTIWAKE_TEST_CEPH_CONF  (default /etc/ceph/ceph.conf)
//	KITTIWAKE_TEST_CEPH_USER  (default kittiwake)
//	KITTIWAKE_TEST_CEPH_POOL  (default kittiwake.index)
//
// Every subtest uses its own RADOS namespace and removes its objects.
func TestLibradosConformance(t *testing.T) {
	conf := envOr("KITTIWAKE_TEST_CEPH_CONF", "/etc/ceph/ceph.conf")
	user := envOr("KITTIWAKE_TEST_CEPH_USER", "kittiwake")
	pool := envOr("KITTIWAKE_TEST_CEPH_POOL", "kittiwake.index")
	client, err := Connect(Config{ConfigFile: conf, User: user})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()
	base, err := client.Pool(pool)
	if err != nil {
		t.Fatalf("pool %s: %v", pool, err)
	}
	run := fmt.Sprintf("conformance-%d", time.Now().UnixNano())
	n := 0
	RunConformance(t, func(t *testing.T) IOContext {
		n++
		ns := base.Namespace(fmt.Sprintf("%s-%d", run, n))
		t.Cleanup(func() { cleanupNamespace(t, ns) })
		return ns
	})
}

func cleanupNamespace(t *testing.T, io IOContext) {
	ctx := context.Background()
	var oids []string
	_ = io.List(ctx, func(oid string) error { oids = append(oids, oid); return nil })
	for _, oid := range oids {
		_ = io.Delete(ctx, oid)
	}
	// Sub-namespaces created by the omap/list subtests.
	for _, sub := range []string{"conf-a", "conf-b", "conf-list"} {
		s := io.Namespace(sub)
		var subOids []string
		_ = s.List(ctx, func(oid string) error { subOids = append(subOids, oid); return nil })
		for _, oid := range subOids {
			_ = s.Delete(ctx, oid)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
