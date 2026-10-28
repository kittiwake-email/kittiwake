package attach

import (
	"context"
	"fmt"
	"kittiwake/rados"
	"os"
	"testing"
	"time"
)

func counterTestPool(t testing.TB) rados.IOContext {
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
