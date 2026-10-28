//go:build integration

package attach

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

func BenchmarkCephAttachmentPut(b *testing.B) {
	conf := os.Getenv("KITTIWAKE_TEST_CEPH_CONF")
	if conf == "" {
		b.Skip("set KITTIWAKE_TEST_CEPH_CONF")
	}
	c, err := rados.Connect(rados.Config{ConfigFile: conf, User: "kittiwake"})
	if err != nil {
		b.Fatal(err)
	}
	base, err := c.Pool("kittiwake.attachments")
	if err != nil {
		c.Close()
		b.Fatal(err)
	}
	io := base.Namespace(fmt.Sprintf("scaling-%d", time.Now().UnixNano()))
	b.Cleanup(func() {
		_ = io.List(context.Background(), func(oid string) error { return io.Delete(context.Background(), oid) })
		c.Close()
	})
	s := New(io)
	data := make([]byte, 16384)
	hash, mb := Hash(data), uuid.New()
	ctx := context.Background()
	if err := s.Put(ctx, hash, data, "application/octet-stream", mb, uuid.New()); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(ctx, hash, data, "application/octet-stream", mb, uuid.New()); err != nil {
			b.Fatal(err)
		}
	}
}
