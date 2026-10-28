//go:build integration

package audit

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

func auditCeph(b testing.TB) rados.IOContext {
	b.Helper()
	conf := os.Getenv("KITTIWAKE_TEST_CEPH_CONF")
	if conf == "" {
		b.Skip("set KITTIWAKE_TEST_CEPH_CONF")
	}
	c, err := rados.Connect(rados.Config{ConfigFile: conf, User: "kittiwake"})
	if err != nil {
		b.Fatal(err)
	}
	pool, err := c.Pool("kittiwake.index")
	if err != nil {
		b.Fatal(err)
	}
	io := pool.Namespace(fmt.Sprintf("audit-scale-%d", time.Now().UnixNano()))
	b.Cleanup(func() {
		_ = io.List(context.Background(), func(oid string) error { return io.Delete(context.Background(), oid) })
		c.Close()
	})
	return io
}
func BenchmarkCephAuditBatch(b *testing.B) {
	s := New(auditCeph(b))
	s.StartBuffering(time.Hour, 100000, func(err error) { b.Error(err) })
	defer s.Close()
	mb := uuid.New()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 128; j++ {
			if err := s.Write(ctx, Record{Mailbox: mb, Protocol: ProtoIMAP, Action: "fetch", Result: ResultOK}); err != nil {
				b.Fatal(err)
			}
		}
		s.Flush(ctx)
	}
}

func TestCephAuditSegments(t *testing.T) { testSegments(t, auditCeph(t)) }
