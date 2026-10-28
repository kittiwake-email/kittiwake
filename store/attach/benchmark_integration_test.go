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
	"kittiwake/store/mariadb"
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

func BenchmarkCephAttachmentRemove(b *testing.B) {
	for _, sqlQueue := range []bool{false, true} {
		b.Run(fmt.Sprintf("sql=%v", sqlQueue), func(b *testing.B) {
			if os.Getenv("KITTIWAKE_TEST_CEPH_CONF") == "" {
				b.Skip("set KITTIWAKE_TEST_CEPH_CONF")
			}
			s := New(counterTestPool(b))
			ctx := context.Background()
			if sqlQueue {
				dsn := os.Getenv("KITTIWAKE_TEST_DSN")
				if dsn == "" {
					b.Skip("set KITTIWAKE_TEST_DSN to an isolated test database")
				}
				db, err := mariadb.Open(dsn)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				if _, err := db.Migrate(ctx); err != nil {
					b.Fatal(err)
				}
				defer db.SQL().ExecContext(ctx, "DELETE FROM attachment_gc WHERE storage_id=0")
				s.SetCandidateQueue(db)
			}
			mb := uuid.New()
			data := []byte("shared attachment")
			hash := Hash(data)
			var msgs []uuid.UUID
			for i := 0; i < b.N; i++ {
				msg := uuid.New()
				msgs = append(msgs, msg)
				if err := s.Put(ctx, hash, data, "text/plain", mb, msg); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for _, msg := range msgs {
				if err := s.RemoveRef(ctx, hash, mb, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
