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
	"kittiwake/store/index"
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

// Includes the mailbox publication that turns uploaded data into visible mail.
func BenchmarkCephAttachmentDelivery(b *testing.B) {
	if os.Getenv("KITTIWAKE_TEST_CEPH_CONF") == "" {
		b.Skip("set KITTIWAKE_TEST_CEPH_CONF")
	}
	io := counterTestPool(b)
	s := New(io)
	idx := index.New(io)
	mb := uuid.New()
	ctx := context.Background()
	root, err := idx.EnsureRoot(ctx, mb)
	if err != nil {
		b.Fatal(err)
	}
	fid := root.Folders[0].ID
	data := make([]byte, 16384)
	hash := Hash(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := uuid.New()
		ref, err := s.Upload(ctx, hash, data, "application/octet-stream", mb)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := idx.Append(ctx, mb, fid, index.Record{MessageID: msg, Size: int64(len(data)), Attachments: []index.AttachmentRef{ref}}, nil); err != nil {
			b.Fatal(err)
		}
	}
}
