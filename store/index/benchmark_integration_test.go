//go:build integration

package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

func benchmarkCeph(b testing.TB) (*Store, rados.IOContext, uuid.UUID, string) {
	b.Helper()
	conf := os.Getenv("KITTIWAKE_TEST_CEPH_CONF")
	if conf == "" {
		b.Skip("set KITTIWAKE_TEST_CEPH_CONF to run isolated Ceph benchmarks")
	}
	client, err := rados.Connect(rados.Config{ConfigFile: conf, User: "kittiwake"})
	if err != nil {
		b.Fatal(err)
	}
	base, err := client.Pool("kittiwake.index")
	if err != nil {
		b.Fatal(err)
	}
	io := base.Namespace(fmt.Sprintf("review-%d", time.Now().UnixNano()))
	b.Cleanup(func() {
		_ = io.List(context.Background(), func(oid string) error { return io.Delete(context.Background(), oid) })
		_ = client.Close()
	})
	s := New(io)
	mb := uuid.New()
	root, err := s.EnsureRoot(context.Background(), mb)
	if err != nil {
		b.Fatal(err)
	}
	return s, io, mb, root.Folders[0].ID
}
func BenchmarkCephAppend(b *testing.B) {
	s, _, mb, fid := benchmarkCeph(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Append(ctx, mb, fid, Record{MessageID: uuid.New(), Size: 1024}, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkCephFlags(b *testing.B) {
	s, _, mb, fid := benchmarkCeph(b)
	ctx := context.Background()
	r, err := s.Append(ctx, mb, fid, Record{MessageID: uuid.New(), Size: 1024}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.SetFlags(ctx, mb, fid, r.UID, func(f []string) []string {
			if len(f) == 0 {
				return []string{FlagSeen}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkCephScan150k(b *testing.B) {
	for _, desc := range []bool{false, true} {
		name := "ascending"
		if desc {
			name = "descending"
		}
		b.Run(name, func(b *testing.B) {
			s, io, mb, fid := benchmarkCeph(b)
			ctx := context.Background()
			_, err := s.updateRoot(ctx, mb, func(r *Root) error {
				f := r.FolderByID(fid)
				f.UIDNext = 150001
				f.Total = 150000
				r.Usage.Messages = 150000
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			byOID := map[string]map[string][]byte{}
			for uid := uint32(1); uid <= 150000; uid++ {
				oid := folderOID(mb, fid, uid)
				if byOID[oid] == nil {
					byOID[oid] = map[string][]byte{}
				}
				data, _ := json.Marshal(Record{UID: uid, MessageID: uuid.New(), Size: 1024, Subject: "benchmark message", InternalDate: time.Unix(1000, 0)})
				byOID[oid][uidKey(uid)] = data
			}
			for oid, kv := range byOID {
				if _, err := io.Write(ctx, oid, rados.NewWriteTx().OmapSet(kv)); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cursor := uint32(0)
				total := 0
				for {
					var rs []Record
					var more bool
					var err error
					if desc {
						rs, more, err = s.ListFolderDesc(ctx, mb, fid, cursor, 1000)
					} else {
						rs, more, err = s.ListFolder(ctx, mb, fid, cursor, 1000)
					}
					if err != nil {
						b.Fatal(err)
					}
					total += len(rs)
					if len(rs) > 0 {
						cursor = rs[len(rs)-1].UID
					}
					if !more {
						break
					}
				}
				if total != 150000 {
					b.Fatalf("scan count %d", total)
				}
			}
		})
	}
}

// Exercise durable recovery and complete reads against real librados, using only
// the isolated benchmark namespace.
func TestCephReviewRecoveryAndReads(t *testing.T) {
	s, io, mb, fid := benchmarkCeph(t)
	ctx := context.Background()
	p := &failIndexWrite{IOContext: io, oid: folderOID(mb, fid, 1)}
	p.fail.Store(true)
	writer := New(p)
	_, err := writer.Append(ctx, mb, fid, Record{MessageID: uuid.New(), Size: 1024}, nil)
	if !errors.Is(err, ErrPending) {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := s.GetRoot(ctx, mb); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRecord(ctx, mb, fid, 1); err != nil {
		t.Fatal(err)
	}
	old := bytes.Repeat([]byte("a"), 32<<10)
	large := bytes.Repeat([]byte("b"), 300<<10)
	if _, err := io.Write(ctx, "read-race", rados.NewWriteTx().WriteFull(old)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			data := old
			if i%2 == 0 {
				data = large
			}
			if _, err := io.Write(ctx, "read-race", rados.NewWriteTx().WriteFull(data)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 100; i++ {
		data, _, err := io.Get(ctx, "read-race")
		if err != nil {
			t.Error(err)
			break
		}
		if !bytes.Equal(data, old) && !bytes.Equal(data, large) {
			t.Errorf("torn read length %d", len(data))
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
