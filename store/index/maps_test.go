package index

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

func testMapBounds(t *testing.T, s *Store, mb uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	base := threadOID(mb, "large-thread")
	kv := map[string][]byte{}
	size := 0
	for i := 0; i < mapMaxKeys; i++ {
		key := fmt.Sprintf("k/%06d", i)
		kv[key] = []byte("value")
		size += len(key) + len(kv[key])
	}
	data, _ := json.Marshal(mapMeta{Format: 1, Keys: mapMaxKeys, Bytes: size})
	if _, err := s.pool(ctx, mb).Write(ctx, base, rados.NewWriteTx().WriteFull(data).OmapSet(kv)); err != nil {
		t.Fatal(err)
	}
	if err := s.mutate(ctx, mb, func(ctx context.Context) error {
		for _, key := range []string{"overflow/one", "overflow/two"} {
			if err := s.mapSet(ctx, mb, base, key, []byte("new")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.mapGet(ctx, mb, base, []string{"k/000000", "overflow/one", "overflow/two"})
	if err != nil || len(got) != 3 {
		t.Fatalf("overflow lookup: %v %v", got, err)
	}
	nodes := 0
	keys := 0
	if err := s.walkMap(ctx, mb, base, func(oid string) error {
		nodes++
		n, err := s.mapNode(ctx, mb, oid)
		if err != nil {
			return err
		}
		values, _, err := s.pool(ctx, mb).OmapList(ctx, oid, "", "", 0)
		if err != nil {
			return err
		}
		if len(values) != n.meta.Keys || n.meta.Keys > mapMaxKeys || n.meta.Bytes > mapMaxBytes {
			t.Errorf("bad bound/count: %s %+v %d", oid, n.meta, len(values))
		}
		keys += len(values)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if nodes < 2 || keys != mapMaxKeys+2 {
		t.Fatalf("nodes=%d keys=%d", nodes, keys)
	}
	if err := s.mutate(ctx, mb, func(ctx context.Context) error {
		if err := s.mapRemove(ctx, mb, base, "k/000000"); err != nil {
			return err
		}
		return s.mapSet(ctx, mb, base, "replacement", []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	direct, err := s.pool(ctx, mb).OmapGet(ctx, base, []string{"replacement"})
	if err != nil || string(direct["replacement"]) != "value" {
		t.Fatalf("empty slots not reused: %v %v", direct, err)
	}
	listed, err := s.mapList(ctx, mb, base, "overflow/")
	if err != nil || len(listed) != 2 {
		t.Fatalf("overflow listing: %v %v", listed, err)
	}
	if err := s.DeleteAll(ctx, mb); err != nil {
		t.Fatal(err)
	}
	if err := s.pool(ctx, mb).List(ctx, func(oid string) error {
		if strings.HasPrefix(oid, base) {
			t.Errorf("map left after mailbox deletion: %s", oid)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestMapBoundsAndOverflow(t *testing.T) { s, mb := newStore(t); testMapBounds(t, s, mb) }
func TestMessageIDMappingsCountLiveCopies(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	root, _ := s.GetRoot(ctx, mb)
	fid := root.Folders[0].ID
	a, err := s.Append(ctx, mb, fid, rec(1, "same"), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Append(ctx, mb, fid, rec(1, "same"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.ThreadID != b.ThreadID {
		t.Fatal("copies did not share thread")
	}
	if _, err := s.Remove(ctx, mb, fid, a.UID, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	reply, err := s.Append(ctx, mb, fid, rec(1, "reply", "same"), nil)
	if err != nil || reply.ThreadID != b.ThreadID {
		t.Fatalf("mapping removed too early: %+v %v", reply, err)
	}
	if _, err := s.Remove(ctx, mb, fid, b.UID, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	mapping, err := s.mapGet(ctx, mb, threadOID(mb, "same"), []string{msgidKey("same")})
	if err != nil || len(mapping) != 0 {
		t.Fatalf("historical mapping leaked: %v %v", mapping, err)
	}
}
func TestMapByteLimit(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	base := threadOID(mb, "bytes")
	value := []byte(strings.Repeat("v", mapMaxBytes/2))
	if err := s.mutate(ctx, mb, func(ctx context.Context) error {
		if err := s.mapSet(ctx, mb, base, "a", value); err != nil {
			return err
		}
		return s.mapSet(ctx, mb, base, "b", value)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.walkMap(ctx, mb, base, func(oid string) error {
		n, err := s.mapNode(ctx, mb, oid)
		if err == nil && n.meta.Bytes > mapMaxBytes {
			t.Error("byte cap exceeded")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
