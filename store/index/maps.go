package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"kittiwake/rados"
)

// Hash shards are entry points, not size bounds. Overflow nodes cap both key
// count and encoded key/value bytes, including for one enormous conversation.
const mapMaxKeys = 4096
const mapMaxBytes = 4 << 20
const mapMaxKeyBytes = 4096

type mapMeta struct {
	Format   int    `json:"format"`
	Keys     int    `json:"keys"`
	Bytes    int    `json:"bytes"`
	Children []byte `json:"children,omitempty"` // 256-bit child bitmap
}
type mapNode struct {
	version uint64
	oid     string
	meta    mapMeta
	missing bool
	updates map[string][]byte // nil means removed; staged read-your-writes
}

func childOID(base, key string, depth int) string {
	hash := sha256.Sum256([]byte(key))
	return base + "/" + hex.EncodeToString(hash[:depth])
}
func hasChild(n *mapNode, key string, depth int) bool {
	if depth >= 32 {
		return false
	}
	hash := sha256.Sum256([]byte(key))
	slot := hash[depth]
	return len(n.meta.Children) == 32 && n.meta.Children[slot/8]&(1<<(slot%8)) != 0
}
func (s *Store) mapLock(ctx context.Context) func() {
	if st, ok := ctx.Value(stageKey{}).(*staged); ok {
		st.mapMu.Lock()
		return st.mapMu.Unlock
	}
	return func() {}
}
func (s *Store) mapNode(ctx context.Context, mb uuid.UUID, oid string) (*mapNode, error) {
	st, _ := ctx.Value(stageKey{}).(*staged)
	if st != nil && st.maps != nil {
		if n := st.maps[oid]; n != nil {
			return n, nil
		}
	}
	n := &mapNode{oid: oid, updates: map[string][]byte{}}
	data, version, err := s.pool(ctx, mb).Get(ctx, oid)
	n.version = version
	n.missing = errors.Is(err, rados.ErrNotFound)
	if err != nil && !n.missing {
		return nil, err
	}
	if n.missing {
		n.meta.Format = 1
	} else {
		if err := json.Unmarshal(data, &n.meta); err != nil {
			return nil, fmt.Errorf("index map %s requires a fresh format-4 index: %w", oid, err)
		}
		if n.meta.Format != 1 || n.meta.Keys < 0 || n.meta.Keys > mapMaxKeys || n.meta.Bytes < 0 || n.meta.Bytes > mapMaxBytes || (len(n.meta.Children) != 0 && len(n.meta.Children) != 32) {
			return nil, errors.New("index: invalid map metadata")
		}
	}
	if st != nil {
		if st.maps == nil {
			st.maps = map[string]*mapNode{}
		}
		st.maps[oid] = n
	}
	return n, nil
}
func (s *Store) nodeValues(ctx context.Context, mb uuid.UUID, n *mapNode, keys []string) (map[string][]byte, error) {
	values := map[string][]byte{}
	var unknown []string
	for _, key := range keys {
		if _, known := n.updates[key]; !known {
			unknown = append(unknown, key)
		}
	}
	if !n.missing && len(unknown) > 0 {
		fetched, err := s.pool(ctx, mb).OmapGet(ctx, n.oid, unknown)
		if err != nil {
			return nil, err
		}
		for _, key := range unknown {
			n.updates[key] = fetched[key]
		}
	}
	for _, key := range keys {
		if value := n.updates[key]; value != nil {
			values[key] = value
		}
	}
	return values, nil
}
func (s *Store) writeMapNode(ctx context.Context, mb uuid.UUID, n *mapNode, tx *rados.WriteTx) error {
	if _, ok := ctx.Value(stageKey{}).(*staged); !ok {
		return errors.New("index: map writes require a root mutation")
	}
	data, err := json.Marshal(n.meta)
	if err != nil {
		return err
	}
	_, err = s.pool(ctx, mb).Write(ctx, n.oid, tx.WriteFull(data))
	if err == nil {
		st := ctx.Value(stageKey{}).(*staged)
		st.mu.Lock()
		w := st.writes[n.oid]
		w.Known = true
		w.Version = n.version
		w.Exists = !n.missing
		st.mu.Unlock()
	}
	return err
}
func (s *Store) mapGet(ctx context.Context, mb uuid.UUID, base string, keys []string) (map[string][]byte, error) {
	if ctx.Value(stageKey{}) == nil {
		return s.readMap(ctx, mb, base, base, keys, 0)
	}
	unlock := s.mapLock(ctx)
	defer unlock()
	out := map[string][]byte{}
	var read func(string, []string, int) error
	read = func(oid string, keys []string, depth int) error {
		n, err := s.mapNode(ctx, mb, oid)
		if err != nil {
			return err
		}
		values, err := s.nodeValues(ctx, mb, n, keys)
		if err != nil {
			return err
		}
		children := map[string][]string{}
		for _, key := range keys {
			if v, ok := values[key]; ok {
				out[key] = v
				continue
			}
			if hasChild(n, key, depth) {
				child := childOID(base, key, depth+1)
				children[child] = append(children[child], key)
			}
		}
		for child, keys := range children {
			if err := read(child, keys, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return out, read(base, keys, 0)
}
func (s *Store) mapSet(ctx context.Context, mb uuid.UUID, base, key string, value []byte) error {
	if len(key) > mapMaxKeyBytes || len(key)+len(value) > mapMaxBytes {
		return ErrInvalid
	}
	unlock := s.mapLock(ctx)
	defer unlock()
	var path []*mapNode
	oid := base
	for depth := 0; ; depth++ {
		n, err := s.mapNode(ctx, mb, oid)
		if err != nil {
			return err
		}
		path = append(path, n)
		values, err := s.nodeValues(ctx, mb, n, []string{key})
		if err != nil {
			return err
		}
		if old, ok := values[key]; ok {
			if n.meta.Bytes+len(value)-len(old) <= mapMaxBytes {
				n.meta.Bytes += len(value) - len(old)
				n.updates[key] = value
				return s.writeMapNode(ctx, mb, n, rados.NewWriteTx().OmapSet(map[string][]byte{key: value}))
			}
			// A growing value can move to an overflow node under the same journal.
			n.meta.Keys--
			n.meta.Bytes -= len(key) + len(old)
			n.updates[key] = nil
			if err := s.writeMapNode(ctx, mb, n, rados.NewWriteTx().OmapRemove(key)); err != nil {
				return err
			}
		}
		if !hasChild(n, key, depth) {
			break
		}
		oid = childOID(base, key, depth+1)
	}
	for _, n := range path {
		if n.meta.Keys < mapMaxKeys && n.meta.Bytes+len(key)+len(value) <= mapMaxBytes {
			n.meta.Keys++
			n.meta.Bytes += len(key) + len(value)
			n.updates[key] = value
			return s.writeMapNode(ctx, mb, n, rados.NewWriteTx().OmapSet(map[string][]byte{key: value}))
		}
	}
	depth := len(path) - 1
	if depth >= 32 {
		return errors.New("index: exhausted map hash depth")
	}
	parent := path[depth]
	if len(parent.meta.Children) == 0 {
		parent.meta.Children = make([]byte, 32)
	}
	hash := sha256.Sum256([]byte(key))
	slot := hash[depth]
	parent.meta.Children[slot/8] |= 1 << (slot % 8)
	if err := s.writeMapNode(ctx, mb, parent, rados.NewWriteTx()); err != nil {
		return err
	}
	child, err := s.mapNode(ctx, mb, childOID(base, key, depth+1))
	if err != nil {
		return err
	}
	child.meta.Keys++
	child.meta.Bytes += len(key) + len(value)
	child.updates[key] = value
	return s.writeMapNode(ctx, mb, child, rados.NewWriteTx().OmapSet(map[string][]byte{key: value}))
}
func (s *Store) mapRemove(ctx context.Context, mb uuid.UUID, base, key string) error {
	unlock := s.mapLock(ctx)
	defer unlock()
	oid := base
	for depth := 0; ; depth++ {
		n, err := s.mapNode(ctx, mb, oid)
		if err != nil {
			return err
		}
		values, err := s.nodeValues(ctx, mb, n, []string{key})
		if err != nil {
			return err
		}
		if value, ok := values[key]; ok {
			n.meta.Keys--
			n.meta.Bytes -= len(key) + len(value)
			n.updates[key] = nil
			return s.writeMapNode(ctx, mb, n, rados.NewWriteTx().OmapRemove(key))
		}
		if !hasChild(n, key, depth) {
			return nil
		}
		oid = childOID(base, key, depth+1)
	}
}
func (s *Store) walkMap(ctx context.Context, mb uuid.UUID, base string, visit func(string) error) error {
	var walk func(string, string) error
	walk = func(oid, suffix string) error {
		n, err := s.mapNode(ctx, mb, oid)
		if err != nil {
			return err
		}
		if n.missing {
			return nil
		}
		for i := 0; i < 256; i++ {
			if len(n.meta.Children) == 32 && n.meta.Children[i/8]&(1<<uint(i%8)) != 0 {
				next := suffix + fmt.Sprintf("%02x", i)
				if err := walk(base+"/"+next, next); err != nil {
					return err
				}
			}
		}
		return visit(oid)
	}
	return walk(base, "")
}
func (s *Store) mapList(ctx context.Context, mb uuid.UUID, base, prefix string) ([]rados.KV, error) {
	var out []rados.KV
	err := s.walkMap(ctx, mb, base, func(oid string) error {
		var after string
		for {
			kv, more, err := s.pool(ctx, mb).OmapList(ctx, oid, prefix, after, 512)
			if err != nil {
				return err
			}
			out = append(out, kv...)
			if !more || len(kv) == 0 {
				return nil
			}
			after = kv[len(kv)-1].Key
		}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, err
}

type messageIDMapping struct {
	ThreadID string `json:"thread"`
	Count    int64  `json:"count"`
}

func (s *Store) changeMessageID(ctx context.Context, mb uuid.UUID, header, tid string, delta int64) error {
	base, key := threadOID(mb, header), msgidKey(header)
	values, err := s.mapGet(ctx, mb, base, []string{key})
	if err != nil {
		return err
	}
	mapping := messageIDMapping{ThreadID: tid}
	if data, ok := values[key]; ok {
		if err := json.Unmarshal(data, &mapping); err != nil {
			return err
		}
	}
	if mapping.ThreadID != tid || mapping.Count+delta < 0 {
		return errors.New("index: inconsistent Message-ID ownership")
	}
	mapping.Count += delta
	if mapping.Count == 0 {
		return s.mapRemove(ctx, mb, base, key)
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}
	return s.mapSet(ctx, mb, base, key, data)
}

// Most point lookups hit the entry shard. Read OMAP first and fetch the overflow
// bitmap only on a miss, keeping ordinary location reads to one RADOS operation.
func (s *Store) readMap(ctx context.Context, mb uuid.UUID, base, oid string, keys []string, depth int) (map[string][]byte, error) {
	values, err := s.pool(ctx, mb).OmapGet(ctx, oid, keys)
	if errors.Is(err, rados.ErrNotFound) {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, key := range keys {
		if _, ok := values[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return values, nil
	}
	n, err := s.mapNode(ctx, mb, oid)
	if err != nil {
		return nil, err
	}
	children := map[string][]string{}
	for _, key := range missing {
		if hasChild(n, key, depth) {
			child := childOID(base, key, depth+1)
			children[child] = append(children[child], key)
		}
	}
	for child, group := range children {
		found, err := s.readMap(ctx, mb, base, child, group, depth+1)
		if err != nil {
			return nil, err
		}
		for key, value := range found {
			values[key] = value
		}
	}
	return values, nil
}
