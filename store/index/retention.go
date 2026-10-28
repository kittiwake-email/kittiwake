package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
)

func retentionShardOID(mb uuid.UUID, shard uint64) string {
	return fmt.Sprintf("%s/retention.%016x", mb, shard)
}
func retentionHandle(shard uint64, key string) string { return fmt.Sprintf("s/%016x/%s", shard, key) }
func retentionTarget(mb uuid.UUID, handle string) (string, string, error) {
	if !strings.HasPrefix(handle, "s/") {
		return retentionOID(mb), handle, nil
	}
	parts := strings.SplitN(handle, "/", 3)
	if len(parts) != 3 {
		return "", "", ErrInvalid
	}
	n, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return "", "", ErrInvalid
	}
	return retentionShardOID(mb, n), parts[2], nil
}
func (s *Store) setRetainedLocation(ctx context.Context, mb, msg uuid.UUID, seq uint64, key string) error {
	value := "retained/" + retentionHandle(seq>>shardBits, key)
	_, err := s.pool(ctx, mb).Write(ctx, threadOID(mb, msg.String()), rados.NewWriteTx().OmapSet(map[string][]byte{"loc/" + msg.String(): []byte(value)}))
	return err
}

// ListRetention returns bounded pages of due entries. Handles are opaque and
// must be passed unchanged to DropRetention. Legacy retention is drained first.
func (s *Store) ListRetention(ctx context.Context, mb uuid.UUID, before time.Time, max int) (entries []RetentionEntry, keys []string, more bool, err error) {
	vr, err := s.getRoot(ctx, mb)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, nil, false, err
	}
	read := func(oid string, shard *uint64) (bool, error) {
		want := max
		if max > 0 {
			want = max - len(entries) + 1
		}
		kv, more, e := s.pool(ctx, mb).OmapList(ctx, oid, "", "", want)
		if errors.Is(e, rados.ErrNotFound) {
			return false, nil
		}
		if e != nil {
			return false, e
		}
		for _, item := range kv {
			var entry RetentionEntry
			if e := json.Unmarshal(item.Value, &entry); e != nil {
				return false, e
			}
			if !before.IsZero() && entry.PurgeAt.After(before) {
				return false, nil
			}
			if max > 0 && len(entries) == max {
				return true, nil
			}
			entries = append(entries, entry)
			key := item.Key
			if shard != nil {
				key = retentionHandle(*shard, key)
			}
			keys = append(keys, key)
		}
		return more, nil
	}
	if more, err = read(retentionOID(mb), nil); err != nil || more {
		return
	}
	if vr.root.RetentionNext == 0 {
		return
	}
	last := (vr.root.RetentionNext - 1) >> shardBits
	for shard := vr.root.RetentionFirst; shard <= last; shard++ {
		if more, err = read(retentionShardOID(mb, shard), &shard); err != nil || more {
			return
		}
	}
	return
}

func (s *Store) dropRetention(ctx context.Context, mb uuid.UUID, handles ...string) error {
	groups := map[string][]string{}
	for _, h := range handles {
		oid, key, err := retentionTarget(mb, h)
		if err != nil {
			return err
		}
		groups[oid] = append(groups[oid], key)
	}
	for oid, keys := range groups {
		values, err := s.pool(ctx, mb).OmapGet(ctx, oid, keys)
		if err != nil && !errors.Is(err, rados.ErrNotFound) {
			return err
		}
		for _, value := range values {
			var entry RetentionEntry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			// DropRetention's contract requires physical deletion first. Removing the
			// key under the root journal advances the shard version, fencing old helpers;
			// the retained shard object must not be deleted or recreated.
			if _, err := s.pool(ctx, mb).Write(ctx, threadOID(mb, entry.MessageID.String()), rados.NewWriteTx().OmapRemove("loc/"+entry.MessageID.String())); err != nil {
				return err
			}
		}
		if _, err := s.pool(ctx, mb).Write(ctx, oid, rados.NewWriteTx().OmapRemove(keys...)); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return err
		}
	}
	// Advance the low watermark without deleting shard objects: their versions
	// fence late mutation helpers. Include staged removals when checking emptiness.
	_, err := s.updateRoot(ctx, mb, func(root *Root) error {
		if root.RetentionNext == 0 {
			return nil
		}
		last := (root.RetentionNext - 1) >> shardBits
		for root.RetentionFirst < last {
			oid := retentionShardOID(mb, root.RetentionFirst)
			removed := map[string]bool{}
			for _, k := range groups[oid] {
				removed[k] = true
			}
			kv, _, err := s.pool(ctx, mb).OmapList(ctx, oid, "", "", len(removed)+1)
			if err != nil && !errors.Is(err, rados.ErrNotFound) {
				return err
			}
			for _, item := range kv {
				if !removed[item.Key] {
					return nil
				}
			}
			root.RetentionFirst++
		}
		return nil
	})
	return err
}

// IsRetained uses the authoritative location's retention handle for new entries.
// Legacy entries keep their old lookup until they are purged.
func (s *Store) IsRetained(ctx context.Context, mb, msg uuid.UUID) (bool, error) {
	if _, err := s.getRoot(ctx, mb); err != nil && !errors.Is(err, ErrNotFound) {
		return false, err
	}
	kv, err := s.pool(ctx, mb).OmapGet(ctx, threadOID(mb, msg.String()), []string{"loc/" + msg.String()})
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return false, err
	}
	loc := string(kv["loc/"+msg.String()])
	if strings.HasPrefix(loc, "retained/") {
		oid, key, err := retentionTarget(mb, strings.TrimPrefix(loc, "retained/"))
		if err != nil {
			return false, err
		}
		entries, err := s.pool(ctx, mb).OmapGet(ctx, oid, []string{key})
		if errors.Is(err, rados.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		_, ok := entries[key]
		return ok, nil
	}
	// Legacy data only; never scan new shards for a point membership check.
	entries, _, err := s.pool(ctx, mb).OmapList(ctx, retentionOID(mb), "", "", 0)
	if errors.Is(err, rados.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		var r RetentionEntry
		if err := json.Unmarshal(entry.Value, &r); err != nil {
			return false, err
		}
		if r.MessageID == msg {
			return true, nil
		}
	}
	return false, nil
}
