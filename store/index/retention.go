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
		return "", "", ErrInvalid
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
	return s.mapSet(ctx, mb, threadOID(mb, msg.String()), "loc/"+msg.String(), []byte(value))
}

// ListRetention returns bounded pages of due entries. Handles are opaque and
// must be passed unchanged to DropRetention.
func (s *Store) ListRetention(ctx context.Context, mb uuid.UUID, before time.Time, max int) (entries []RetentionEntry, keys []string, more bool, err error) {
	vr, err := s.getRoot(ctx, mb)
	if errors.Is(err, ErrNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
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
			if err := s.mapRemove(ctx, mb, threadOID(mb, entry.MessageID.String()), "loc/"+entry.MessageID.String()); err != nil {
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
	kv, err := s.mapGet(ctx, mb, threadOID(mb, msg.String()), []string{"loc/" + msg.String()})
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
	return false, nil
}

// NextRetention returns the earliest deadline without decoding all retained
// records. Each range is ordered by deadline, so one key per range suffices.
func (s *Store) NextRetention(ctx context.Context, mb uuid.UUID) (time.Time, error) {
	vr, err := s.getRoot(ctx, mb)
	if errors.Is(err, ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	var next time.Time
	read := func(oid string) error {
		kv, _, err := s.pool(ctx, mb).OmapList(ctx, oid, "", "", 1)
		if errors.Is(err, rados.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(kv) > 0 {
			var e RetentionEntry
			if err := json.Unmarshal(kv[0].Value, &e); err != nil {
				return err
			}
			if next.IsZero() || e.PurgeAt.Before(next) {
				next = e.PurgeAt
			}
		}
		return nil
	}
	if vr.root.RetentionNext > 0 {
		for shard := vr.root.RetentionFirst; shard <= (vr.root.RetentionNext-1)>>shardBits; shard++ {
			if err := read(retentionShardOID(mb, shard)); err != nil {
				return next, err
			}
		}
	}
	return next, nil
}
