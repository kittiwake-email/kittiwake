package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"kittiwake/rados"
)

const segmentKeys = 4096
const segmentBytes = 4 << 20

var ErrExpired = errors.New("audit: date already retired")

type dayHead struct {
	Format int    `json:"format"`
	Last   uint64 `json:"last"`
	First  uint64 `json:"first"`
	Closed bool   `json:"closed,omitempty"`
}
type segmentHead struct {
	Keys   int  `json:"keys"`
	Bytes  int  `json:"bytes"`
	Closed bool `json:"closed,omitempty"`
}
type catalog struct {
	Start string `json:"start"`
	First string `json:"first"`
	Last  string `json:"last"`
}

func segmentOID(base string, n uint64) string { return fmt.Sprintf("%s/%016x", base, n) }
func catalogOID(base string) string           { return base[:strings.LastIndex(base, "/")+1] + "_catalog" }
func encoded(v any) []byte                    { b, _ := json.Marshal(v); return b }
func loadJSON(ctx context.Context, io rados.IOContext, oid string, v any) (uint64, error) {
	b, ver, err := io.Get(ctx, oid)
	if err != nil {
		return ver, err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return ver, fmt.Errorf("audit metadata %s (reset pre-segment lab data): %w", oid, err)
	}
	return ver, nil
}
func conditional(ver uint64, missing bool) *rados.WriteTx {
	tx := rados.NewWriteTx()
	if missing {
		return tx.CreateExclusive()
	}
	return tx.AssertVersion(ver)
}
func conflict(err error) bool {
	return errors.Is(err, rados.ErrVersionMismatch) || errors.Is(err, rados.ErrExists)
}

// Register the day before writing: a crash cannot hide a partially written day
// from expiry. A retired day is permanently closed, fencing delayed writers.
func registerDay(ctx context.Context, io rados.IOContext, base string) error {
	ds := base[strings.LastIndex(base, "/")+1:]
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var c catalog
		ver, err := loadJSON(ctx, io, catalogOID(base), &c)
		missing := errors.Is(err, rados.ErrNotFound)
		if err != nil && !missing {
			return err
		}
		if c.First != "" && c.First <= ds && ds <= c.Last {
			return nil
		}
		if c.Start == "" || ds < c.Start {
			c.Start = ds
		}
		if c.First == "" || ds < c.First {
			c.First = ds
		}
		if ds > c.Last {
			c.Last = ds
		}
		_, err = io.Write(ctx, catalogOID(base), conditional(ver, missing).WriteFull(encoded(c)))
		if conflict(err) {
			continue
		}
		return err
	}
}

func appendSegmented(ctx context.Context, io rados.IOContext, base string, kv map[string][]byte) error {
	if len(kv) == 0 {
		return nil
	}
	keys := make([]string, 0, len(kv))
	for k, v := range kv {
		if len(k)+len(v) > segmentBytes {
			return errors.New("audit: record exceeds segment byte limit")
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if err := registerDay(ctx, io, base); err != nil {
		return err
	}
	for len(keys) > 0 {
		chunk := map[string][]byte{}
		size, n := 0, 0
		for n < len(keys) && n < segmentKeys {
			k := keys[n]
			b := len(k) + len(kv[k])
			if size+b > segmentBytes {
				break
			}
			chunk[k] = kv[k]
			size += b
			n++
		}
		if err := appendChunk(ctx, io, base, chunk, size); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}
func appendChunk(ctx context.Context, io rados.IOContext, base string, kv map[string][]byte, size int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var h dayHead
		hv, err := loadJSON(ctx, io, base, &h)
		if errors.Is(err, rados.ErrNotFound) {
			_, err = io.Write(ctx, base, rados.NewWriteTx().CreateExclusive().WriteFull(encoded(dayHead{Format: 1})))
			if err == nil || conflict(err) {
				continue
			}
			return err
		}
		if err != nil {
			return err
		}
		if h.Format != 1 {
			return errors.New("audit: unknown segment format")
		}
		if h.Closed {
			return ErrExpired
		}
		oid := segmentOID(base, h.Last)
		var seg segmentHead
		sv, err := loadJSON(ctx, io, oid, &seg)
		missing := errors.Is(err, rados.ErrNotFound)
		if err != nil && !missing {
			return err
		}
		if seg.Closed {
			return ErrExpired
		}
		if seg.Keys+len(kv) > segmentKeys || seg.Bytes+size > segmentBytes {
			h.Last++
			_, err = io.Write(ctx, base, rados.NewWriteTx().AssertVersion(hv).WriteFull(encoded(h)))
			if err == nil || conflict(err) {
				continue
			}
			return err
		}
		seg.Keys += len(kv)
		seg.Bytes += size
		_, err = io.Write(ctx, oid, conditional(sv, missing).WriteFull(encoded(seg)).OmapSet(kv))
		if conflict(err) {
			continue
		}
		return err
	}
}

// Retiring clears data but retains small version fences, so an already-issued
// exclusive create or CAS cannot repopulate a purged segment. Whole-mailbox
// removal deletes these fences after removing the mailbox registry entry.
func retireDay(ctx context.Context, io rados.IOContext, base string) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var h dayHead
		ver, err := loadJSON(ctx, io, base, &h)
		missing := errors.Is(err, rados.ErrNotFound)
		if err != nil && !missing {
			return false, err
		}
		if !h.Closed {
			h.Closed = true
			h.Format = 1
			if missing {
				h.First = 1
			} // no allocated segment
			_, err = io.Write(ctx, base, conditional(ver, missing).WriteFull(encoded(h)))
			if conflict(err) {
				continue
			}
			if err != nil {
				return false, err
			}
		}
		hadData := h.First <= h.Last
		for h.First <= h.Last {
			_, err := io.Write(ctx, segmentOID(base, h.First), rados.NewWriteTx().OmapClear().WriteFull(encoded(segmentHead{Closed: true})))
			if err != nil {
				return false, err
			}
			// Persist progress after every segment; another collector may be ahead.
			for {
				var current dayHead
				cv, err := loadJSON(ctx, io, base, &current)
				if err != nil {
					return false, err
				}
				if current.First > h.First {
					h = current
					break
				}
				current.First = h.First + 1
				_, err = io.Write(ctx, base, rados.NewWriteTx().AssertVersion(cv).WriteFull(encoded(current)))
				if conflict(err) {
					continue
				}
				if err != nil {
					return false, err
				}
				h = current
				break
			}
		}
		return hadData, nil
	}
}

func purgeSegmented(ctx context.Context, io rados.IOContext, cutoff time.Time, days int, oid func(string) string) (int, error) {
	catOID := catalogOID(oid(day(cutoff)))
	n := 0
	for i := 0; i < days; i++ {
		var c catalog
		ver, err := loadJSON(ctx, io, catOID, &c)
		if errors.Is(err, rados.ErrNotFound) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if c.First == "" || c.First > c.Last || c.First >= day(cutoff) {
			return n, nil
		}
		d, err := time.Parse("2006-01-02", c.First)
		if err != nil {
			return n, err
		}
		removed, err := retireDay(ctx, io, oid(c.First))
		if err != nil {
			return n, err
		}
		if removed {
			n++
		}
		c.First = day(d.AddDate(0, 0, 1))
		_, err = io.Write(ctx, catOID, rados.NewWriteTx().AssertVersion(ver).WriteFull(encoded(c)))
		if conflict(err) {
			continue
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
