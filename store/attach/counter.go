package attach

// Counter format 2 separates immutable payloads from reference bookkeeping.
// Each hash has 256 mailbox-derived deduplication partitions. A partition has
// one scalar counter and one recoverable mutation, never an unbounded OMAP.
// Forward receipts (one small object per message/attachment) make retries exact;
// mailbox counters authorize downloads without enumerating messages. Released
// receipts are retained to fence delayed retries. Old hash-named objects remain
// readable and are reclaimed by the legacy collector.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"kittiwake/rados"
	"kittiwake/store/storage"
)

type counterState struct {
	Format      int              `json:"format"`
	Generation  string           `json:"generation"`
	Count       int64            `json:"count"`
	Size        uint64           `json:"size"`
	ContentType string           `json:"content_type"`
	Created     time.Time        `json:"created"`
	Touched     time.Time        `json:"touched"`
	Deleting    bool             `json:"deleting,omitempty"`
	Pending     *counterMutation `json:"pending,omitempty"`
}
type counterSnapshot struct {
	state   counterState
	version uint64
}

type counterMutation struct {
	ID      string          `json:"id"`
	Count   int64           `json:"count"`
	Touched time.Time       `json:"touched"`
	Targets []counterTarget `json:"targets"`
}
type counterTarget struct {
	OID     string          `json:"oid"`
	Version uint64          `json:"version"`
	Exists  bool            `json:"exists"`
	Data    json.RawMessage `json:"data"`
}
type receipt struct {
	Active  bool      `json:"active"`
	Created time.Time `json:"created"`
}
type ownerCount struct {
	Count int64 `json:"count"`
}

func counterID(hash string, mb uuid.UUID) string { return fmt.Sprintf("v2/%s/%02x", hash, mb[0]) }
func counterOID(id string) string                { return id + "/counter" }
func payloadOID(id, generation string) string    { return id + "/data/" + generation }
func receiptOID(hash string, mb, msg uuid.UUID) string {
	return "receipt/" + mb.String() + "/" + msg.String() + "/" + hash
}
func ownerOID(hash string, mb uuid.UUID) string { return "owner/" + mb.String() + "/" + hash }
func validHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	for _, c := range hash {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func writeJSON(v any) []byte { b, _ := json.Marshal(v); return b }
func readJSON(ctx context.Context, io rados.IOContext, oid string, out any) (uint64, error) {
	b, v, err := io.Get(ctx, oid)
	if err != nil {
		return 0, err
	}
	if err = json.Unmarshal(b, out); err != nil {
		return 0, fmt.Errorf("attachment metadata %s: %w", oid, err)
	}
	return v, nil
}
func (s *Store) loadCounter(ctx context.Context, io rados.IOContext, id string) (counterState, uint64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return counterState{}, 0, err
		}
		var st counterState
		v, err := readJSON(ctx, io, counterOID(id), &st)
		if err != nil {
			return st, v, err
		}
		if st.Format != 2 || st.Count < 0 || (st.Generation == "" && st.Count != 0) || (st.Pending != nil && (len(st.Pending.Targets) != 2 || st.Pending.Count < 0 || st.Deleting)) {
			return st, v, errors.New("attach: invalid counter metadata")
		}
		if st.Pending == nil {
			return st, v, nil
		}
		if err := s.recoverCounter(ctx, io, id, st.Pending); err != nil {
			return st, v, err
		}
	}
}
func (s *Store) recoverCounter(ctx context.Context, io rados.IOContext, id string, m *counterMutation, known ...counterSnapshot) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(m.Targets))
	for _, target := range m.Targets {
		wg.Add(1)
		go func(target counterTarget) {
			defer wg.Done()
			errs <- func() error {
				tx := rados.NewWriteTx()
				if target.Exists {
					tx.AssertVersion(target.Version)
				} else {
					tx.CreateExclusive()
				}
				tx.WriteFull(target.Data).SetXattr("mutation", []byte(m.ID))
				if _, err := io.Write(ctx, target.OID, tx); err != nil {
					if !errors.Is(err, rados.ErrVersionMismatch) && !errors.Is(err, rados.ErrExists) {
						return err
					}
					attrs, aerr := io.GetXattrs(ctx, target.OID)
					if aerr == nil && string(attrs["mutation"]) == m.ID {
						return nil
					}
					var current counterState
					if _, e := readJSON(ctx, io, counterOID(id), &current); e == nil && (current.Pending == nil || current.Pending.ID != m.ID) {
						return nil
					}
					return fmt.Errorf("attachment reference recovery %s: %w", id, err)
				}
				return nil
			}()
		}(target)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	for {
		var st counterState
		var v uint64
		var err error
		if len(known) > 0 {
			st, v = known[0].state, known[0].version
			known = nil
		} else {
			v, err = readJSON(ctx, io, counterOID(id), &st)
		}
		if err != nil {
			return err
		}
		if st.Pending == nil || st.Pending.ID != m.ID {
			return nil
		}
		st.Count, st.Touched, st.Pending = m.Count, m.Touched, nil
		_, err = io.Write(ctx, counterOID(id), rados.NewWriteTx().AssertVersion(v).WriteFull(writeJSON(st)))
		if errors.Is(err, rados.ErrVersionMismatch) {
			continue
		}
		return err
	}
}
func (s *Store) putCounter(ctx context.Context, hash string, data []byte, contentType string, mb, msg uuid.UUID) error {
	if Hash(data) != hash {
		return ErrHash
	}
	io, id := s.pool(ctx, mb), counterID(hash, mb)
	for attempt := 0; attempt < 128; attempt++ {
		st, v, err := s.loadCounter(ctx, io, id)
		missing := errors.Is(err, rados.ErrNotFound)
		if err != nil && !missing {
			return err
		}
		if st.Deleting {
			if _, err := s.collectCounter(ctx, io, id, 0); err != nil {
				return err
			}
			continue
		}
		if st.Generation == "" {
			now := s.Now().UTC()
			st = counterState{Format: 2, Generation: uuid.NewString(), Size: uint64(len(data)), ContentType: contentType, Created: now, Touched: now}
			tx := rados.NewWriteTx()
			if missing {
				tx.CreateExclusive()
			} else {
				tx.AssertVersion(v)
			}
			v, err = io.Write(ctx, counterOID(id), tx.WriteFull(writeJSON(st)))
			if errors.Is(err, rados.ErrVersionMismatch) || errors.Is(err, rados.ErrExists) {
				continue
			}
			if err != nil {
				return err
			}
		}
		if st.Count == 0 {
			_, err = io.Write(ctx, payloadOID(id, st.Generation), rados.NewWriteTx().CreateExclusive().WriteFull(data))
			if err != nil && !errors.Is(err, rados.ErrExists) {
				return err
			}
		}
		err = s.changeCounter(ctx, io, id, hash, mb, msg, true, st.Generation, counterSnapshot{st, v})
		if errors.Is(err, ErrNotFound) {
			// A collector retired this generation before our reservation. Its
			// unique name can never become current again.
			_ = io.Delete(ctx, payloadOID(id, st.Generation))
			continue
		}
		return err
	}
	return errors.New("attach: counter contention")
}
func (s *Store) changeCounter(ctx context.Context, io rados.IOContext, id, hash string, mb, msg uuid.UUID, add bool, generation string, known ...counterSnapshot) error {
	for attempt := 0; attempt < 128; attempt++ {
		var st counterState
		var v uint64
		var err error
		if len(known) > 0 {
			st, v = known[0].state, known[0].version
			known = nil
		} else {
			st, v, err = s.loadCounter(ctx, io, id)
		}
		if err != nil {
			return mapErr(err)
		}
		if st.Generation == "" || st.Deleting || (generation != "" && generation != st.Generation) {
			return ErrNotFound
		}
		var r receipt
		var owner ownerCount
		var rv, ov uint64
		var re, oe error
		var reads sync.WaitGroup
		reads.Add(2)
		go func() { defer reads.Done(); rv, re = readJSON(ctx, io, receiptOID(hash, mb, msg), &r) }()
		go func() { defer reads.Done(); ov, oe = readJSON(ctx, io, ownerOID(hash, mb), &owner) }()
		reads.Wait()
		if re != nil && !errors.Is(re, rados.ErrNotFound) {
			return re
		}
		if add && r.Active || !add && !r.Active {
			return nil
		}
		if add && re == nil {
			return errors.New("attach: reference already released")
		}
		if oe != nil && !errors.Is(oe, rados.ErrNotFound) {
			return oe
		}
		delta := int64(-1)
		if add {
			delta = 1
		}
		if st.Count+delta < 0 || owner.Count+delta < 0 {
			return errors.New("attach: corrupt reference count")
		}
		now := s.Now().UTC()
		r.Active = add
		if r.Created.IsZero() {
			r.Created = now
		}
		owner.Count += delta
		st.Pending = &counterMutation{ID: uuid.NewString(), Count: st.Count + delta, Touched: now, Targets: []counterTarget{
			{OID: receiptOID(hash, mb, msg), Version: rv, Exists: re == nil, Data: writeJSON(r)},
			{OID: ownerOID(hash, mb), Version: ov, Exists: oe == nil, Data: writeJSON(owner)},
		}}
		v, err = io.Write(ctx, counterOID(id), rados.NewWriteTx().AssertVersion(v).WriteFull(writeJSON(st)))
		if errors.Is(err, rados.ErrVersionMismatch) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(min(attempt+1, 20)) * time.Millisecond):
			}
			continue
		}
		if err != nil {
			return err
		}
		return s.recoverCounter(ctx, io, id, st.Pending, counterSnapshot{st, v})
	}
	return errors.New("attach: counter contention")
}
func (s *Store) counterInfo(ctx context.Context, io rados.IOContext, id string) (Info, error) {
	st, v, err := s.loadCounter(ctx, io, id)
	if err != nil {
		return Info{}, mapErr(err)
	}
	if st.Generation == "" {
		return Info{}, ErrNotFound
	}
	return Info{Hash: id, Size: st.Size, ContentType: st.ContentType, Created: st.Created, Touched: st.Touched, Refs: int(st.Count), Version: v}, nil
}
func (s *Store) collectCounter(ctx context.Context, io rados.IOContext, id string, grace time.Duration) (bool, error) {
	st, v, err := s.loadCounter(ctx, io, id)
	if errors.Is(err, rados.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if st.Generation == "" || st.Count != 0 {
		return false, nil
	}
	if !st.Deleting {
		if s.Now().Sub(st.Touched) < grace {
			return false, nil
		}
		st.Deleting = true
		v, err = io.Write(ctx, counterOID(id), rados.NewWriteTx().AssertVersion(v).WriteFull(writeJSON(st)))
		if errors.Is(err, rados.ErrVersionMismatch) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	if err := io.Delete(ctx, payloadOID(id, st.Generation)); err != nil && !errors.Is(err, rados.ErrNotFound) {
		return false, err
	}
	st.Generation = ""
	st.Deleting = false
	_, err = io.Write(ctx, counterOID(id), rados.NewWriteTx().AssertVersion(v).WriteFull(writeJSON(st)))
	if errors.Is(err, rados.ErrVersionMismatch) {
		return false, nil
	}
	return err == nil, err
}

// ReconcileCounters streams forward receipts once, rather than walking a reverse
// reference set for every shared attachment. Inactive receipts fence old retries.
func (s *Store) ReconcileCounters(ctx context.Context, age time.Duration, exists func(context.Context, uuid.UUID, uuid.UUID) (bool, error)) error {
	return s.r.Each(ctx, storage.RoleAttachments, func(cid int64, io rados.IOContext) error {
		return io.List(ctx, func(oid string) error {
			if strings.HasPrefix(oid, "v2/") && strings.Contains(oid, "/data/") {
				parts := strings.Split(oid, "/data/")
				if len(parts) != 2 {
					return nil
				}
				st, _, err := s.loadCounter(ctx, io, parts[0])
				if err != nil {
					return err
				}
				if st.Generation == parts[1] {
					return nil
				}
				obj, err := io.Stat(ctx, oid)
				if errors.Is(err, rados.ErrNotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				if s.Now().Sub(obj.ModTime) < age {
					return nil
				}
				_, err = io.Write(ctx, oid, rados.NewWriteTx().AssertVersion(obj.Version).Remove())
				if errors.Is(err, rados.ErrNotFound) || errors.Is(err, rados.ErrVersionMismatch) {
					return nil
				}
				return err
			}
			if !strings.HasPrefix(oid, "receipt/") {
				return nil
			}
			parts := strings.Split(oid, "/")
			if len(parts) != 4 || !validHash(parts[3]) {
				return nil
			}
			mb, e1 := uuid.Parse(parts[1])
			msg, e2 := uuid.Parse(parts[2])
			if e1 != nil || e2 != nil {
				return nil
			}
			// Finish the counter's prepared mutation before interpreting its receipt.
			id := counterID(parts[3], mb)
			st, _, err := s.loadCounter(ctx, io, id)
			if err != nil {
				return err
			}
			var r receipt
			_, err = readJSON(ctx, io, oid, &r)
			if err != nil {
				return err
			}
			if !r.Active || s.Now().Sub(r.Created) < age || s.Now().Sub(st.Touched) < age {
				return nil
			}
			ok, err := exists(storage.WithID(ctx, cid), mb, msg)
			if err != nil || ok {
				return err
			}
			return s.RemoveRef(storage.WithID(ctx, cid), parts[3], mb, msg)
		})
	})
}
