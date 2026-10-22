// Package audit persists audit records: per mailbox, one object per day
// (`<mailbox>/audit/<YYYY-MM-DD>`, records as omap entries keyed by
// timestamp) in the index pool of the mailbox's storage; platform-wide
// admin actions go to an AdminLog (a MariaDB table in production, the
// `audit/admin/<YYYY-MM-DD>` object on the memory backend). See README.md
// "Auditing".
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/store/storage"
)

// Protocols.
const (
	ProtoIMAP  = "imap"
	ProtoPOP3  = "pop3"
	ProtoJMAP  = "jmap"
	ProtoLMTP  = "lmtp"
	ProtoAdmin = "admin"
	ProtoREST  = "rest"
	ProtoCLI   = "cli"
)

// Results.
const (
	ResultOK     = "ok"
	ResultDenied = "denied"
	ResultError  = "error"
)

// Record is one audit entry.
type Record struct {
	Time      time.Time `json:"time"`
	Mailbox   uuid.UUID `json:"mailbox,omitempty"`
	Account   string    `json:"account,omitempty"` // username
	AccountID uuid.UUID `json:"account_id,omitempty"`
	TokenID   string    `json:"token_id,omitempty"`
	TokenNote string    `json:"token_comment,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	Protocol  string    `json:"protocol"`
	ClientIP  string    `json:"client_ip,omitempty"`
	Action    string    `json:"action"`
	Target    string    `json:"target,omitempty"` // folder id, "folder/uid", domain, username, ...
	Detail    string    `json:"detail,omitempty"`
	Result    string    `json:"result"`
	// Admin is set when an admin acted on a mailbox/account on behalf of someone.
	Admin string `json:"admin,omitempty"`
}

// Store is the audit store. Writes are synchronous by default; daemons call
// StartBuffering so records are grouped per target object and flushed in one
// omap write per object per interval (one RADOS write per hundreds of
// records instead of one per record). Records also go to the structured log
// synchronously, so a crash loses at most one interval of RADOS copies.
type Store struct {
	r     storage.Resolver
	admin AdminLog
	Now   func() time.Time
	seq   atomic.Uint64

	mu           sync.Mutex
	buffered     bool
	pending      map[target]map[string][]byte // storage+oid -> key -> record
	pendingAdmin []Record
	count        int
	maxBatch     int
	wake         chan struct{}
	stop         chan struct{}
	done         chan struct{}
	errs         func(error)
}

// New returns a store on the index pool context.
func New(indexPool rados.IOContext) *Store {
	return NewMulti(storage.FixedPools(indexPool, indexPool, indexPool), RadosAdminLog(indexPool))
}

// NewMulti returns a store that writes mailbox records to the index pool
// of each mailbox's storage and admin records to admin.
func NewMulti(r storage.Resolver, admin AdminLog) *Store {
	return &Store{r: r, admin: admin, Now: time.Now, pending: map[target]map[string][]byte{}}
}

// AdminLog stores the platform-wide records (admin and CLI actions, and
// records without a mailbox).
type AdminLog interface {
	WriteAdmin(ctx context.Context, recs []Record) error
	ReadAdmin(ctx context.Context, q Query) (records []Record, next string, err error)
	PurgeAdminBefore(ctx context.Context, cutoff time.Time) (int, error)
}

// target is one daily object on one storage.
type target struct {
	storage int64
	oid     string
}

// StartBuffering switches to batched writes: flush every interval or when
// maxBatch records are pending. onError receives flush failures (may be nil).
func (s *Store) StartBuffering(interval time.Duration, maxBatch int, onError func(error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buffered {
		return
	}
	if maxBatch <= 0 {
		maxBatch = 500
	}
	s.buffered, s.maxBatch, s.errs = true, maxBatch, onError
	s.wake, s.stop, s.done = make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	go s.flusher(interval)
}

func (s *Store) flusher(interval time.Duration) {
	defer close(s.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-s.wake:
		case <-s.stop:
			s.flushAll(context.Background())
			return
		}
		s.flushAll(context.Background())
	}
}

func (s *Store) flushAll(ctx context.Context) {
	s.mu.Lock()
	batch := s.pending
	admin := s.pendingAdmin
	s.pending = map[target]map[string][]byte{}
	s.pendingAdmin = nil
	s.count = 0
	s.mu.Unlock()
	for t, kv := range batch {
		io, err := s.r.PoolOf(ctx, t.storage, storage.RoleIndex)
		if err == nil {
			_, err = io.Write(ctx, t.oid, rados.NewWriteTx().OmapSet(kv))
		}
		if err != nil && s.errs != nil {
			s.errs(fmt.Errorf("audit flush %s on storage %d (%d records): %w", t.oid, t.storage, len(kv), err))
		}
	}
	if len(admin) > 0 {
		if err := s.admin.WriteAdmin(ctx, admin); err != nil && s.errs != nil {
			s.errs(fmt.Errorf("audit flush admin log (%d records): %w", len(admin), err))
		}
	}
}

// Flush writes everything pending (tests and shutdown).
func (s *Store) Flush(ctx context.Context) { s.flushAll(ctx) }

// Close stops buffering after a final flush.
func (s *Store) Close() {
	s.mu.Lock()
	if !s.buffered {
		s.mu.Unlock()
		return
	}
	s.buffered = false
	stop, done := s.stop, s.done
	s.mu.Unlock()
	close(stop)
	<-done
}

func day(t time.Time) string { return t.UTC().Format("2006-01-02") }

func mailboxOID(mb uuid.UUID, d string) string { return mb.String() + "/audit/" + d }
func adminOID(d string) string                 { return "audit/admin/" + d }

func (s *Store) key(t time.Time) string {
	return fmt.Sprintf("%020d-%06d", t.UTC().UnixNano(), s.seq.Add(1)%1000000)
}

// Write appends the record to the mailbox's daily object (if Mailbox is
// set) and, when the actor is an admin or the record has no mailbox, to the
// admin log. Time is filled in if zero.
func (s *Store) Write(ctx context.Context, r Record) error {
	if r.Time.IsZero() {
		r.Time = s.Now().UTC()
	}
	data, err := json.Marshal(&r)
	if err != nil {
		return err
	}
	k := s.key(r.Time)
	var t *target
	if r.Mailbox != uuid.Nil {
		cid, _, err := s.r.Pool(ctx, r.Mailbox, storage.RoleIndex)
		if err != nil {
			return err
		}
		t = &target{storage: cid, oid: mailboxOID(r.Mailbox, day(r.Time))}
	}
	toAdmin := r.Protocol == ProtoAdmin || r.Protocol == ProtoCLI || r.Admin != "" || r.Mailbox == uuid.Nil
	s.mu.Lock()
	if s.buffered {
		if t != nil {
			m := s.pending[*t]
			if m == nil {
				m = map[string][]byte{}
				s.pending[*t] = m
			}
			m[k] = data
			s.count++
		}
		if toAdmin {
			s.pendingAdmin = append(s.pendingAdmin, r)
			s.count++
		}
		full := s.count >= s.maxBatch
		s.mu.Unlock()
		if full {
			select {
			case s.wake <- struct{}{}:
			default:
			}
		}
		return nil
	}
	s.mu.Unlock()
	if t != nil {
		io, err := s.r.PoolOf(ctx, t.storage, storage.RoleIndex)
		if err != nil {
			return err
		}
		if _, err := io.Write(ctx, t.oid, rados.NewWriteTx().OmapSet(map[string][]byte{k: data})); err != nil {
			return err
		}
	}
	if toAdmin {
		return s.admin.WriteAdmin(ctx, []Record{r})
	}
	return nil
}

// Query describes an audit query.
type Query struct {
	Mailbox  uuid.UUID // zero = admin log
	From, To time.Time // inclusive day range; zero To = From's day, zero From = today
	Account  string
	Action   string
	ClientIP string
	After    string // cursor: "<day>/<key>"
	Max      int
}

// Read returns matching records, newest day last, with a cursor for more.
func (s *Store) Read(ctx context.Context, q Query) (records []Record, next string, err error) {
	if q.Max <= 0 {
		q.Max = 100
	}
	if q.Mailbox == uuid.Nil {
		return s.admin.ReadAdmin(ctx, q)
	}
	_, io, err := s.r.Pool(ctx, q.Mailbox, storage.RoleIndex)
	if err != nil {
		return nil, "", err
	}
	return readObjects(ctx, io, q, s.Now, func(ds string) string { return mailboxOID(q.Mailbox, ds) })
}

// Matches reports whether r passes the query's record filters.
func (q Query) Matches(r Record) bool {
	if q.Account != "" && !strings.EqualFold(r.Account, q.Account) && !strings.EqualFold(r.Admin, q.Account) {
		return false
	}
	if q.Action != "" && r.Action != q.Action {
		return false
	}
	if q.ClientIP != "" && r.ClientIP != q.ClientIP {
		return false
	}
	return true
}

// DayRange returns the inclusive day range of the query (zero From = today).
func (q Query) DayRange(now func() time.Time) (from, to time.Time) {
	from, to = q.From, q.To
	if from.IsZero() {
		from = now()
	}
	if to.IsZero() {
		to = from
	}
	return from.UTC().Truncate(24 * time.Hour), to.UTC().Truncate(24 * time.Hour)
}

// readObjects walks daily omap objects named by oid(day) on one pool.
func readObjects(ctx context.Context, io rados.IOContext, q Query, now func() time.Time, oid func(day string) string) (records []Record, next string, err error) {
	from, to := q.DayRange(now)
	startDay, startKey := "", ""
	if q.After != "" {
		if i := strings.Index(q.After, "/"); i > 0 {
			startDay, startKey = q.After[:i], q.After[i+1:]
		}
	}
	for d := from; !d.After(to); d = d.Add(24 * time.Hour) {
		ds := day(d)
		if startDay != "" && ds < startDay {
			continue
		}
		after := ""
		if ds == startDay {
			after = startKey
		}
		for {
			kv, more, err := io.OmapList(ctx, oid(ds), "", after, 500)
			if err != nil {
				if errors.Is(err, rados.ErrNotFound) {
					break
				}
				return nil, "", err
			}
			for _, e := range kv {
				after = e.Key
				var r Record
				if err := json.Unmarshal(e.Value, &r); err != nil {
					continue
				}
				if !q.Matches(r) {
					continue
				}
				records = append(records, r)
				if len(records) >= q.Max {
					return records, ds + "/" + e.Key, nil
				}
			}
			if !more {
				break
			}
		}
	}
	return records, "", nil
}

// PurgeBefore deletes daily audit objects older than cutoff for a mailbox
// (or the admin log when mb is zero). days limits how far back to look.
func (s *Store) PurgeBefore(ctx context.Context, mb uuid.UUID, cutoff time.Time, days int) (int, error) {
	if mb == uuid.Nil {
		return s.admin.PurgeAdminBefore(ctx, cutoff)
	}
	_, io, err := s.r.Pool(ctx, mb, storage.RoleIndex)
	if err != nil {
		return 0, err
	}
	return purgeObjects(ctx, io, cutoff, days, func(d string) string { return mailboxOID(mb, d) })
}

func purgeObjects(ctx context.Context, io rados.IOContext, cutoff time.Time, days int, oid func(day string) string) (int, error) {
	n := 0
	for i := 1; i <= days; i++ {
		d := cutoff.UTC().Add(-time.Duration(i) * 24 * time.Hour)
		err := io.Delete(ctx, oid(day(d)))
		if err == nil {
			n++
		} else if !errors.Is(err, rados.ErrNotFound) {
			return n, err
		}
	}
	return n, nil
}

// ---- admin log on RADOS (memory backend, single storage) ----

type radosAdminLog struct {
	io  rados.IOContext
	seq atomic.Uint64
}

// RadosAdminLog keeps admin records as `audit/admin/<day>` objects on one
// index pool (the memory backend; production uses MariaDB).
func RadosAdminLog(indexPool rados.IOContext) AdminLog { return &radosAdminLog{io: indexPool} }

func (l *radosAdminLog) WriteAdmin(ctx context.Context, recs []Record) error {
	byDay := map[string]map[string][]byte{}
	for _, r := range recs {
		data, err := json.Marshal(&r)
		if err != nil {
			return err
		}
		d := day(r.Time)
		if byDay[d] == nil {
			byDay[d] = map[string][]byte{}
		}
		byDay[d][fmt.Sprintf("%020d-%06d", r.Time.UTC().UnixNano(), l.seq.Add(1)%1000000)] = data
	}
	for d, kv := range byDay {
		if _, err := l.io.Write(ctx, adminOID(d), rados.NewWriteTx().OmapSet(kv)); err != nil {
			return err
		}
	}
	return nil
}

func (l *radosAdminLog) ReadAdmin(ctx context.Context, q Query) ([]Record, string, error) {
	return readObjects(ctx, l.io, q, time.Now, adminOID)
}

func (l *radosAdminLog) PurgeAdminBefore(ctx context.Context, cutoff time.Time) (int, error) {
	return purgeObjects(ctx, l.io, cutoff, 60, adminOID)
}
