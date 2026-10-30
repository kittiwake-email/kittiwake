// Package audit persists audit records in bounded daily segments
// (`<mailbox>/audit/<YYYY-MM-DD>/<segment>`, with scalar daily headers) in the index pool of the mailbox's storage; platform-wide
// admin actions go to an AdminLog (a MariaDB table in production, the
// `audit/admin/<YYYY-MM-DD>` object on the memory backend). See README.md
// "Auditing".
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	Time    time.Time `json:"time"`
	Mailbox uuid.UUID `json:"mailbox,omitempty"`
	// Namespace of the target (platform records: the namespace acted
	// upon, empty for platform-level actions).
	Namespace string    `json:"namespace,omitempty"`
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
	writerID string
	seq      atomic.Uint64
	r        storage.Resolver
	admin    AdminLog
	Now      func() time.Time

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
	return &Store{writerID: uuid.NewString(), r: r, admin: admin, Now: time.Now, pending: map[target]map[string][]byte{}}
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
			err = appendSegmented(ctx, io, t.oid, kv)
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
	return fmt.Sprintf("%020d-%020d-%s", t.UTC().UnixNano(), s.seq.Add(1), s.writerID)
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
		if err := appendSegmented(ctx, io, t.oid, map[string][]byte{k: data}); err != nil {
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
	Mailbox uuid.UUID // zero = admin log
	// Namespace restricts the admin log to records of one namespace.
	Namespace string
	From, To  time.Time // inclusive day range; zero To = From's day, zero From = today
	Account   string
	Action    string
	ClientIP  string
	After     string // cursor: "<day>/<key>"
	Max       int
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
	if q.Namespace != "" && r.Namespace != q.Namespace {
		return false
	}
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
	var startSegment uint64
	if q.After != "" {
		parts := strings.SplitN(q.After, "/", 3)
		if len(parts) != 3 {
			return nil, "", errors.New("audit: invalid cursor")
		}
		startDay, startKey = parts[0], parts[2]
		startSegment, err = strconv.ParseUint(parts[1], 16, 64)
		if err != nil {
			return nil, "", err
		}
	}
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		ds := day(d)
		if ds < startDay {
			continue
		}
		var h dayHead
		_, err := loadJSON(ctx, io, oid(ds), &h)
		if errors.Is(err, rados.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		if h.Closed {
			continue
		}
		first := uint64(0)
		if ds == startDay {
			first = startSegment
		}
		for seg := first; seg <= h.Last; seg++ {
			after := ""
			if ds == startDay && seg == startSegment {
				after = startKey
			}
			for {
				kv, more, err := io.OmapList(ctx, segmentOID(oid(ds), seg), "", after, 500)
				if errors.Is(err, rados.ErrNotFound) {
					break
				}
				if err != nil {
					return nil, "", err
				}
				for _, e := range kv {
					after = e.Key
					var r Record
					if err := json.Unmarshal(e.Value, &r); err != nil {
						return nil, "", err
					}
					if !q.Matches(r) {
						continue
					}
					records = append(records, r)
					if len(records) >= q.Max {
						return records, fmt.Sprintf("%s/%016x/%s", ds, seg, e.Key), nil
					}
				}
				if !more {
					break
				}
			}
		}
	}
	return records, "", nil
}

// PurgeBefore retires audit data older than cutoff for a mailbox
// (or the admin log when mb is zero). days bounds work from the durable cursor.
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
	return purgeSegmented(ctx, io, cutoff, days, oid)
}

// ---- admin log on RADOS (memory backend, single storage) ----

type radosAdminLog struct {
	writerID string
	seq      atomic.Uint64
	io       rados.IOContext
}

// RadosAdminLog keeps admin records as `audit/admin/<day>` objects on one
// index pool (the memory backend; production uses MariaDB).
func RadosAdminLog(indexPool rados.IOContext) AdminLog {
	return &radosAdminLog{io: indexPool, writerID: uuid.NewString()}
}

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
		byDay[d][fmt.Sprintf("%020d-%020d-%s", r.Time.UTC().UnixNano(), l.seq.Add(1), l.writerID)] = data
	}
	for d, kv := range byDay {
		if err := appendSegmented(ctx, l.io, adminOID(d), kv); err != nil {
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

// DeleteAll removes audit data and retirement fences for a mailbox whose
// registry entry has already been removed. The catalog avoids a pool-wide scan.
func (s *Store) DeleteAll(ctx context.Context, mb uuid.UUID) error {
	_, io, err := s.r.Pool(ctx, mb, storage.RoleIndex)
	if err != nil {
		return err
	}
	catOID := catalogOID(mailboxOID(mb, ""))
	var c catalog
	_, err = loadJSON(ctx, io, catOID, &c)
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	first, err := time.Parse("2006-01-02", c.Start)
	if err != nil {
		return err
	}
	for d := first; day(d) <= c.Last; d = d.AddDate(0, 0, 1) {
		base := mailboxOID(mb, day(d))
		var h dayHead
		_, err := loadJSON(ctx, io, base, &h)
		if errors.Is(err, rados.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		for seg := uint64(0); seg <= h.Last; seg++ {
			if err := io.Delete(ctx, segmentOID(base, seg)); err != nil && !errors.Is(err, rados.ErrNotFound) {
				return err
			}
		}
		if err := io.Delete(ctx, base); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return err
		}
	}
	err = io.Delete(ctx, catOID)
	if errors.Is(err, rados.ErrNotFound) {
		return nil
	}
	return err
}
