// Package audit persists audit records in the RADOS index pool: one object
// per mailbox per day (`<mailbox>/audit/<YYYY-MM-DD>`) and one platform-wide
// per day for admin actions (`audit/admin/<YYYY-MM-DD>`), records as omap
// entries keyed by timestamp. See README.md "Auditing".
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

	"dovetail/rados"
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
	io  rados.IOContext
	Now func() time.Time
	seq atomic.Uint64

	mu       sync.Mutex
	buffered bool
	pending  map[string]map[string][]byte // oid -> key -> record
	count    int
	maxBatch int
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	errs     func(error)
}

// New returns a store on the index pool context.
func New(indexPool rados.IOContext) *Store {
	return &Store{io: indexPool, Now: time.Now, pending: map[string]map[string][]byte{}}
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
	s.pending = map[string]map[string][]byte{}
	s.count = 0
	s.mu.Unlock()
	for oid, kv := range batch {
		if _, err := s.io.Write(ctx, oid, rados.NewWriteTx().OmapSet(kv)); err != nil && s.errs != nil {
			s.errs(fmt.Errorf("audit flush %s (%d records): %w", oid, len(kv), err))
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
// admin object. Time is filled in if zero.
func (s *Store) Write(ctx context.Context, r Record) error {
	if r.Time.IsZero() {
		r.Time = s.Now().UTC()
	}
	data, err := json.Marshal(&r)
	if err != nil {
		return err
	}
	k := s.key(r.Time)
	d := day(r.Time)
	var oids []string
	if r.Mailbox != uuid.Nil {
		oids = append(oids, mailboxOID(r.Mailbox, d))
	}
	if r.Protocol == ProtoAdmin || r.Protocol == ProtoCLI || r.Admin != "" || r.Mailbox == uuid.Nil {
		oids = append(oids, adminOID(d))
	}
	s.mu.Lock()
	if s.buffered {
		for _, oid := range oids {
			m := s.pending[oid]
			if m == nil {
				m = map[string][]byte{}
				s.pending[oid] = m
			}
			m[k] = data
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
	for _, oid := range oids {
		if _, err := s.io.Write(ctx, oid, rados.NewWriteTx().OmapSet(map[string][]byte{k: data})); err != nil {
			return err
		}
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
	from, to := q.From, q.To
	if from.IsZero() {
		from = s.Now()
	}
	if to.IsZero() {
		to = from
	}
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
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
		oid := adminOID(ds)
		if q.Mailbox != uuid.Nil {
			oid = mailboxOID(q.Mailbox, ds)
		}
		for {
			kv, more, err := s.io.OmapList(ctx, oid, "", after, 500)
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
				if q.Account != "" && !strings.EqualFold(r.Account, q.Account) && !strings.EqualFold(r.Admin, q.Account) {
					continue
				}
				if q.Action != "" && r.Action != q.Action {
					continue
				}
				if q.ClientIP != "" && r.ClientIP != q.ClientIP {
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
	n := 0
	for i := 1; i <= days; i++ {
		d := cutoff.UTC().Add(-time.Duration(i) * 24 * time.Hour)
		oid := adminOID(day(d))
		if mb != uuid.Nil {
			oid = mailboxOID(mb, day(d))
		}
		err := s.io.Delete(ctx, oid)
		if err == nil {
			n++
		} else if !errors.Is(err, rados.ErrNotFound) {
			return n, err
		}
	}
	return n, nil
}
