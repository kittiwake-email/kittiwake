package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"kittiwake/mail"
	"kittiwake/service/auditor"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
)

// RunOptions control one backup run.
type RunOptions struct {
	// Force runs even when the mailbox is unchanged or disabled.
	Force bool
	// Full rewrites every folder entry instead of reusing unchanged ones.
	Full bool
}

var errUnchanged = errors.New("unchanged")

// Backup runs one backup of a mailbox: a skip when nothing changed since
// the last run, otherwise new packs and a manifest. The mailbox's next
// due time is set either way.
func (s *Service) Backup(ctx context.Context, mb uuid.UUID, opts RunOptions) (Run, error) {
	if err := s.authorize(ctx); err != nil {
		return Run{}, err
	}
	mv, err := s.st.Identity.GetMailbox(ctx, mb)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return Run{}, ErrNotFound
		}
		return Run{}, err
	}
	every, retention := s.Policy(ctx, mv.Value)
	now := s.Now().UTC()
	if every == 0 && !opts.Force {
		_ = s.cat.Schedule(ctx, mb, 0, time.Time{})
		return Run{}, ErrDisabled
	}
	state, err := s.cat.BackupState(ctx, mb)
	if err != nil {
		return Run{}, err
	}
	full := opts.Full || state.ForceFull || state.LastRun == ""
	var man *Manifest
	for attempt := 0; attempt < 2; attempt++ {
		var pw *packWriter
		man, pw, err = s.runOnce(ctx, mv.Value, state, retention, full, opts.Force)
		if errors.Is(err, errUnchanged) {
			if ferr := s.cat.SkipRun(ctx, mb, every, now.Add(Jitter(every))); ferr != nil {
				return Run{}, ferr
			}
			return Run{}, nil
		}
		if err != nil {
			if pw != nil {
				pw.Abort()
			}
			retry := now.Add(min(every, time.Hour))
			if every == 0 {
				retry = time.Time{}
			}
			_ = s.cat.FailRun(ctx, mb, truncate(err.Error(), 250), every, retry)
			s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "backup", Detail: "error=" + err.Error(), Result: audit.ResultError})
			return Run{}, err
		}
		if man.Fuzzy && attempt == 0 {
			// The mailbox changed under us: try once more before accepting
			// a fuzzy run (restore repairs the rest).
			s.log.Debug("backup fuzzy; retrying", slog.String("mailbox", mb.String()))
			pw.Abort()
			_ = s.tg.Delete(ctx, s.manifestKey(mb, man.Run))
			continue
		}
		break
	}
	run := Run{
		ID: man.Run, Mailbox: mb, Started: man.Started, Finished: man.Finished, ModSeq: man.ModSeq, Fuzzy: man.Fuzzy, Full: man.Full,
		ManifestKey: s.manifestKey(mb, man.Run), Packs: man.Packs, Bytes: man.Bytes, Messages: man.Messages, NewMessages: man.NewMessages,
		Expires: man.Finished.Add(retention),
	}
	due := now.Add(Jitter(every))
	if every == 0 {
		due = time.Time{}
	}
	if err := s.cat.FinishRun(ctx, run, every, due); err != nil {
		return Run{}, err
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "backup", Target: run.ID,
		Detail: fmt.Sprintf("messages=%d new=%d bytes=%d packs=%d fuzzy=%v full=%v errors=%d", man.Messages, man.NewMessages, man.Bytes, len(man.Packs), man.Fuzzy, man.Full, len(man.Errors))})
	s.log.Info("backup", slog.String("mailbox", mb.String()), slog.String("run", run.ID), slog.Int("messages", man.Messages),
		slog.Int("new", man.NewMessages), slog.Int64("bytes", man.Bytes), slog.Bool("fuzzy", man.Fuzzy), slog.Int("errors", len(man.Errors)))
	return run, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// copier copies one mailbox's data into packs and blobs, reusing the
// references of messages the previous run already holds.
type copier struct {
	s         *Service
	ctx       context.Context
	mb        uuid.UUID
	pw        *packWriter
	retain    time.Time
	known     map[uuid.UUID]Message // from previous entries
	blobs     map[string]Blob       // name → known blob (cache over the catalog)
	newBlobs  []Blob
	blobRefs  []string
	blobBytes int64
	newCount  int
	errors    []string
}

func (c *copier) note(format string, args ...any) {
	if len(c.errors) < 1000 {
		c.errors = append(c.errors, fmt.Sprintf(format, args...))
	}
}

// message returns the backup record of an index record, copying its data
// when the previous run does not hold it. ok is false when the message
// object is gone (the record is skipped and noted).
func (c *copier) message(rec index.Record) (Message, bool, error) {
	m := Message{
		UID: rec.UID, MessageID: rec.MessageID, ThreadID: rec.ThreadID, Flags: rec.Flags, Size: rec.Size,
		InternalDate: rec.InternalDate, Date: rec.Date, Subject: rec.Subject, From: rec.From, FromEmail: rec.FromEmail,
		HasAttachments: rec.HasAttachments, MsgIDHeader: rec.MsgIDHeader,
	}
	if k, ok := c.known[rec.MessageID]; ok {
		m.Doc, m.Raw, m.Attachments = k.Doc, k.Raw, k.Attachments
		return m, true, nil
	}
	ok, err := c.copyData(rec.MessageID, &m)
	if err != nil || !ok {
		return m, ok, err
	}
	c.known[rec.MessageID] = m
	c.newCount++
	return m, true, nil
}

// copyData fetches document, kept original and attachments of a message
// into the packs (or blobs) and fills the references.
func (c *copier) copyData(msg uuid.UUID, m *Message) (bool, error) {
	docBytes, err := c.s.st.Messages.GetDoc(c.ctx, c.mb, msg)
	if errors.Is(err, message.ErrNotFound) {
		c.note("message %s: document missing", msg)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var doc mail.Document
	if err := json.Unmarshal(docBytes, &doc); err != nil {
		c.note("message %s: document unreadable: %v", msg, err)
		return false, nil
	}
	if m.Size == 0 {
		m.Size = doc.Size
	}
	if m.Date.IsZero() {
		m.Date = doc.Date
	}
	if m.InternalDate.IsZero() {
		m.InternalDate = doc.Date
	}
	if m.Subject == "" {
		m.Subject = doc.Subject
	}
	if m.MsgIDHeader == "" {
		m.MsgIDHeader = doc.MessageID
	}
	if m.Doc, err = c.pw.Add(docBytes); err != nil {
		return false, err
	}
	if doc.RawStored == nil || *doc.RawStored {
		raw, err := c.s.st.Messages.GetRaw(c.ctx, c.mb, msg)
		switch {
		case err == nil:
			ref, err := c.pw.Add(raw)
			if err != nil {
				return false, err
			}
			m.Raw = &ref
		case errors.Is(err, message.ErrNotFound):
			if doc.RawStored != nil {
				c.note("message %s: kept original missing", msg)
			}
		default:
			return false, err
		}
	}
	types := partTypes(&doc)
	for _, hash := range mail.AttachmentHashes(&doc) {
		data, err := c.s.st.Attachments.Get(c.ctx, c.mb, hash)
		if errors.Is(err, attach.ErrNotFound) {
			c.note("message %s: attachment %s missing", msg, hash)
			continue
		}
		if err != nil {
			return false, err
		}
		a := Attachment{Hash: hash, ContentType: types[hash]}
		if int64(len(data)) >= c.s.cfg.BlobThreshold {
			a.Ref, err = c.blob(hash, data)
		} else {
			a.Ref, err = c.pw.Add(data)
		}
		if err != nil {
			return false, err
		}
		m.Attachments = append(m.Attachments, a)
	}
	return true, nil
}

// blob stores a large attachment once per content under its keyed name.
func (c *copier) blob(hash string, data []byte) (Ref, error) {
	name := c.s.ring.Name(hash)
	key := c.s.blobKey(name)
	if b, ok := c.blobs[name]; ok {
		c.blobRefs = append(c.blobRefs, name)
		return Ref{Key: key, Length: b.Sealed, Size: b.Size, SHA256: hash}, nil
	}
	have, err := c.s.cat.HaveBlobs(c.ctx, []string{name})
	if err != nil {
		return Ref{}, err
	}
	if b, ok := have[name]; ok {
		c.blobs[name] = b
		c.blobRefs = append(c.blobRefs, name)
		return Ref{Key: key, Length: b.Sealed, Size: b.Size, SHA256: hash}, nil
	}
	ref, err := sealTo(c.ctx, c.s.tg, c.s.ring, key, data, c.retain)
	if err != nil {
		return Ref{}, err
	}
	b := Blob{Name: name, Size: ref.Size, Sealed: ref.Length, Created: c.s.Now().UTC()}
	c.blobs[name] = b
	c.newBlobs = append(c.newBlobs, b)
	c.blobRefs = append(c.blobRefs, name)
	c.blobBytes += ref.Length
	return ref, nil
}

// partTypes maps attachment hashes to their MIME types.
func partTypes(doc *mail.Document) map[string]string {
	out := map[string]string{}
	var walk func(p *mail.Part)
	walk = func(p *mail.Part) {
		if p == nil {
			return
		}
		if p.Attachment != "" {
			out[p.Attachment] = p.Type + "/" + p.Subtype
		}
		for _, ch := range p.Children {
			walk(ch)
		}
	}
	walk(doc.Root)
	return out
}

func refKeys(ms []Message) []string {
	var keys []string
	for _, m := range ms {
		keys = append(keys, m.Doc.Key)
		if m.Raw != nil {
			keys = append(keys, m.Raw.Key)
		}
		for _, a := range m.Attachments {
			keys = append(keys, a.Ref.Key)
		}
	}
	return keys
}

// isPack tells pack keys from blob keys among referenced keys.
func (s *Service) isPack(key string) bool {
	return len(key) > 5 && key[len(key)-5:] == ".pack"
}

func (s *Service) packKeys(keys []string) []string {
	var out []string
	for _, k := range uniqueStrings(keys) {
		if s.isPack(k) {
			out = append(out, k)
		}
	}
	return out
}

// runOnce performs one attempt of a backup run.
func (s *Service) runOnce(ctx context.Context, m identity.Mailbox, state State, retention time.Duration, full, force bool) (*Manifest, *packWriter, error) {
	mb := m.ID
	now := s.Now().UTC()
	root, err := s.st.Index.GetRoot(ctx, mb)
	if errors.Is(err, index.ErrNotFound) {
		root = index.Root{MailboxID: mb}
	} else if err != nil {
		return nil, nil, err
	}
	if !full && !force && state.LastRun != "" && root.ModSeq == state.LastModSeq {
		return nil, nil, errUnchanged
	}
	var prev *Manifest
	if state.LastRun != "" {
		if run, err := s.cat.GetRun(ctx, mb, state.LastRun); err == nil {
			prev, err = s.loadManifest(ctx, run.ManifestKey)
			if err != nil {
				s.log.Warn("previous manifest unreadable; full run", slog.String("mailbox", mb.String()), slog.String("error", err.Error()))
				prev, full = nil, true
			}
		} else {
			full = true
		}
	}
	runID := newRunID(now)
	man := &Manifest{
		Version: ManifestVersion, Mailbox: mb, Run: runID, Started: now, Host: s.Hostname, ModSeq: root.ModSeq, Full: full || prev == nil,
		StorageID: m.StorageID, State: m.State, Quota: m.Quota, Hold: m.Hold,
		Root: RootInfo{Usage: root.Usage, Created: root.Created},
	}
	if prev != nil {
		man.Previous = prev.Run
	}
	man.Accounts, _, _ = s.st.Identity.ListMailboxAccounts(ctx, mb, "", 1000)
	if man.Accounts == nil {
		man.Accounts = []string{}
	}
	retain := s.retainUntil(now, retention)
	pw := newPackWriter(ctx, s.tg, s.ring, s.runPrefix(mb, runID), s.cfg.SpoolDir, s.cfg.PackSize, retain)
	c := &copier{s: s, ctx: ctx, mb: mb, pw: pw, retain: retain, known: map[uuid.UUID]Message{}, blobs: map[string]Blob{}}

	// Which previous folder entries can be kept as they are, and which
	// must be reloaded so their messages' references can be reused.
	prevFolders := map[string]FolderRef{}
	if prev != nil {
		for _, pf := range prev.Folders {
			prevFolders[pf.ID] = pf
		}
	}
	unchanged := func(f *index.Folder) (FolderRef, bool) {
		pf, ok := prevFolders[f.ID]
		return pf, ok && !man.Full && pf.ModSeq == f.ModSeq && pf.UIDValidity == f.UIDValidity
	}
	current := map[string]bool{}
	for _, f := range root.Folders {
		current[f.ID] = true
		if _, keep := unchanged(f); !keep && !man.Full {
			if pf, ok := prevFolders[f.ID]; ok {
				s.reuse(ctx, c, pf.Records)
			}
		}
	}
	for id, pf := range prevFolders {
		if !current[id] && !man.Full {
			s.reuse(ctx, c, pf.Records) // deleted folder: its messages may have moved
		}
	}
	if prev != nil && prev.Retention != nil && !man.Full {
		if retained, err := s.loadRetained(ctx, prev.Retention); err == nil {
			for _, r := range retained {
				c.known[r.MessageID] = r.Message
			}
		} else {
			s.log.Warn("previous retention entry unreadable", slog.String("mailbox", mb.String()), slog.String("error", err.Error()))
		}
	}

	// Folders.
	for _, f := range root.Folders {
		if pf, keep := unchanged(f); keep {
			man.Folders = append(man.Folders, pf)
			man.Messages += pf.Messages
			continue
		}
		entry := FolderEntry{Folder: *f, Messages: []Message{}}
		var after uint32
		for {
			recs, more, err := s.st.Index.ListFolder(ctx, mb, f.ID, after, s.cfg.ScanBatch)
			if err != nil {
				return nil, pw, fmt.Errorf("list %s: %w", f.ID, err)
			}
			for _, rec := range recs {
				after = rec.UID
				msg, ok, err := c.message(rec)
				if err != nil {
					return nil, pw, err
				}
				if ok {
					entry.Messages = append(entry.Messages, msg)
				}
			}
			if !more || len(recs) == 0 {
				break
			}
			if err := ctx.Err(); err != nil {
				return nil, pw, err
			}
		}
		data, err := encodeJSON(&entry)
		if err != nil {
			return nil, pw, err
		}
		ref, err := pw.Add(data)
		if err != nil {
			return nil, pw, err
		}
		fr := FolderRef{ID: f.ID, Name: f.Name, Role: f.Role, UIDValidity: f.UIDValidity, ModSeq: f.ModSeq, Messages: len(entry.Messages), Bytes: f.Size, Records: ref,
			Packs: s.packKeys(append(refKeys(entry.Messages), ref.Key))}
		man.Folders = append(man.Folders, fr)
		man.Messages += fr.Messages
	}
	if man.Folders == nil {
		man.Folders = []FolderRef{}
	}

	// Retention set (expunged, not yet purged): bounded by the retention
	// ranges, read in one go.
	var retained []Retained
	if root.ModSeq > 0 {
		entries, _, _, err := s.st.Index.ListRetention(ctx, mb, time.Time{}, 0)
		if err != nil {
			return nil, pw, fmt.Errorf("retention: %w", err)
		}
		for _, e := range entries {
			msg, ok, err := c.message(index.Record{UID: e.UID, MessageID: e.MessageID, Size: e.Size, InternalDate: e.DeletedAt})
			if err != nil {
				return nil, pw, err
			}
			if ok {
				retained = append(retained, Retained{Message: msg, FolderID: e.FolderID, DeletedAt: e.DeletedAt, PurgeAt: e.PurgeAt, Reason: e.Reason})
			}
		}
	}
	if len(retained) > 0 {
		data, err := encodeJSON(retained)
		if err != nil {
			return nil, pw, err
		}
		ref, err := pw.Add(data)
		if err != nil {
			return nil, pw, err
		}
		man.Retention, man.Retained = &ref, len(retained)
	}

	// Audit log, one entry per day with records. Days before the previous
	// run's day are carried over; that day and later are read again.
	var auditFrom time.Time
	if prev != nil {
		cut := prev.Finished.UTC().Truncate(24 * time.Hour)
		for _, d := range prev.Audit {
			if d.Day < cut.Format("2006-01-02") {
				man.Audit = append(man.Audit, d)
			}
		}
		auditFrom = cut
	} else {
		auditFrom = now.AddDate(0, 0, -s.cfg.AuditDays).Truncate(24 * time.Hour)
	}
	for d := auditFrom; !d.After(now); d = d.AddDate(0, 0, 1) {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		n, cursor := 0, ""
		for {
			recs, next, err := s.st.Audit.Read(ctx, audit.Query{Mailbox: mb, From: d, To: d, After: cursor, Max: 1000})
			if err != nil {
				c.note("audit %s: %v", d.Format("2006-01-02"), err)
				break
			}
			for _, r := range recs {
				_ = enc.Encode(r)
				n++
			}
			if next == "" {
				break
			}
			cursor = next
		}
		if n == 0 {
			continue
		}
		gz, err := gzipBytes(buf.Bytes())
		if err != nil {
			return nil, pw, err
		}
		ref, err := pw.Add(gz)
		if err != nil {
			return nil, pw, err
		}
		man.Audit = append(man.Audit, AuditDay{Day: d.Format("2006-01-02"), Records: n, Ref: ref})
	}

	if err := pw.Close(); err != nil {
		return nil, pw, err
	}
	var packs []string
	for _, fr := range man.Folders {
		packs = append(packs, fr.Packs...)
	}
	if man.Retention != nil {
		packs = append(packs, man.Retention.Key)
		packs = append(packs, s.packKeys(refKeys(messagesOf(retained)))...)
	}
	for _, d := range man.Audit {
		packs = append(packs, d.Ref.Key)
	}
	man.Packs = s.packKeys(packs)
	if man.Packs == nil {
		man.Packs = []string{}
	}
	man.NewMessages = c.newCount
	man.Errors = c.errors
	man.Bytes = pw.Written + c.blobBytes

	// Consistency check: did the mailbox move while we copied it?
	if root2, err := s.st.Index.GetRoot(ctx, mb); err == nil && root2.ModSeq != root.ModSeq {
		man.Fuzzy = true
	}
	man.Finished = s.Now().UTC()
	data, err := encodeJSON(man)
	if err != nil {
		return nil, pw, err
	}
	ref, err := sealTo(ctx, s.tg, s.ring, s.manifestKey(mb, runID), data, retain)
	if err != nil {
		return nil, pw, err
	}
	man.Bytes += ref.Length
	if len(c.newBlobs) > 0 {
		if err := s.cat.AddBlobs(ctx, c.newBlobs); err != nil {
			return nil, pw, err
		}
	}
	if len(c.blobRefs) > 0 {
		if err := s.cat.AddBlobRefs(ctx, mb, uniqueStrings(c.blobRefs)); err != nil {
			return nil, pw, err
		}
	}
	return man, pw, nil
}

// reuse loads a previous folder entry so its messages' references can be
// reused; a missing entry just means those messages are copied again.
func (s *Service) reuse(ctx context.Context, c *copier, ref Ref) {
	fe, err := s.loadFolder(ctx, ref)
	if err != nil {
		s.log.Warn("previous folder entry unreadable", slog.String("mailbox", c.mb.String()), slog.String("error", err.Error()))
		return
	}
	for _, m := range fe.Messages {
		c.known[m.MessageID] = m
	}
}

func messagesOf(rs []Retained) []Message {
	out := make([]Message, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Message)
	}
	return out
}
