package imap

import (
	"bufio"
	"bytes"
	"errors"
	"mime"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-message/textproto"

	"dovetail/service/mailbox"
	"dovetail/store/index"
)

// selected is the session's view of the selected folder: a snapshot of UIDs
// (sequence number i+1 = uids[i]) plus per-UID records for change detection.
type selected struct {
	s         *session
	folderID  string
	name      string
	readOnly  bool
	uids      []uint32
	recs      map[uint32]index.Record
	modseq    uint64   // mailbox modseq at the last sync
	vanished  []uint32 // UIDs gone from the folder but not yet reported as EXPUNGE
	searchRes imap.UIDSet
	// pendingAdded are UIDs applied locally (own APPEND) still to be
	// announced with EXISTS on the next sync.
	pendingAdded []uint32
}

// noteAppended applies a record this session just appended. When nothing
// else changed in the mailbox meanwhile (the record's modseq directly
// follows ours), the next sync needs no reads at all.
func (sel *selected) noteAppended(rec index.Record) {
	if rec.ModSeq != sel.modseq+1 {
		return // other changes interleaved; the next sync reads the log
	}
	if _, known := sel.recs[rec.UID]; known {
		return
	}
	sel.recs[rec.UID] = rec
	sel.uids = append(sel.uids, rec.UID)
	sort.Slice(sel.uids, func(i, j int) bool { return sel.uids[i] < sel.uids[j] })
	sel.pendingAdded = append(sel.pendingAdded, rec.UID)
	sel.modseq = rec.ModSeq
}

func newSelected(s *session, f *index.Folder, readOnly bool) (*selected, error) {
	sel := &selected{s: s, folderID: f.ID, name: f.Name, readOnly: readOnly, recs: map[uint32]index.Record{}}
	recs, err := sel.listAll()
	if err != nil {
		return nil, err
	}
	for _, r := range recs {
		sel.uids = append(sel.uids, r.UID)
		sel.recs[r.UID] = r
	}
	sel.modseq = f.ModSeq // folder modseq <= mailbox modseq; the first sync catches up
	return sel, nil
}

func (sel *selected) close() {}

func (sel *selected) listAll() ([]index.Record, error) {
	var out []index.Record
	var after uint32
	for {
		recs, more, err := sel.s.srv.mb.List(sel.s.ctx, sel.s.mb, sel.folderID, after, 1000)
		if err != nil {
			return nil, sel.s.srv.mapErr(err, "list")
		}
		out = append(out, recs...)
		if len(recs) > 0 {
			after = recs[len(recs)-1].UID
		}
		if !more || len(recs) == 0 {
			break
		}
	}
	return out, nil
}

func (sel *selected) firstUnseen() uint32 {
	for i, uid := range sel.uids {
		if r, ok := sel.recs[uid]; ok && !r.HasFlag(index.FlagSeen) {
			return uint32(i + 1)
		}
	}
	return 0
}

func (sel *selected) seqOf(uid uint32) uint32 {
	i := sort.Search(len(sel.uids), func(i int) bool { return sel.uids[i] >= uid })
	if i < len(sel.uids) && sel.uids[i] == uid {
		return uint32(i + 1)
	}
	return 0
}

func (sel *selected) removeUID(uid uint32) {
	if i := sel.seqOf(uid); i > 0 {
		sel.uids = append(sel.uids[:i-1], sel.uids[i:]...)
	}
	delete(sel.recs, uid)
}

// sync refreshes the snapshot from the index and emits untagged updates.
// When allowExpunge is false, vanished messages stay in the snapshot until
// a later sync may report them.
// sync brings the session up to date with one change-log read (which
// also yields the current mailbox modseq): only records that changed are
// read. A log horizon miss or an implausibly large delta falls back to a
// full re-list.
func (sel *selected) sync(w *imapserver.UpdateWriter, allowExpunge bool) error {
	var flagChanged, added []uint32
	var current uint64
	ok := true
	var err error
	if sel.s.srv.mb != nil && len(sel.pendingAdded) > 0 && sel.freshEnough() {
		// Only our own append happened; announce it without reading.
		current = sel.modseq
	} else {
		flagChanged, added, current, ok, err = sel.applyChanges()
		if err != nil {
			return err
		}
	}
	added = append(sel.pendingAdded, added...)
	sel.pendingAdded = nil
	if !ok {
		root, err := sel.s.root()
		if err != nil {
			return err
		}
		if root.FolderByID(sel.folderID) == nil {
			return noErr(imap.ResponseCodeNonExistent, "Selected mailbox was deleted")
		}
		if flagChanged, added, err = sel.relist(); err != nil {
			return err
		}
		current = root.ModSeq
	}
	sel.modseq = current
	return sel.emit(w, flagChanged, added, allowExpunge)
}

// freshEnough reports whether the session may skip the change-log read:
// the last thing that happened to the mailbox, as far as this process has
// seen, is our own append.
func (sel *selected) freshEnough() bool {
	root, err := sel.s.srv.mb.RootCached(sel.s.ctx, sel.s.mb, 2*time.Second)
	return err == nil && root.ModSeq == sel.modseq
}

// applyChanges folds the mailbox change log since the session's modseq
// into the session state. ok=false asks for a full re-list.
func (sel *selected) applyChanges() (flagChanged, added []uint32, current uint64, ok bool, err error) {
	const maxDelta = 2000
	since := sel.modseq
	var changes []index.Change
	for {
		ch, more, cur, err := sel.s.srv.mb.Changes(sel.s.ctx, sel.s.mb, since, 1000)
		if err != nil {
			if errors.Is(err, mailbox.ErrNoHorizon) {
				return nil, nil, 0, false, nil
			}
			return nil, nil, 0, false, sel.s.srv.mapErr(err, "sync")
		}
		current = cur
		changes = append(changes, ch...)
		if len(changes) > maxDelta {
			return nil, nil, 0, false, nil
		}
		if !more || len(ch) == 0 {
			break
		}
		since = ch[len(ch)-1].ModSeq
	}
	if len(changes) == 0 {
		return nil, nil, current, true, nil
	}
	for _, c := range changes {
		if c.Kind == index.KindFolder && c.Op == index.OpDestroyed && c.ID == sel.folderID {
			return nil, nil, 0, false, nil
		}
	}
	newUIDs := map[uint32]bool{}
	updUIDs := map[uint32]bool{}
	for _, c := range changes {
		if c.Kind != index.KindEmail || c.FolderID != sel.folderID {
			// A message moved out of this folder is logged under its new
			// folder; it shows up as a missing record below.
			continue
		}
		switch c.Op {
		case index.OpCreated:
			if _, known := sel.recs[c.UID]; !known {
				newUIDs[c.UID] = true
			}
		case index.OpUpdated:
			if _, known := sel.recs[c.UID]; known {
				updUIDs[c.UID] = true
			} else {
				newUIDs[c.UID] = true // moved into this folder
			}
		case index.OpDestroyed:
			if _, known := sel.recs[c.UID]; known {
				sel.vanished = append(sel.vanished, c.UID)
				delete(sel.recs, c.UID)
			}
		}
	}
	// Messages moved away appear as "updated" under another folder; detect
	// those by re-reading records we have not already seen change.
	for _, c := range changes {
		if c.Kind == index.KindEmail && c.FolderID != sel.folderID && c.Op == index.OpUpdated {
			for uid, r := range sel.recs {
				if r.MessageID.String() == c.ID {
					sel.vanished = append(sel.vanished, uid)
					delete(sel.recs, uid)
					delete(updUIDs, uid)
					break
				}
			}
		}
	}
	// New records: one newest-first read down to the lowest new UID.
	if len(newUIDs) > 0 {
		low := uint32(0)
		for uid := range newUIDs {
			if low == 0 || uid < low {
				low = uid
			}
		}
		before := uint32(0)
		for {
			recs, more, err := sel.s.srv.mb.ListDesc(sel.s.ctx, sel.s.mb, sel.folderID, before, 500)
			if err != nil {
				return nil, nil, 0, false, sel.s.srv.mapErr(err, "sync")
			}
			done := false
			for _, r := range recs {
				if newUIDs[r.UID] {
					sel.recs[r.UID] = r
					added = append(added, r.UID)
				}
				if r.UID <= low {
					done = true
					break
				}
				before = r.UID
			}
			if done || !more || len(recs) == 0 {
				break
			}
		}
	}
	for uid := range updUIDs {
		r, err := sel.s.srv.mb.GetByUID(sel.s.ctx, sel.s.mb, sel.folderID, uid)
		if err != nil {
			if errors.Is(err, mailbox.ErrNotFound) {
				sel.vanished = append(sel.vanished, uid)
				delete(sel.recs, uid)
				continue
			}
			return nil, nil, 0, false, sel.s.srv.mapErr(err, "sync")
		}
		sel.recs[uid] = r
		flagChanged = append(flagChanged, uid)
	}
	sort.Slice(added, func(i, j int) bool { return added[i] < added[j] })
	sel.uids = append(sel.uids, added...)
	sort.Slice(sel.uids, func(i, j int) bool { return sel.uids[i] < sel.uids[j] })
	return flagChanged, added, current, true, nil
}

// relist is the full fallback: read the whole folder and diff.
func (sel *selected) relist() (flagChanged, added []uint32, err error) {
	recs, err := sel.listAll()
	if err != nil {
		return nil, nil, err
	}
	fresh := make(map[uint32]index.Record, len(recs))
	for _, r := range recs {
		fresh[r.UID] = r
	}
	for _, uid := range sel.uids {
		old, ok := sel.recs[uid]
		if !ok {
			continue // already pending expunge
		}
		nr, exists := fresh[uid]
		switch {
		case !exists:
			sel.vanished = append(sel.vanished, uid)
			delete(sel.recs, uid)
		case nr.ModSeq != old.ModSeq:
			sel.recs[uid] = nr
			flagChanged = append(flagChanged, uid)
		}
	}
	known := map[uint32]bool{}
	for _, uid := range sel.uids {
		known[uid] = true
	}
	for _, r := range recs {
		if !known[r.UID] {
			added = append(added, r.UID)
			sel.recs[r.UID] = r
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i] < added[j] })
	sel.uids = append(sel.uids, added...)
	sort.Slice(sel.uids, func(i, j int) bool { return sel.uids[i] < sel.uids[j] })
	return flagChanged, added, nil
}

// emit writes the untagged responses for a sync.
func (sel *selected) emit(w *imapserver.UpdateWriter, flagChanged, added []uint32, allowExpunge bool) error {
	for _, uid := range flagChanged {
		if seq := sel.seqOf(uid); seq > 0 {
			if err := w.WriteMessageFlags(seq, imap.UID(uid), toFlags(sel.recs[uid].Flags)); err != nil {
				return err
			}
		}
	}
	if len(added) > 0 {
		if err := w.WriteNumMessages(uint32(len(sel.uids))); err != nil {
			return err
		}
	}
	if allowExpunge && len(sel.vanished) > 0 {
		// Highest sequence numbers first so earlier numbers stay valid.
		sort.Slice(sel.vanished, func(i, j int) bool { return sel.vanished[i] > sel.vanished[j] })
		for _, uid := range sel.vanished {
			seq := sel.seqOf(uid)
			sel.removeUID(uid)
			if seq > 0 {
				if err := w.WriteExpunge(seq); err != nil {
					return err
				}
			}
		}
		sel.vanished = nil
		if err := w.WriteNumMessages(uint32(len(sel.uids))); err != nil {
			return err
		}
	}
	return nil
}

// resolve turns a sequence or UID set into the UIDs it denotes (in
// ascending sequence order), ignoring numbers that do not exist.
func (sel *selected) resolve(numSet imap.NumSet) []uint32 {
	if imap.IsSearchRes(numSet) {
		numSet = sel.searchRes
	}
	var out []uint32
	switch ns := numSet.(type) {
	case imap.SeqSet:
		max := uint32(len(sel.uids))
		for _, r := range ns {
			start, stop := r.Start, r.Stop
			if start == 0 {
				start = max
			}
			if stop == 0 {
				stop = max
			}
			if start > stop {
				start, stop = stop, start
			}
			for seq := start; seq <= stop && seq >= 1; seq++ {
				if int(seq) <= len(sel.uids) {
					out = append(out, sel.uids[seq-1])
				}
				if seq == ^uint32(0) {
					break
				}
			}
		}
	case imap.UIDSet:
		var maxUID uint32
		if len(sel.uids) > 0 {
			maxUID = sel.uids[len(sel.uids)-1]
		}
		static := make(imap.UIDSet, 0, len(ns))
		for _, r := range ns {
			start, stop := uint32(r.Start), uint32(r.Stop)
			if start == 0 {
				start = maxUID
			}
			if stop == 0 {
				stop = maxUID
			}
			if start > stop {
				start, stop = stop, start
			}
			static = append(static, imap.UIDRange{Start: imap.UID(start), Stop: imap.UID(stop)})
		}
		for _, uid := range sel.uids {
			if static.Contains(imap.UID(uid)) {
				out = append(out, uid)
			}
		}
	}
	// Dedup, keep order.
	seen := map[uint32]bool{}
	dedup := out[:0]
	for _, u := range out {
		if !seen[u] && sel.recs[u].UID != 0 {
			seen[u] = true
			dedup = append(dedup, u)
		}
	}
	return dedup
}

func toFlags(in []string) []imap.Flag {
	out := make([]imap.Flag, 0, len(in))
	for _, f := range in {
		out = append(out, imap.Flag(f))
	}
	return out
}

func fromFlags(in []imap.Flag) []string {
	out := make([]string, 0, len(in))
	for _, f := range in {
		out = append(out, string(f))
	}
	return out
}

// ---- STORE ----

func (sel *selected) store(w *imapserver.FetchWriter, numSet imap.NumSet, flags *imap.StoreFlags) error {
	if sel.readOnly {
		return noErr(imap.ResponseCodeNoPerm, "Mailbox is read-only")
	}
	uids := sel.resolve(numSet)
	for _, uid := range uids {
		op := mailbox.FlagOp{}
		switch flags.Op {
		case imap.StoreFlagsSet:
			op.Replace = fromFlags(flags.Flags)
			if op.Replace == nil {
				op.Replace = []string{}
			}
		case imap.StoreFlagsAdd:
			op.Add = fromFlags(flags.Flags)
		case imap.StoreFlagsDel:
			op.Remove = fromFlags(flags.Flags)
		}
		rec, err := sel.s.srv.mb.SetFlagsByUID(sel.s.ctx, sel.s.mb, sel.folderID, uid, op)
		if err != nil {
			return sel.s.srv.mapErr(err, "store")
		}
		sel.recs[uid] = rec
		if !flags.Silent {
			rw := w.CreateMessage(sel.seqOf(uid))
			rw.WriteUID(imap.UID(uid))
			rw.WriteFlags(toFlags(rec.Flags))
			if err := rw.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- EXPUNGE ----

func (sel *selected) expunge(w *imapserver.ExpungeWriter, uidSet *imap.UIDSet) error {
	if sel.readOnly {
		return noErr(imap.ResponseCodeNoPerm, "Mailbox is read-only")
	}
	var only []uint32
	if uidSet != nil {
		only = sel.resolve(*uidSet)
		if only == nil {
			only = []uint32{}
		}
	}
	expunged, err := sel.s.srv.mb.Expunge(sel.s.ctx, sel.s.mb, sel.folderID, only)
	if err != nil {
		return sel.s.srv.mapErr(err, "expunge")
	}
	sort.Slice(expunged, func(i, j int) bool { return expunged[i] > expunged[j] })
	for _, uid := range expunged {
		seq := sel.seqOf(uid)
		sel.removeUID(uid)
		if seq > 0 {
			if err := w.WriteExpunge(seq); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- COPY / MOVE ----

func (sel *selected) copy(numSet imap.NumSet, dest *index.Folder) (*imap.CopyData, error) {
	uids := sel.resolve(numSet)
	data := &imap.CopyData{UIDValidity: dest.UIDValidity}
	for _, uid := range uids {
		rec := sel.recs[uid]
		raw, err := sel.s.srv.mb.Raw(sel.s.ctx, sel.s.mb, rec.MessageID)
		if err != nil {
			return nil, sel.s.srv.mapErr(err, "copy")
		}
		d, err := sel.s.srv.del.Import(sel.s.ctx, sel.s.mb, dest.ID, raw, rec.Flags, rec.InternalDate)
		if err != nil {
			return nil, sel.s.srv.mapErr(err, "copy")
		}
		data.SourceUIDs.AddNum(imap.UID(uid))
		data.DestUIDs.AddNum(imap.UID(d.UID))
	}
	return data, nil
}

func (sel *selected) move(w *imapserver.MoveWriter, numSet imap.NumSet, dest *index.Folder) error {
	if sel.readOnly {
		return noErr(imap.ResponseCodeNoPerm, "Mailbox is read-only")
	}
	if dest.ID == sel.folderID {
		return noErr(imap.ResponseCodeCannot, "Source and destination mailboxes are identical")
	}
	uids := sel.resolve(numSet)
	data := &imap.CopyData{UIDValidity: dest.UIDValidity}
	var moved []uint32
	for _, uid := range uids {
		rec, err := sel.s.srv.mb.MoveByUID(sel.s.ctx, sel.s.mb, sel.folderID, uid, dest.ID)
		if err != nil {
			return sel.s.srv.mapErr(err, "move")
		}
		data.SourceUIDs.AddNum(imap.UID(uid))
		data.DestUIDs.AddNum(imap.UID(rec.UID))
		moved = append(moved, uid)
	}
	if len(moved) == 0 {
		return nil
	}
	if err := w.WriteCopyData(data); err != nil {
		return err
	}
	sort.Slice(moved, func(i, j int) bool { return moved[i] > moved[j] })
	for _, uid := range moved {
		seq := sel.seqOf(uid)
		sel.removeUID(uid)
		if seq > 0 {
			if err := w.WriteExpunge(seq); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- FETCH ----

type fetched struct {
	rec index.Record
	raw []byte
}

func (sel *selected) rawFor(f *fetched) ([]byte, error) {
	if f.raw != nil {
		return f.raw, nil
	}
	raw, err := sel.s.srv.mb.Raw(sel.s.ctx, sel.s.mb, f.rec.MessageID)
	if err != nil {
		return nil, err
	}
	f.raw = raw
	return raw, nil
}

func (sel *selected) fetch(w *imapserver.FetchWriter, numSet imap.NumSet, options *imap.FetchOptions) error {
	markSeen := false
	for _, bs := range options.BodySection {
		if !bs.Peek {
			markSeen = true
		}
	}
	for _, bs := range options.BinarySection {
		if !bs.Peek {
			markSeen = true
		}
	}
	needRaw := options.Envelope || options.BodyStructure != nil || len(options.BodySection) > 0 || len(options.BinarySection) > 0 || len(options.BinarySectionSize) > 0
	for _, uid := range sel.resolve(numSet) {
		f := &fetched{rec: sel.recs[uid]}
		if markSeen && !sel.readOnly && !f.rec.HasFlag(index.FlagSeen) {
			rec, err := sel.s.srv.mb.SetFlagsByUID(sel.s.ctx, sel.s.mb, sel.folderID, uid, mailbox.FlagOp{Add: []string{index.FlagSeen}})
			if err == nil {
				f.rec = rec
				sel.recs[uid] = rec
			} else if !errors.Is(err, mailbox.ErrHold) {
				return sel.s.srv.mapErr(err, "fetch")
			}
		}
		if needRaw {
			if _, err := sel.rawFor(f); err != nil {
				return sel.s.srv.mapErr(err, "fetch")
			}
		}
		rw := w.CreateMessage(sel.seqOf(uid))
		rw.WriteUID(imap.UID(uid))
		if options.Flags || (markSeen && f.rec.HasFlag(index.FlagSeen)) {
			rw.WriteFlags(toFlags(f.rec.Flags))
		}
		if options.InternalDate {
			rw.WriteInternalDate(f.rec.InternalDate)
		}
		if options.RFC822Size {
			rw.WriteRFC822Size(f.rec.Size)
		}
		if options.Envelope {
			rw.WriteEnvelope(envelopeOf(f.raw))
		}
		if options.BodyStructure != nil {
			rw.WriteBodyStructure(imapserver.ExtractBodyStructure(bytes.NewReader(f.raw)))
		}
		for _, bs := range options.BodySection {
			buf := imapserver.ExtractBodySection(bytes.NewReader(f.raw), bs)
			wc := rw.WriteBodySection(bs, int64(len(buf)))
			_, werr := wc.Write(buf)
			cerr := wc.Close()
			if werr != nil {
				return werr
			}
			if cerr != nil {
				return cerr
			}
		}
		for _, bs := range options.BinarySection {
			buf := imapserver.ExtractBinarySection(bytes.NewReader(f.raw), bs)
			wc := rw.WriteBinarySection(bs, int64(len(buf)))
			_, werr := wc.Write(buf)
			cerr := wc.Close()
			if werr != nil {
				return werr
			}
			if cerr != nil {
				return cerr
			}
		}
		for _, bss := range options.BinarySectionSize {
			rw.WriteBinarySectionSize(bss, imapserver.ExtractBinarySectionSize(bytes.NewReader(f.raw), bss))
		}
		if err := rw.Close(); err != nil {
			return err
		}
	}
	return nil
}

func envelopeOf(raw []byte) *imap.Envelope {
	h, err := textproto.ReadHeader(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		return &imap.Envelope{}
	}
	return imapserver.ExtractEnvelope(h)
}

// ---- SEARCH ----

func (sel *selected) search(kind imapserver.NumKind, criteria *imap.SearchCriteria, options *imap.SearchOptions) (*imap.SearchData, error) {
	data := &imap.SearchData{}
	var seqSet imap.SeqSet
	var uidSet imap.UIDSet
	cache := map[uint32]*fetched{}
	for i, uid := range sel.uids {
		rec, ok := sel.recs[uid]
		if !ok {
			continue
		}
		seq := uint32(i + 1)
		f := &fetched{rec: rec}
		cache[uid] = f
		if !sel.matches(f, seq, criteria) {
			continue
		}
		uidSet.AddNum(imap.UID(uid))
		num := uid
		if kind == imapserver.NumKindSeq {
			seqSet.AddNum(seq)
			num = seq
		}
		if data.Min == 0 || num < data.Min {
			data.Min = num
		}
		if num > data.Max {
			data.Max = num
		}
		data.Count++
	}
	if kind == imapserver.NumKindSeq {
		data.All = seqSet
	} else {
		data.All = uidSet
	}
	if options.ReturnSave {
		sel.searchRes = uidSet
	}
	return data, nil
}

func (sel *selected) matches(f *fetched, seq uint32, c *imap.SearchCriteria) bool {
	rec := f.rec
	for _, ss := range c.SeqNum {
		if imap.IsSearchRes(ss) {
			if !sel.searchRes.Contains(imap.UID(rec.UID)) {
				return false
			}
			continue
		}
		if !containsSeq(ss, seq, uint32(len(sel.uids))) {
			return false
		}
	}
	for _, us := range c.UID {
		if imap.IsSearchRes(us) {
			if !sel.searchRes.Contains(imap.UID(rec.UID)) {
				return false
			}
			continue
		}
		var maxUID uint32
		if len(sel.uids) > 0 {
			maxUID = sel.uids[len(sel.uids)-1]
		}
		if !containsUID(us, rec.UID, maxUID) {
			return false
		}
	}
	if !matchDate(rec.InternalDate, c.Since, c.Before) {
		return false
	}
	if !c.SentSince.IsZero() || !c.SentBefore.IsZero() {
		if rec.Date.IsZero() || !matchDate(rec.Date, c.SentSince, c.SentBefore) {
			return false
		}
	}
	for _, fl := range c.Flag {
		if !rec.HasFlag(string(fl)) {
			return false
		}
	}
	for _, fl := range c.NotFlag {
		if rec.HasFlag(string(fl)) {
			return false
		}
	}
	if c.Larger != 0 && rec.Size <= c.Larger {
		return false
	}
	if c.Smaller != 0 && rec.Size >= c.Smaller {
		return false
	}
	for _, hf := range c.Header {
		if !sel.matchHeader(f, hf) {
			return false
		}
	}
	for _, t := range c.Text {
		if !sel.matchText(f, t, true) {
			return false
		}
	}
	for _, b := range c.Body {
		if !sel.matchText(f, b, false) {
			return false
		}
	}
	for i := range c.Not {
		if sel.matches(f, seq, &c.Not[i]) {
			return false
		}
	}
	for i := range c.Or {
		if !sel.matches(f, seq, &c.Or[i][0]) && !sel.matches(f, seq, &c.Or[i][1]) {
			return false
		}
	}
	return true
}

func containsSeq(ss imap.SeqSet, seq, max uint32) bool {
	for _, r := range ss {
		start, stop := r.Start, r.Stop
		if start == 0 {
			start = max
		}
		if stop == 0 {
			stop = max
		}
		if start > stop {
			start, stop = stop, start
		}
		if seq >= start && seq <= stop {
			return true
		}
	}
	return false
}

func containsUID(us imap.UIDSet, uid, max uint32) bool {
	for _, r := range us {
		start, stop := uint32(r.Start), uint32(r.Stop)
		if start == 0 {
			start = max
		}
		if stop == 0 {
			stop = max
		}
		if start > stop {
			start, stop = stop, start
		}
		if uid >= start && uid <= stop {
			return true
		}
	}
	return false
}

func matchDate(t, since, before time.Time) bool {
	t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	if !since.IsZero() && t.Before(since) {
		return false
	}
	if !before.IsZero() && !t.Before(before) {
		return false
	}
	return true
}

// matchHeader answers header criteria from the index when it can (Subject,
// From) and from the raw header otherwise.
func (sel *selected) matchHeader(f *fetched, hf imap.SearchCriteriaHeaderField) bool {
	key := strings.ToLower(hf.Key)
	val := strings.ToLower(hf.Value)
	switch key {
	case "subject":
		return strings.Contains(strings.ToLower(f.rec.Subject), val)
	case "from":
		return strings.Contains(strings.ToLower(f.rec.From), val)
	}
	raw, err := sel.rawFor(f)
	if err != nil {
		return false
	}
	h, err := textproto.ReadHeader(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		return false
	}
	fields := h.FieldsByKey(hf.Key)
	if hf.Value == "" {
		return fields.Len() > 0
	}
	dec := new(mime.WordDecoder)
	for fields.Next() {
		v := fields.Value()
		if d, err := dec.DecodeHeader(v); err == nil {
			v = d
		}
		if strings.Contains(strings.ToLower(v), val) {
			return true
		}
	}
	return false
}

// matchText searches the raw message (header + body for TEXT, body only
// for BODY). It is a plain substring scan over the raw bytes, which is what
// RFC 3501 permits ("MAY be approximate").
func (sel *selected) matchText(f *fetched, pattern string, includeHeader bool) bool {
	if pattern == "" {
		return true
	}
	p := strings.ToLower(pattern)
	if includeHeader && strings.Contains(strings.ToLower(f.rec.Subject), p) {
		return true
	}
	raw, err := sel.rawFor(f)
	if err != nil {
		return false
	}
	body := raw
	if !includeHeader {
		if i := bytes.Index(raw, []byte("\r\n\r\n")); i >= 0 {
			body = raw[i+4:]
		} else if i := bytes.Index(raw, []byte("\n\n")); i >= 0 {
			body = raw[i+2:]
		}
	}
	return bytes.Contains(bytes.ToLower(body), []byte(p))
}
