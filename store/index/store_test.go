package index

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

func newStore(t *testing.T) (*Store, uuid.UUID) {
	t.Helper()
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	s := New(io)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { now = now.Add(time.Second); return now }
	mb := uuid.New()
	if _, err := s.EnsureRoot(context.Background(), mb); err != nil {
		t.Fatal(err)
	}
	return s, mb
}

func rec(size int64, msgid string, refs ...string) Record {
	return Record{MessageID: uuid.New(), Size: size, Subject: "s", MsgIDHeader: msgid, References: refs}
}

func TestEnsureRootDefaults(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	r, err := s.GetRoot(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Folders) != 6 || r.FolderByRole(RoleInbox).Name != "INBOX" || r.FolderByName("Trash").Role != RoleTrash {
		t.Fatalf("folders: %+v", r.Folders)
	}
	if r.ModSeq != 6 || r.Horizon != 1 {
		t.Fatalf("modseq %d horizon %d", r.ModSeq, r.Horizon)
	}
	// Idempotent.
	r2, _ := s.EnsureRoot(ctx, mb)
	if r2.ModSeq != r.ModSeq {
		t.Fatal("EnsureRoot not idempotent")
	}
	ch, _, cur, err := s.Changes(ctx, mb, 0, 100)
	if err != nil || len(ch) != 6 || cur != 6 || ch[0].Kind != KindFolder {
		t.Fatalf("changes: %v %d %v", ch, cur, err)
	}
	ok, _ := s.Exists(ctx, uuid.New())
	if ok {
		t.Fatal("exists on unknown")
	}
}

func TestFolders(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	f, err := s.CreateFolder(ctx, mb, "Projects/2026/Q4", "")
	if err != nil || f.Name != "Projects/2026/Q4" {
		t.Fatal(f, err)
	}
	r, _ := s.GetRoot(ctx, mb)
	if r.FolderByName("Projects") == nil || r.FolderByName("Projects/2026") == nil {
		t.Fatal("parents not created")
	}
	if _, err := s.CreateFolder(ctx, mb, "Projects", ""); !errors.Is(err, ErrExists) {
		t.Fatalf("expected exists: %v", err)
	}
	if _, err := s.CreateFolder(ctx, mb, "/bad", ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid: %v", err)
	}
	if _, err := s.CreateFolder(ctx, mb, "inbox", ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid inbox case: %v", err)
	}
	if _, err := s.CreateFolder(ctx, mb, "Other", RoleTrash); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate role: %v", err)
	}
	// Rename moves children.
	if err := s.RenameFolder(ctx, mb, r.FolderByName("Projects").ID, "Work"); err != nil {
		t.Fatal(err)
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.FolderByName("Work/2026/Q4") == nil || r.FolderByName("Projects") != nil {
		t.Fatalf("rename: %+v", r.Folders)
	}
	if err := s.RenameFolder(ctx, mb, r.FolderByRole(RoleInbox).ID, "X"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inbox rename: %v", err)
	}
	if err := s.RenameFolder(ctx, mb, r.FolderByName("Work").ID, "Work/2026/Sub"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("move under itself: %v", err)
	}
	if err := s.RenameFolder(ctx, mb, r.FolderByName("Work").ID, "Nope/Work"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing parent: %v", err)
	}
	// Delete: not with children, not with role.
	if err := s.DeleteFolder(ctx, mb, r.FolderByName("Work").ID); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("delete with children: %v", err)
	}
	if err := s.DeleteFolder(ctx, mb, r.FolderByRole(RoleSent).ID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("delete role: %v", err)
	}
	if err := s.DeleteFolder(ctx, mb, r.FolderByName("Work/2026/Q4").ID); err != nil {
		t.Fatal(err)
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.FolderByName("Work/2026/Q4") != nil {
		t.Fatal("not deleted")
	}
	// Subscription update.
	uf, err := s.UpdateFolder(ctx, mb, r.FolderByName("Work").ID, func(f *Folder) error { f.Subscribed = false; f.Name = "hack"; return nil })
	if err != nil || uf.Subscribed || uf.Name != "Work" {
		t.Fatalf("update folder: %+v %v", uf, err)
	}
}

func TestAppendListFlagsMoveRemove(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	r, _ := s.GetRoot(ctx, mb)
	inbox := r.FolderByRole(RoleInbox).ID
	trash := r.FolderByRole(RoleTrash).ID

	a, err := s.Append(ctx, mb, inbox, rec(100, "a@x"))
	if err != nil || a.UID != 1 || a.ThreadID != a.MessageID.String() {
		t.Fatalf("%+v %v", a, err)
	}
	b, _ := s.Append(ctx, mb, inbox, rec(200, "b@x", "a@x"))
	if b.UID != 2 || b.ThreadID != a.ThreadID {
		t.Fatalf("threading: %+v", b)
	}
	c, _ := s.Append(ctx, mb, inbox, Record{MessageID: uuid.New(), Size: 50, Flags: []string{"\\seen", "$label1", "\\SEEN"}})
	if c.UID != 3 {
		t.Fatal("uid")
	}
	r, _ = s.GetRoot(ctx, mb)
	f := r.FolderByID(inbox)
	if f.Total != 3 || f.Unseen != 2 || f.Size != 350 || f.UIDNext != 4 || r.Usage.Bytes != 350 || r.Usage.Messages != 3 {
		t.Fatalf("counters: %+v usage %+v", f, r.Usage)
	}
	if len(f.Keywords) != 1 || f.Keywords[0] != "$label1" {
		t.Fatalf("keywords: %v", f.Keywords)
	}
	// List with pagination.
	recs, more, err := s.ListFolder(ctx, mb, inbox, 0, 2)
	if err != nil || !more || len(recs) != 2 || recs[0].UID != 1 {
		t.Fatalf("list: %v %v %v", recs, more, err)
	}
	recs, more, _ = s.ListFolder(ctx, mb, inbox, 2, 10)
	if more || len(recs) != 1 || recs[0].UID != 3 {
		t.Fatalf("list page 2: %v", recs)
	}
	// Locate / thread.
	loc, got, err := s.GetByMessageID(ctx, mb, b.MessageID)
	if err != nil || loc.FolderID != inbox || loc.UID != 2 || got.Size != 200 {
		t.Fatalf("locate: %v %v %v", loc, got, err)
	}
	members, _ := s.ThreadMembers(ctx, mb, a.ThreadID)
	if len(members) != 2 {
		t.Fatalf("thread members: %v", members)
	}
	// Flags.
	upd, err := s.SetFlags(ctx, mb, inbox, 1, func(fl []string) []string { return append(fl, "\\Seen", "\\Flagged") })
	if err != nil || !upd.HasFlag(FlagSeen) || !upd.HasFlag("\\flagged") || upd.ModSeq <= c.ModSeq {
		t.Fatalf("flags: %+v %v", upd, err)
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.FolderByID(inbox).Unseen != 1 {
		t.Fatalf("unseen after seen: %d", r.FolderByID(inbox).Unseen)
	}
	// Move.
	moved, err := s.Move(ctx, mb, inbox, 2, trash)
	if err != nil || moved.UID != 1 || moved.MessageID != b.MessageID {
		t.Fatalf("move: %+v %v", moved, err)
	}
	if _, err := s.GetRecord(ctx, mb, inbox, 2); !errors.Is(err, ErrNotFound) {
		t.Fatal("source not removed")
	}
	loc, _ = s.Locate(ctx, mb, b.MessageID)
	if loc.FolderID != trash || loc.UID != 1 {
		t.Fatalf("locate after move: %v", loc)
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.FolderByID(inbox).Total != 2 || r.FolderByID(trash).Total != 1 || r.FolderByID(trash).Unseen != 1 || r.Usage.Messages != 3 {
		t.Fatalf("counters after move: inbox %+v trash %+v", r.FolderByID(inbox), r.FolderByID(trash))
	}
	// Remove to retention.
	purgeAt := s.Now().Add(24 * time.Hour)
	entry, err := s.Remove(ctx, mb, trash, 1, "expunge", purgeAt)
	if err != nil || entry.MessageID != b.MessageID {
		t.Fatal(err)
	}
	if _, err := s.Locate(ctx, mb, b.MessageID); !errors.Is(err, ErrNotFound) {
		t.Fatal("still locatable")
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.Usage.Messages != 2 || r.Usage.Bytes != 150 || r.FolderByID(trash).Total != 0 {
		t.Fatalf("usage after remove: %+v", r.Usage)
	}
	members, _ = s.ThreadMembers(ctx, mb, a.ThreadID)
	if len(members) != 1 {
		t.Fatalf("thread members after remove: %v", members)
	}
	ents, keys, _, err := s.ListRetention(ctx, mb, time.Time{}, 10)
	if err != nil || len(ents) != 1 || len(keys) != 1 || ents[0].Reason != "expunge" {
		t.Fatalf("retention: %v %v", ents, err)
	}
	ents, _, _, _ = s.ListRetention(ctx, mb, s.Now(), 10)
	if len(ents) != 0 {
		t.Fatal("not due yet should be excluded")
	}
	ents, _, _, _ = s.ListRetention(ctx, mb, purgeAt.Add(time.Hour), 10)
	if len(ents) != 1 {
		t.Fatal("due should be included")
	}
	if err := s.DropRetention(ctx, mb, keys...); err != nil {
		t.Fatal(err)
	}
	ents, _, _, _ = s.ListRetention(ctx, mb, time.Time{}, 10)
	if len(ents) != 0 {
		t.Fatal("drop failed")
	}
	// Change log reflects everything.
	ch, _, cur, _ := s.Changes(ctx, mb, 6, 100)
	ops := ""
	for _, c := range ch {
		ops += c.Op[:1]
	}
	// 3 created, 1 updated (flags), 1 updated (move), 1 destroyed
	if ops != "cccuud" || cur != 12 {
		t.Fatalf("ops %q cur %d", ops, cur)
	}
	ch, _, _, _ = s.Changes(ctx, mb, 12, 100)
	if len(ch) != 0 {
		t.Fatal("no changes expected")
	}
	// Recalculate matches.
	before, _ := s.GetRoot(ctx, mb)
	after, err := s.Recalculate(ctx, mb)
	if err != nil || after.Usage != before.Usage || after.FolderByID(inbox).Unseen != before.FolderByID(inbox).Unseen {
		t.Fatalf("recalc: %+v vs %+v %v", after.Usage, before.Usage, err)
	}
	// Empty folder can be deleted after creating one.
	nf, _ := s.CreateFolder(ctx, mb, "Tmp", "")
	if err := s.DeleteFolder(ctx, mb, nf.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteFolder(ctx, mb, inbox); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inbox delete: %v", err)
	}
}

func TestChangeLogTrimAndHorizon(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	r, _ := s.GetRoot(ctx, mb)
	inbox := r.FolderByRole(RoleInbox).ID
	for i := 0; i < 50; i++ {
		if _, err := s.Append(ctx, mb, inbox, rec(1, fmt.Sprintf("m%d@x", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.TrimChanges(ctx, mb, 10); err != nil {
		t.Fatal(err)
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.Horizon != r.ModSeq-10+1 {
		t.Fatalf("horizon %d modseq %d", r.Horizon, r.ModSeq)
	}
	if _, _, _, err := s.Changes(ctx, mb, 5, 100); !errors.Is(err, ErrNoHorizon) {
		t.Fatalf("expected no horizon, got %v", err)
	}
	ch, _, _, err := s.Changes(ctx, mb, r.Horizon-1, 100)
	if err != nil || len(ch) != 10 {
		t.Fatalf("changes from horizon: %d %v", len(ch), err)
	}
}

func TestWatchNotifyAndDeleteAll(t *testing.T) {
	s, mb := newStore(t)
	ctx := context.Background()
	w, err := s.Watch(ctx, mb)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := s.Notify(ctx, mb, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.Events():
	case <-time.After(time.Second):
		t.Fatal("no event")
	}
	if err := s.Notify(ctx, uuid.New(), nil); err != nil {
		t.Fatalf("notify unknown should be nil: %v", err)
	}
	if err := s.DeleteAll(ctx, mb); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Exists(ctx, mb); ok {
		t.Fatal("still exists")
	}
	if err := s.DeleteAll(ctx, mb); err != nil {
		t.Fatal("second delete should be nil")
	}
}

func TestConcurrentAppendsUniqueUIDs(t *testing.T) {
	s, mb := newStore(t)
	s.Now = time.Now
	ctx := context.Background()
	r, _ := s.GetRoot(ctx, mb)
	inbox := r.FolderByRole(RoleInbox).ID
	const n = 40
	results := make(chan Record, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			rr, err := s.Append(ctx, mb, inbox, rec(1, ""))
			if err != nil {
				errs <- err
				return
			}
			results <- rr
		}()
	}
	seen := map[uint32]bool{}
	seenMod := map[uint64]bool{}
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case rr := <-results:
			if seen[rr.UID] || seenMod[rr.ModSeq] {
				t.Fatalf("duplicate uid %d or modseq %d", rr.UID, rr.ModSeq)
			}
			seen[rr.UID] = true
			seenMod[rr.ModSeq] = true
		}
	}
	r, _ = s.GetRoot(ctx, mb)
	if r.FolderByID(inbox).Total != n || r.Usage.Messages != n {
		t.Fatalf("counters: %d %d", r.FolderByID(inbox).Total, r.Usage.Messages)
	}
}
