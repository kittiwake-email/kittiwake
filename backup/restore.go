package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/mail"
	"kittiwake/service/auditor"
	"kittiwake/store/identity"
	"kittiwake/store/index"
)

// RestoreOptions control a restore.
type RestoreOptions struct {
	// IncludeDeleted also restores messages that were in the retention
	// set (expunged but not yet purged), into the Trash folder.
	IncludeDeleted bool
	// StorageID pins the storage of the new mailbox (0: placement).
	StorageID int64
	// Into, when set, restores into that existing mailbox instead of a
	// new one (used by RestoreMessage and tests); folders are matched by
	// role and name.
	Into uuid.UUID
}

// RestoreResult reports what a restore did.
type RestoreResult struct {
	Mailbox  uuid.UUID `json:"mailbox"`
	Run      string    `json:"run"`
	Messages int       `json:"messages"`
	Folders  int       `json:"folders"`
	Errors   []string  `json:"errors,omitempty"`
}

// Restore rebuilds a mailbox from a run into a new mailbox (side by side:
// the source is never touched) and returns the new mailbox id. The
// operator then re-points accounts at it. Messages get new ids and UIDs;
// flags, dates, folders, keywords and subscriptions are kept.
func (s *Service) Restore(ctx context.Context, mb uuid.UUID, runID string, opts RestoreOptions) (RestoreResult, error) {
	if err := s.authorize(ctx); err != nil {
		return RestoreResult{}, err
	}
	run, err := s.cat.GetRun(ctx, mb, runID)
	if err != nil {
		return RestoreResult{}, err
	}
	man, err := s.loadManifest(ctx, run.ManifestKey)
	if err != nil {
		return RestoreResult{}, err
	}
	target := opts.Into
	if target == uuid.Nil {
		nm, err := s.adm.CreateMailbox(ctx, identity.Mailbox{Quota: man.Quota, StorageID: opts.StorageID})
		if err != nil {
			return RestoreResult{}, err
		}
		target = nm.ID
	}
	res := RestoreResult{Mailbox: target, Run: runID}
	root, err := s.st.Index.EnsureRoot(ctx, target)
	if err != nil {
		return res, err
	}
	r := &restorer{s: s, ctx: ctx, mb: target, root: &root, res: &res}
	for _, fr := range man.Folders {
		fe, err := s.loadFolder(ctx, fr.Records)
		if err != nil {
			r.note("folder %s (%s): %v", fr.Name, fr.ID, err)
			continue
		}
		fid, err := r.folder(fe.Folder.Name, fe.Folder.Role, fe.Folder.Subscribed)
		if err != nil {
			r.note("folder %s: %v", fe.Folder.Name, err)
			continue
		}
		res.Folders++
		for _, m := range fe.Messages {
			if err := r.message(fid, m); err != nil {
				if ctx.Err() != nil {
					return res, err
				}
				r.note("%s/%d (%s): %v", fe.Folder.Name, m.UID, m.MessageID, err)
			}
		}
	}
	if opts.IncludeDeleted && man.Retention != nil {
		retained, err := s.loadRetained(ctx, man.Retention)
		if err != nil {
			r.note("retention: %v", err)
		} else if len(retained) > 0 {
			fid, err := r.folder("Trash", index.RoleTrash, true)
			if err != nil {
				r.note("trash: %v", err)
			} else {
				for _, m := range retained {
					if err := r.message(fid, m.Message); err != nil {
						if ctx.Err() != nil {
							return res, err
						}
						r.note("retained %s: %v", m.MessageID, err)
					}
				}
			}
		}
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: target, Action: "restore", Target: mb.String() + "/" + runID,
		Detail: fmt.Sprintf("messages=%d folders=%d errors=%d", res.Messages, res.Folders, len(res.Errors))})
	s.log.Info("restore", slog.String("source", mb.String()), slog.String("run", runID), slog.String("mailbox", target.String()),
		slog.Int("messages", res.Messages), slog.Int("errors", len(res.Errors)))
	return res, nil
}

// RestoreMessage restores one message of a run into an existing mailbox
// (by default the source mailbox itself), into the folder it was in when
// the backup ran, or INBOX when that folder no longer exists.
func (s *Service) RestoreMessage(ctx context.Context, mb uuid.UUID, runID string, msg uuid.UUID, into uuid.UUID) (RestoreResult, error) {
	if err := s.authorize(ctx); err != nil {
		return RestoreResult{}, err
	}
	if into == uuid.Nil {
		into = mb
	}
	run, err := s.cat.GetRun(ctx, mb, runID)
	if err != nil {
		return RestoreResult{}, err
	}
	man, err := s.loadManifest(ctx, run.ManifestKey)
	if err != nil {
		return RestoreResult{}, err
	}
	root, err := s.st.Index.EnsureRoot(ctx, into)
	if err != nil {
		return RestoreResult{}, err
	}
	res := RestoreResult{Mailbox: into, Run: runID}
	r := &restorer{s: s, ctx: ctx, mb: into, root: &root, res: &res}
	find := func() (Message, string, string, bool) {
		for _, fr := range man.Folders {
			fe, err := s.loadFolder(ctx, fr.Records)
			if err != nil {
				continue
			}
			for _, m := range fe.Messages {
				if m.MessageID == msg {
					return m, fe.Folder.Name, fe.Folder.Role, true
				}
			}
		}
		if retained, err := s.loadRetained(ctx, man.Retention); err == nil {
			for _, m := range retained {
				if m.MessageID == msg {
					return m.Message, "Trash", index.RoleTrash, true
				}
			}
		}
		return Message{}, "", "", false
	}
	m, name, role, ok := find()
	if !ok {
		return res, fmt.Errorf("%w: message %s in run %s", ErrNotFound, msg, runID)
	}
	fid := ""
	if f := root.FolderByName(name); f != nil {
		fid = f.ID
	} else if f := root.FolderByRole(role); role != "" && f != nil {
		fid = f.ID
	} else if f := root.FolderByRole(index.RoleInbox); f != nil {
		fid = f.ID
	} else {
		return res, index.ErrNotFound
	}
	if err := r.message(fid, m); err != nil {
		return res, err
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: into, Action: "restore-message", Target: msg.String(), Detail: fmt.Sprintf("source=%s run=%s folder=%s", mb, runID, fid)})
	return res, nil
}

type restorer struct {
	s    *Service
	ctx  context.Context
	mb   uuid.UUID
	root *index.Root
	res  *RestoreResult
}

func (r *restorer) note(format string, args ...any) {
	if len(r.res.Errors) < 1000 {
		r.res.Errors = append(r.res.Errors, fmt.Sprintf(format, args...))
	}
}

// folder finds or creates the target folder: by role, then by name.
func (r *restorer) folder(name, role string, subscribed bool) (string, error) {
	var f *index.Folder
	if role != "" {
		f = r.root.FolderByRole(role)
	}
	if f == nil {
		f = r.root.FolderByName(name)
	}
	if f == nil {
		for _, x := range r.root.Folders {
			if strings.EqualFold(x.Name, name) {
				f = x
				break
			}
		}
	}
	if f == nil {
		nf, err := r.s.st.Index.CreateFolder(r.ctx, r.mb, name, "")
		if err != nil {
			return "", err
		}
		root, err := r.s.st.Index.GetRoot(r.ctx, r.mb)
		if err != nil {
			return "", err
		}
		*r.root = root
		f = r.root.FolderByID(nf.ID)
	}
	if f.Subscribed != subscribed {
		_, _ = r.s.st.Index.UpdateFolder(r.ctx, r.mb, f.ID, func(x *index.Folder) error { x.Subscribed = subscribed; return nil })
	}
	return f.ID, nil
}

// message appends one backed-up message to a folder of the target mailbox
// under a new message id, through the index's normal append path.
func (r *restorer) message(fid string, m Message) error {
	docBytes, err := readRef(r.ctx, r.s.tg, r.s.ring, m.Doc)
	if err != nil {
		return err
	}
	var doc mail.Document
	if err := json.Unmarshal(docBytes, &doc); err != nil {
		return fmt.Errorf("document: %w", err)
	}
	var raw []byte
	if m.Raw != nil {
		if raw, err = readRef(r.ctx, r.s.tg, r.s.ring, *m.Raw); err != nil {
			return err
		}
	}
	var refs []index.AttachmentRef
	for _, a := range m.Attachments {
		data, err := readRef(r.ctx, r.s.tg, r.s.ring, a.Ref)
		if err != nil {
			return err
		}
		ref, err := r.s.st.Attachments.Upload(r.ctx, a.Hash, data, a.ContentType, r.mb)
		if err != nil {
			return fmt.Errorf("attachment %s: %w", a.Hash, err)
		}
		refs = append(refs, ref)
	}
	msgID := uuid.New()
	rec := index.Record{
		MessageID: msgID, Flags: m.Flags, Size: m.Size, InternalDate: m.InternalDate, Date: m.Date, Subject: m.Subject, From: m.From, FromEmail: m.FromEmail,
		HasAttachments: m.HasAttachments, MsgIDHeader: m.MsgIDHeader, InReplyTo: doc.InReplyTo, References: doc.References, Attachments: refs,
	}
	if rec.Size == 0 {
		rec.Size = doc.Size
	}
	if rec.InternalDate.IsZero() {
		rec.InternalDate = r.s.Now().UTC()
	}
	_, err = r.s.st.Index.Append(r.ctx, r.mb, fid, rec, func(uid uint32, _ uint64) error {
		loc := index.Location{FolderID: fid, UID: uid}.String()
		if err := r.s.st.Messages.Put(r.ctx, r.mb, msgID, docBytes, loc); err != nil {
			return err
		}
		if raw != nil {
			return r.s.st.Messages.PutRaw(r.ctx, r.mb, msgID, raw)
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, index.ErrPending) {
			_ = r.s.st.Messages.Delete(r.ctx, r.mb, msgID)
		}
		return err
	}
	r.res.Messages++
	return nil
}

// jobRetry is how long a failed job waits before the queue retries it.
const jobRetry = 5 * time.Minute
