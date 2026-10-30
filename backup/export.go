package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup/maildir"
	"kittiwake/backup/target"
	"kittiwake/mail"
	"kittiwake/service/auditor"
	"kittiwake/store/identity"
)

// ExportManifest is the manifest.json inside an export archive: enough to
// document a chain of custody (what was exported, from which backup, with
// a hash per file).
type ExportManifest struct {
	Version    int             `json:"version"`
	Mailbox    uuid.UUID       `json:"mailbox"`
	Run        string          `json:"run"`
	BackupAt   time.Time       `json:"backup_at"`
	ExportedAt time.Time       `json:"exported_at"`
	ExportedBy string          `json:"exported_by"`
	Host       string          `json:"host"`
	Hold       *identity.Hold  `json:"hold,omitempty"`
	Accounts   []string        `json:"accounts"`
	Folders    []ExportFolder  `json:"folders"`
	Messages   []ExportMessage `json:"messages"`
	AuditDays  []string        `json:"audit_days"`
	AuditCount int             `json:"audit_count"`
	Errors     []string        `json:"errors,omitempty"`
}

// ExportFolder is one folder in the export manifest.
type ExportFolder struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Maildir     string `json:"maildir"`
	Role        string `json:"role,omitempty"`
	UIDValidity uint32 `json:"uidvalidity"`
	Messages    int    `json:"messages"`
}

// ExportMessage is one message file in the export manifest.
type ExportMessage struct {
	MessageID uuid.UUID `json:"message_id"`
	Folder    string    `json:"folder"`
	UID       uint32    `json:"uid"`
	Flags     []string  `json:"flags,omitempty"`
	Size      int64     `json:"size"`
	Path      string    `json:"path"`
	SHA256    string    `json:"sha256"`
}

// ExportResult describes a finished export.
type ExportResult struct {
	Key      string `json:"key"`
	Bytes    int64  `json:"bytes"`
	Messages int    `json:"messages"`
	Errors   []string
}

// Export renders a run as a Maildir++ tar.gz and stores it, unencrypted,
// under the exports prefix on the target with an expiry. requestedBy is
// recorded in the manifest. The archive never touches the mail stores:
// everything comes from the backup.
func (s *Service) Export(ctx context.Context, mb uuid.UUID, runID, requestedBy string) (ExportResult, error) {
	if err := s.authorize(ctx); err != nil {
		return ExportResult{}, err
	}
	run, err := s.cat.GetRun(ctx, mb, runID)
	if err != nil {
		return ExportResult{}, err
	}
	ctx, err = s.enter(ctx, run.Namespace)
	if err != nil {
		return ExportResult{}, err
	}
	man, err := s.loadManifest(ctx, run.ManifestKey)
	if err != nil {
		return ExportResult{}, err
	}
	now := s.Now().UTC()
	spool, err := os.CreateTemp(s.cfg.SpoolDir, "kittiwake-export-*")
	if err != nil {
		return ExportResult{}, err
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	gz := gzip.NewWriter(spool)
	tw := tar.NewWriter(gz)
	prefix := "kittiwake-" + mb.String()
	w := maildir.New(tw, prefix, s.Hostname, now)
	em := ExportManifest{Version: 2, Mailbox: mb, Run: runID, BackupAt: man.Finished, ExportedAt: now, ExportedBy: requestedBy, Host: s.Hostname, Hold: man.Hold, Accounts: man.Accounts, Messages: []ExportMessage{}, AuditDays: []string{}}
	res := ExportResult{}
	for _, fr := range man.Folders {
		fe, err := s.loadFolder(ctx, fr.Records)
		if err != nil {
			em.Errors = append(em.Errors, fmt.Sprintf("folder %s: %v", fr.Name, err))
			continue
		}
		f := maildir.Folder{Name: fe.Folder.Name, Role: fe.Folder.Role, UIDValidity: fe.Folder.UIDValidity, UIDNext: fe.Folder.UIDNext, Subscribed: fe.Folder.Subscribed, Keywords: fe.Folder.Keywords}
		i := 0
		var written []ExportMessage
		files, err := w.Folder(f, func() (maildir.File, error) {
			for i < len(fe.Messages) {
				m := fe.Messages[i]
				i++
				data, err := s.render(ctx, m)
				if err != nil {
					em.Errors = append(em.Errors, fmt.Sprintf("%s/%d (%s): %v", fe.Folder.Name, m.UID, m.MessageID, err))
					continue
				}
				written = append(written, ExportMessage{MessageID: m.MessageID, Folder: fe.Folder.ID, UID: m.UID, Flags: m.Flags, Size: int64(len(data))})
				return maildir.File{UID: m.UID, Flags: m.Flags, InternalDate: m.InternalDate, Data: data}, nil
			}
			return maildir.File{}, io.EOF
		})
		if err != nil {
			return res, err
		}
		for j, wf := range files {
			written[j].Path, written[j].SHA256 = wf.Path, wf.SHA256
			em.Messages = append(em.Messages, written[j])
		}
		em.Folders = append(em.Folders, ExportFolder{ID: fe.Folder.ID, Name: fe.Folder.Name, Maildir: maildir.Name(f), Role: fe.Folder.Role, UIDValidity: fe.Folder.UIDValidity, Messages: len(files)})
		res.Messages += len(files)
	}
	if em.Folders == nil {
		em.Folders = []ExportFolder{}
	}
	if err := w.Finish(); err != nil {
		return res, err
	}
	// Audit log: every day the backup holds, as one JSON-lines file.
	var auditBuf bytes.Buffer
	for _, d := range man.Audit {
		b, err := readRef(ctx, s.be(ctx).tg, s.be(ctx).ring, d.Ref)
		if err != nil {
			em.Errors = append(em.Errors, fmt.Sprintf("audit %s: %v", d.Day, err))
			continue
		}
		lines, err := gunzip(b)
		if err != nil {
			em.Errors = append(em.Errors, fmt.Sprintf("audit %s: %v", d.Day, err))
			continue
		}
		auditBuf.Write(lines)
		em.AuditDays = append(em.AuditDays, d.Day)
		em.AuditCount += d.Records
	}
	if err := w.AddFile("audit.jsonl", auditBuf.Bytes(), now); err != nil {
		return res, err
	}
	manJSON, _ := json.MarshalIndent(em, "", "  ")
	if err := w.AddFile("manifest.json", manJSON, now); err != nil {
		return res, err
	}
	if err := tw.Close(); err != nil {
		return res, err
	}
	if err := gz.Close(); err != nil {
		return res, err
	}
	size, err := spool.Seek(0, io.SeekCurrent)
	if err != nil {
		return res, err
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return res, err
	}
	key := s.exportKey(mb, runID, now)
	if err := s.be(ctx).tg.Put(ctx, key, spool, size, target.PutOptions{ContentType: "application/gzip"}); err != nil {
		return res, err
	}
	res.Key, res.Bytes, res.Errors = key, size, em.Errors
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "export", Target: runID, Detail: fmt.Sprintf("key=%s messages=%d bytes=%d audit=%d errors=%d", key, res.Messages, size, em.AuditCount, len(em.Errors))})
	s.log.Info("export", slog.String("mailbox", mb.String()), slog.String("run", runID), slog.String("key", key), slog.Int("messages", res.Messages), slog.Int64("bytes", size))
	return res, nil
}

// render returns the message bytes: the kept original, else the message
// rendered from its document and attachments, all read from the backup.
func (s *Service) render(ctx context.Context, m Message) ([]byte, error) {
	if m.Raw != nil {
		return readRef(ctx, s.be(ctx).tg, s.be(ctx).ring, *m.Raw)
	}
	docBytes, err := readRef(ctx, s.be(ctx).tg, s.be(ctx).ring, m.Doc)
	if err != nil {
		return nil, err
	}
	var doc mail.Document
	if err := json.Unmarshal(docBytes, &doc); err != nil {
		return nil, err
	}
	atts := map[string]Ref{}
	for _, a := range m.Attachments {
		atts[a.Hash] = a.Ref
	}
	return mail.Render(&doc, func(hash string) ([]byte, error) {
		ref, ok := atts[hash]
		if !ok {
			return nil, fmt.Errorf("attachment %s not in backup", hash)
		}
		return readRef(ctx, s.be(ctx).tg, s.be(ctx).ring, ref)
	})
}

// OpenExport streams a stored export archive from the namespace's target.
func (s *Service) OpenExport(ctx context.Context, namespace, key string) (io.ReadCloser, int64, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, 0, err
	}
	ctx, err := s.enter(ctx, namespace)
	if err != nil {
		return nil, 0, err
	}
	o, err := s.be(ctx).tg.Head(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	rc, err := s.be(ctx).tg.Get(ctx, key, 0, -1)
	return rc, o.Size, err
}
