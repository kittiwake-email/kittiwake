package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup"
	"kittiwake/proto/httpx"
	"kittiwake/service/actor"
	"kittiwake/store/identity"
)

// backupSettingsRequest patches a mailbox's backup settings. Empty
// strings clear a field (inherit again).
type backupSettingsRequest struct {
	Interval  *string `json:"interval,omitempty"`
	Retention *string `json:"retention,omitempty"`
	Disabled  *bool   `json:"disabled,omitempty"`
}

func parseDurationField(v string) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, badRequest(fmt.Sprintf("%q is not a duration (e.g. 24h)", v))
	}
	return d, nil
}

// applyBackupSettings validates and applies a settings patch.
func (s *Server) applyBackupSettings(dst *identity.BackupSettings, req *backupSettingsRequest) error {
	if req.Interval != nil {
		d, err := parseDurationField(*req.Interval)
		if err != nil {
			return err
		}
		if s.cfg.Backup != nil {
			if err := s.cfg.Backup.ValidateInterval(d); err != nil {
				return err
			}
		}
		dst.Interval = d
	}
	if req.Retention != nil {
		d, err := parseDurationField(*req.Retention)
		if err != nil {
			return err
		}
		dst.Retention = d
	}
	if req.Disabled != nil {
		dst.Disabled = *req.Disabled
	}
	return nil
}

// scheduleBackup (re)schedules a mailbox after its settings changed. The
// backup scheduler's reconcile pass would find it anyway; this makes the
// change immediate.
func (s *Server) scheduleBackup(ctx context.Context, m identity.Mailbox) {
	if s.cfg.Backup == nil {
		return
	}
	if err := s.cfg.Backup.Enable(ctx, m); err != nil {
		s.log.Warn("backup scheduling failed", "mailbox", m.ID.String(), "error", err.Error())
	}
}

func (s *Server) backupSummary(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Backup == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	if !s.platformOnly(w, r) {
		return
	}
	sum, err := s.cfg.Backup.Catalog().Summary(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	cfg := s.cfg.Backup.Config()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"summary": sum,
		"policy": map[string]any{
			"default_interval": cfg.DefaultInterval.String(), "min_interval": cfg.MinInterval.String(), "max_interval": cfg.MaxInterval.String(),
			"default_retention": cfg.DefaultRetention.String(), "maintain_interval": cfg.MaintainInterval.String(), "export_expiry": cfg.ExportExpiry.String(),
		},
	})
}

func (s *Server) backupJobs(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Backup == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	var mb uuid.UUID
	if v := r.URL.Query().Get("mailbox"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			s.fail(w, r, badRequest("mailbox must be a UUID"))
			return
		}
		mb = id
	}
	if mb != uuid.Nil {
		if _, err := s.admin.GetMailbox(r.Context(), mb); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	jobs, err := s.cfg.Backup.Catalog().ListJobs(r.Context(), mb, r.URL.Query().Get("kind"), 200)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if jobs == nil {
		jobs = []backup.Job{}
	}
	if actor.From(r.Context()).Namespace != "" {
		// A tenant admin sees only jobs on its own mailboxes.
		kept := jobs[:0]
		for _, j := range jobs {
			if _, err := s.admin.GetMailbox(r.Context(), j.Mailbox); err == nil {
				kept = append(kept, j)
			}
		}
		jobs = kept
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": jobs})
}

func (s *Server) backupJob(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Backup == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	j, err := s.cfg.Backup.Catalog().GetJob(r.Context(), r.PathValue("jid"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if _, err := s.admin.GetMailbox(r.Context(), j.Mailbox); err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, j)
}

// mailboxBackups shows the schedule state, the effective policy and the runs.
func (s *Server) mailboxBackups(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if s.cfg.Backup == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	m, err := s.admin.GetMailbox(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	cat := s.cfg.Backup.Catalog()
	st, err := cat.BackupState(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	runs, err := cat.ListRuns(r.Context(), id, 100)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if runs == nil {
		runs = []backup.Run{}
	}
	every, retention := s.cfg.Backup.Policy(r.Context(), m)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"state": st, "settings": m.Backup,
		"policy": map[string]any{"interval": every.String(), "retention": retention.String(), "enabled": every > 0},
		"runs":   runs,
	})
}

func (s *Server) mailboxBackupGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if s.cfg.Backup == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	if _, err := s.admin.GetMailbox(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	run, err := s.cfg.Backup.Catalog().GetRun(r.Context(), id, r.PathValue("run"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, run)
}

// mailboxBackupRun enqueues a backup run now.
func (s *Server) mailboxBackupRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if s.cfg.Backup == nil || s.queue == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	var body struct {
		Force bool `json:"force"`
		Full  bool `json:"full"`
	}
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &body); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if _, err := s.admin.GetMailbox(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	taskID, err := s.queue.Enqueue(r.Context(), backup.TaskBackup, backup.BackupTask{Mailbox: id, Force: true, Full: body.Full}, time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"task_id": taskID, "mailbox": id, "full": body.Full})
}

// fileJob records a job and enqueues it for the backup nodes.
func (s *Server) fileJob(w http.ResponseWriter, r *http.Request, j backup.Job) {
	if s.cfg.Backup == nil || s.queue == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return
	}
	m, err := s.admin.GetMailbox(r.Context(), j.Mailbox)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	j.ID = uuid.NewString()
	j.Namespace = m.Namespace
	j.State = backup.JobPending
	j.Requested = actor.From(r.Context()).Username
	j.Created = time.Now().UTC()
	if err := s.cfg.Backup.Catalog().PutJob(r.Context(), j); err != nil {
		s.fail(w, r, err)
		return
	}
	if _, err := s.queue.Enqueue(r.Context(), backup.TaskJob, backup.JobTask{Job: j.ID}, time.Time{}); err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, j)
}

func (s *Server) runJob(w http.ResponseWriter, r *http.Request, kind string) (uuid.UUID, string, bool) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return uuid.Nil, "", false
	}
	if s.cfg.Backup == nil {
		s.fail(w, r, backup.ErrNotConfigured)
		return uuid.Nil, "", false
	}
	if _, err := s.admin.GetMailbox(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return uuid.Nil, "", false
	}
	runID := r.PathValue("run")
	if runID != "" {
		if _, err := s.cfg.Backup.Catalog().GetRun(r.Context(), id, runID); err != nil {
			s.fail(w, r, err)
			return uuid.Nil, "", false
		}
	}
	return id, runID, true
}

func (s *Server) mailboxBackupRestore(w http.ResponseWriter, r *http.Request) {
	id, runID, ok := s.runJob(w, r, backup.JobRestore)
	if !ok {
		return
	}
	s.fileJob(w, r, backup.Job{Kind: backup.JobRestore, Mailbox: id, RunID: runID})
}

func (s *Server) mailboxBackupRestoreMessage(w http.ResponseWriter, r *http.Request) {
	id, runID, ok := s.runJob(w, r, backup.JobRestoreMessage)
	if !ok {
		return
	}
	var body struct {
		Message uuid.UUID `json:"message"`
		Into    uuid.UUID `json:"into"`
	}
	if err := decodeJSON(r, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	if body.Message == uuid.Nil {
		s.fail(w, r, badRequest("message is required"))
		return
	}
	if body.Into != uuid.Nil {
		if _, err := s.admin.GetMailbox(r.Context(), body.Into); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	s.fileJob(w, r, backup.Job{Kind: backup.JobRestoreMessage, Mailbox: id, RunID: runID, MessageID: body.Message, Target: body.Into})
}

func (s *Server) mailboxBackupExport(w http.ResponseWriter, r *http.Request) {
	id, runID, ok := s.runJob(w, r, backup.JobExport)
	if !ok {
		return
	}
	s.fileJob(w, r, backup.Job{Kind: backup.JobExport, Mailbox: id, RunID: runID})
}

func (s *Server) mailboxBackupVerify(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.runJob(w, r, backup.JobVerify)
	if !ok {
		return
	}
	s.fileJob(w, r, backup.Job{Kind: backup.JobVerify, Mailbox: id})
}

func (s *Server) mailboxBackupForget(w http.ResponseWriter, r *http.Request) {
	id, _, ok := s.runJob(w, r, backup.JobForget)
	if !ok {
		return
	}
	s.fileJob(w, r, backup.Job{Kind: backup.JobForget, Mailbox: id})
}
