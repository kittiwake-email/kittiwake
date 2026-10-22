package rest

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"dovetail/proto/httpx"
	"dovetail/service/actor"
	"dovetail/service/export"
	"dovetail/worker"
)

// exportNameRe matches archive names the worker produces.
var exportNameRe = regexp.MustCompile(`^[0-9a-f-]{36}-[0-9TZ]{16}\.tar(\.gz)?$`)

// mailboxExport schedules an export-mailbox task for the worker.
func (s *Server) mailboxExport(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if s.cfg.Export == nil || s.cfg.ExportDir == "" || s.queue == nil {
		httpx.WriteProblem(w, http.StatusNotImplemented, "about:blank", "export not configured (export.directory)")
		return
	}
	var body struct {
		Gzip *bool `json:"gzip"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if _, err := s.admin.GetMailbox(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	gz := true
	if body.Gzip != nil {
		gz = *body.Gzip
	}
	taskID, err := s.queue.Enqueue(r.Context(), worker.TaskExportMailbox, worker.ExportTask{Mailbox: id, Gzip: gz, Requested: actor.From(r.Context()).Username}, time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"task_id": taskID, "mailbox": id, "gzip": gz, "hint": "poll GET /v1/admin/exports?mailbox=" + id.String()})
}

// mailboxImport imports a Maildir archive (tar or tar.gz) from the body.
func (s *Server) mailboxImport(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if s.cfg.Export == nil {
		httpx.WriteProblem(w, http.StatusNotImplemented, "about:blank", "import not configured")
		return
	}
	// Archives are large: lift the JSON body limit for this endpoint.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<40)
	sum, err := s.cfg.Export.ImportArchive(r.Context(), id, r.Body)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"messages": sum.Messages, "folders": sum.Folders, "bytes": sum.Bytes, "errors": sum.Errors})
}

// exportList lists finished exports (optionally for one mailbox).
func (s *Server) exportList(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ExportDir == "" {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": []any{}})
		return
	}
	filter := r.URL.Query().Get("mailbox")
	entries, err := os.ReadDir(s.cfg.ExportDir)
	if err != nil && !os.IsNotExist(err) {
		s.fail(w, r, err)
		return
	}
	items := []map[string]any{}
	for _, e := range entries {
		if e.IsDir() || !exportNameRe.MatchString(e.Name()) {
			continue
		}
		if filter != "" && !strings.HasPrefix(e.Name(), filter+"-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		item := map[string]any{"name": e.Name(), "bytes": info.Size(), "modified": info.ModTime().UTC(), "url": "/v1/admin/exports/" + e.Name()}
		if b, err := os.ReadFile(filepath.Join(s.cfg.ExportDir, e.Name()+".json")); err == nil {
			var res worker.ExportResult
			if json.Unmarshal(b, &res) == nil {
				item["result"] = res
			}
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i]["name"].(string) > items[j]["name"].(string) })
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// exportDownload streams an archive.
func (s *Server) exportDownload(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if s.cfg.ExportDir == "" || !exportNameRe.MatchString(name) {
		httpx.WriteProblem(w, http.StatusNotFound, "about:blank", "no such export")
		return
	}
	p := filepath.Join(s.cfg.ExportDir, name)
	f, err := os.Open(p)
	if err != nil {
		httpx.WriteProblem(w, http.StatusNotFound, "about:blank", "no such export")
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ct := "application/x-tar"
	if strings.HasSuffix(name, ".gz") {
		ct = "application/gzip"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	http.ServeContent(w, r, name, st.ModTime(), f)
	_ = export.ErrNotFound // keep the import explicit for error mapping in rest.go
}
