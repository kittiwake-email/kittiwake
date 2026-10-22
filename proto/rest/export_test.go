package rest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kittiwake/rados"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/service/export"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
	"kittiwake/worker"
)

func identityMailbox() identity.Mailbox { return identity.Mailbox{} }

func TestExportEndpoints(t *testing.T) {
	f := newFixture(t)
	// Not configured: 501 for export, 404 for download, empty list.
	f.expect(f.do("POST", "/v1/admin/mailboxes/"+f.mb.ID.String()+"/export", f.bearer(f.adminTk), nil, ""), 501, "export unconfigured")
	r := f.do("GET", "/v1/admin/exports", f.bearer(f.adminTk), nil, "")
	f.expect(r, 200, "list unconfigured")
	f.expect(f.do("GET", "/v1/admin/exports/nope.tar", f.bearer(f.adminTk), nil, ""), 404, "download unconfigured")

	// Configure an export service and directory on a fresh server.
	c := rados.NewMemoryClient()
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auditStore := audit.New(idxp)
	aud := auditor.New(auditStore, log, nil)
	idx := index.New(idxp)
	msgs, atts := message.New(msgp), attach.New(attp)
	q := queue.New(idxp)
	del := delivery.New(f.ids, idx, msgs, atts, q, aud, log, delivery.Config{})
	exp := export.New(f.ids, idx, msgs, atts, auditStore, del, aud, log)
	dir := t.TempDir()
	f.srv.cfg.Export, f.srv.cfg.ExportDir, f.srv.queue = exp, dir, q
	ctx := context.Background()
	if _, err := del.Deliver(ctx, delivery.Request{Recipient: "wido@example.org", Raw: []byte("From: a@b\r\nSubject: s\r\n\r\nx\r\n")}); err != nil {
		t.Fatal(err)
	}

	f.expect(f.do("POST", "/v1/admin/mailboxes/"+f.mb.ID.String()+"/export", f.bearer(f.user.ID.String()), nil, ""), 401, "non-admin")
	r = f.do("POST", "/v1/admin/mailboxes/"+f.mb.ID.String()+"/export", f.bearer(f.adminTk), map[string]any{"gzip": false}, "")
	f.expect(r, 202, "schedule export")
	if r.body["task_id"] == "" {
		t.Fatal("no task id")
	}
	// Run the worker task.
	w := worker.New(worker.Config{ExportDir: dir, PollInterval: time.Millisecond}, worker.Stores{Identity: f.ids, Index: idx, Messages: msgs, Attachments: atts, Audit: auditStore, Queue: q}, aud, log)
	w.Export = exp
	w.RunOnce(actor.With(ctx, actor.System("cli")))
	r = f.do("GET", "/v1/admin/exports?mailbox="+f.mb.ID.String(), f.bearer(f.adminTk), nil, "")
	f.expect(r, 200, "list")
	items := r.body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items: %v", items)
	}
	name := items[0].(map[string]any)["name"].(string)
	if items[0].(map[string]any)["result"] == nil {
		t.Fatal("result sidecar missing")
	}
	// Download is byte-exact with the file on disk.
	req, _ := http.NewRequest("GET", f.ts.URL+"/v1/admin/exports/"+name, nil)
	req.Header.Set("Authorization", "Bearer "+f.adminTk)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	disk, _ := os.ReadFile(filepath.Join(dir, name))
	if resp.StatusCode != 200 || !bytes.Equal(body, disk) || resp.Header.Get("Content-Type") != "application/x-tar" {
		t.Fatalf("download: %d %d/%d %s", resp.StatusCode, len(body), len(disk), resp.Header.Get("Content-Type"))
	}
	f.expect(f.do("GET", "/v1/admin/exports/../../etc/passwd", f.bearer(f.adminTk), nil, ""), 404, "traversal")
	// Import the archive into a second mailbox.
	mb2, _ := f.ids.CreateMailbox(ctx, identityMailbox())
	req, _ = http.NewRequest("POST", f.ts.URL+"/v1/admin/mailboxes/"+mb2.ID.String()+"/import", bytes.NewReader(disk))
	req.Header.Set("Authorization", "Bearer "+f.adminTk)
	req.Header.Set("Content-Type", "application/x-tar")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(body, []byte(`"messages":1`)) {
		t.Fatalf("import: %d %s", resp.StatusCode, body)
	}
	root, _ := idx.GetRoot(ctx, mb2.ID)
	if root.Usage.Messages != 1 {
		t.Fatal("import did not store the message")
	}
}
