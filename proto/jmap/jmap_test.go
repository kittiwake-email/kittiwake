package jmap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"kittiwake/mail"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/rados"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/service/mailbox"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

type fixture struct {
	t     *testing.T
	srv   *Server
	ids   *identity.Store
	idx   *index.Store
	aud   *auditor.Auditor
	del   *delivery.Service
	mbsvc *mailbox.Service
	mb    identity.Mailbox
	acct  identity.Account
	act   actor.Actor
	ts    *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	ids := identity.New(meta)
	idx := index.New(idxp)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(audit.New(idxp), log, nil)
	msgs := message.New(msgp)
	atts := attach.New(attp)
	q := queue.New(idxp)
	mbsvc := mailbox.New(ids, idx, msgs, atts, q, aud, log, mailbox.Config{TrashRetention: time.Hour, DefaultQuota: identity.QuotaLimits{HardBytes: 1 << 30, HardMessages: 1000}})
	del := delivery.New(ids, idx, msgs, atts, q, aud, log, delivery.Config{InlineMax: 16, DefaultQuota: identity.QuotaLimits{HardBytes: 1 << 30}})
	ctx := context.Background()
	_, _ = ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{})
	acct, _ := ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{"read", "write", "send"}, Enabled: true})
	f := &fixture{t: t, ids: ids, idx: idx, aud: aud, del: del, mbsvc: mbsvc, mb: mb, acct: acct}
	f.act = actor.Actor{AccountID: acct.ID, Username: acct.Username, MailboxID: mb.ID, Roles: acct.Roles, Protocol: "jmap", ClientIP: "::1"}
	f.srv = New(Config{BaseURL: "https://mail.example.org", HoldCheck: func(ctx context.Context, id uuid.UUID) bool {
		v, err := ids.GetMailbox(ctx, id)
		return err == nil && v.Value.Hold != nil
	}}, mbsvc, del, log)
	mux := http.NewServeMux()
	// Route through f.srv dynamically so tests may swap the server (withSubmit).
	mux.Handle("/.well-known/jmap", f.withActor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.srv.SessionHandler().ServeHTTP(w, r) })))
	mux.Handle("/jmap/", f.withActor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.srv.ServeHTTP(w, r) })))
	f.ts = httptest.NewServer(mux)
	t.Cleanup(f.ts.Close)
	return f
}

// withActor injects the fixture actor (overridable via X-Test-Actor header).
func (f *fixture) withActor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		act := f.act
		if h := r.Header.Get("X-Test-Actor"); h != "" {
			_ = json.Unmarshal([]byte(h), &act)
		}
		next.ServeHTTP(w, r.WithContext(actor.With(r.Context(), act)))
	})
}

func (f *fixture) deliver(raw string) delivery.Delivered {
	f.t.Helper()
	out, err := f.del.Deliver(context.Background(), delivery.Request{Recipient: "wido@example.org", Raw: []byte(raw)})
	if err != nil {
		f.t.Fatal(err)
	}
	return out[0]
}

func msg(subject, from, msgID, inReplyTo string) string {
	s := "From: " + from + "\r\nTo: wido@example.org\r\nSubject: " + subject + "\r\nMessage-ID: <" + msgID + ">\r\nDate: Tue, 21 Oct 2026 10:00:00 +0000\r\n"
	if inReplyTo != "" {
		s += "In-Reply-To: <" + inReplyTo + ">\r\n"
	}
	return s + "\r\nbody of " + subject + "\r\n"
}

const pdfData = "%PDF-1.4 this is a fake pdf with enough bytes to be externalized for sure 0123456789"

func mixedMsg() string {
	b64 := base64.StdEncoding.EncodeToString([]byte(pdfData))
	return "From: Alice <alice@example.com>\r\nTo: wido@example.org\r\nSubject: =?utf-8?q?Caf=C3=A9_report?=\r\nMessage-ID: <mixed@example.com>\r\nX-Custom: one\r\nX-Custom: two\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: multipart/alternative; boundary=A\r\n\r\n--A\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nPlain text body\r\n--A\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<p>HTML <b>body</b></p>\r\n--A--\r\n--B\r\nContent-Type: application/pdf; name=report.pdf\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=report.pdf\r\n\r\n" + b64 + "\r\n--B--\r\n"
}

type call struct {
	name string
	args map[string]any
	id   string
}

func (f *fixture) do(calls ...call) []invocation {
	f.t.Helper()
	resp, body := f.doRaw(calls...)
	if resp.StatusCode != 200 {
		f.t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var out response
	if err := json.Unmarshal(body, &out); err != nil {
		f.t.Fatalf("decode: %v: %s", err, body)
	}
	return out.MethodResponses
}

func (f *fixture) doRaw(calls ...call) (*http.Response, []byte) {
	f.t.Helper()
	var mc []any
	for _, c := range calls {
		mc = append(mc, []any{c.name, c.args, c.id})
	}
	body, _ := json.Marshal(map[string]any{"using": []string{CapCore, CapMail, CapQuota}, "methodCalls": mc})
	return f.post("/jmap/api", "application/json", body, nil)
}

func (f *fixture) post(path, ctype string, body []byte, hdr map[string]string) (*http.Response, []byte) {
	f.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, f.ts.URL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", ctype)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (f *fixture) get(path string, hdr map[string]string) (*http.Response, []byte) {
	f.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, f.ts.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (f *fixture) acctArgs(extra map[string]any) map[string]any {
	m := map[string]any{"accountId": f.mb.ID.String()}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func (f *fixture) inbox() string {
	root, _ := f.mbsvc.Root(actor.With(context.Background(), f.act), f.mb.ID)
	return root.FolderByRole(index.RoleInbox).ID
}

func (f *fixture) folderByRole(role string) string {
	root, _ := f.mbsvc.Root(actor.With(context.Background(), f.act), f.mb.ID)
	return root.FolderByRole(role).ID
}

func first(r []invocation, t *testing.T, name string) map[string]any {
	t.Helper()
	if len(r) == 0 {
		t.Fatal("no responses")
	}
	if r[0].name != name {
		t.Fatalf("expected %s, got %s: %v", name, r[0].name, r[0].args)
	}
	return r[0].args
}

func ids(v any) []string {
	arr, _ := v.([]any)
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		out = append(out, x.(string))
	}
	return out
}

func TestSession(t *testing.T) {
	f := newFixture(t)
	resp, body := f.get("/.well-known/jmap", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	var sess map[string]any
	_ = json.Unmarshal(body, &sess)
	caps := sess["capabilities"].(map[string]any)
	if caps[CapCore] == nil || caps[CapMail] == nil {
		t.Fatal("capabilities")
	}
	accounts := sess["accounts"].(map[string]any)
	a := accounts[f.mb.ID.String()].(map[string]any)
	if a["name"] != "wido@example.org" || a["isReadOnly"] != false {
		t.Fatalf("account: %v", a)
	}
	if sess["apiUrl"] != "https://mail.example.org/jmap/api" || sess["primaryAccounts"].(map[string]any)[CapMail] != f.mb.ID.String() {
		t.Fatalf("urls: %v", sess)
	}
	if !strings.Contains(sess["downloadUrl"].(string), "{blobId}") {
		t.Fatal("downloadUrl")
	}
	// Admin without mailbox: empty accounts.
	adm, _ := json.Marshal(actor.Actor{Username: "root@example.org", Roles: []string{"admin"}})
	_, body = f.get("/.well-known/jmap", map[string]string{"X-Test-Actor": string(adm)})
	_ = json.Unmarshal(body, &sess)
	if len(sess["accounts"].(map[string]any)) != 0 {
		t.Fatal("admin accounts should be empty")
	}
}

func TestEchoAndErrors(t *testing.T) {
	f := newFixture(t)
	r := f.do(call{"Core/echo", map[string]any{"hello": "world"}, "c0"})
	if first(r, t, "Core/echo")["hello"] != "world" || r[0].callID != "c0" {
		t.Fatal("echo")
	}
	r = f.do(call{"Nope/get", map[string]any{}, "c1"})
	if r[0].name != "error" || r[0].args["type"] != "unknownMethod" {
		t.Fatalf("unknown method: %v", r[0])
	}
	r = f.do(call{"Mailbox/get", map[string]any{"accountId": f.mb.ID.String(), "ids": "notarray"}, "c2"})
	if r[0].args["type"] != "invalidArguments" {
		t.Fatalf("invalid args: %v", r[0])
	}
	r = f.do(call{"Mailbox/get", map[string]any{"accountId": uuid.NewString()}, "c3"})
	if r[0].args["type"] != "accountNotFound" {
		t.Fatalf("other account: %v", r[0])
	}
	// Request-level errors.
	resp, body := f.post("/jmap/api", "application/json", []byte("{not json"), nil)
	if resp.StatusCode != 400 || !strings.Contains(string(body), "notJSON") {
		t.Fatalf("notJSON: %d %s", resp.StatusCode, body)
	}
	resp, body = f.post("/jmap/api", "application/json", []byte(`{"using":["urn:x"],"methodCalls":[]}`), nil)
	if resp.StatusCode != 400 || !strings.Contains(string(body), "unknownCapability") {
		t.Fatalf("unknownCapability: %d %s", resp.StatusCode, body)
	}
	var many []call
	for i := 0; i < 20; i++ {
		many = append(many, call{"Core/echo", map[string]any{}, "x"})
	}
	resp, body = f.doRaw(many...)
	if resp.StatusCode != 400 || !strings.Contains(string(body), "maxCallsInRequest") {
		t.Fatalf("limit: %d %s", resp.StatusCode, body)
	}
}

func TestMailboxGetAndSet(t *testing.T) {
	f := newFixture(t)
	r := f.do(call{"Mailbox/get", f.acctArgs(nil), "0"})
	res := first(r, t, "Mailbox/get")
	list := res["list"].([]any)
	if len(list) != 6 {
		t.Fatalf("default folders: %d", len(list))
	}
	var inboxID string
	for _, m := range list {
		mm := m.(map[string]any)
		if mm["role"] == "inbox" {
			inboxID = mm["id"].(string)
			if mm["name"] != "INBOX" || mm["parentId"] != nil || mm["myRights"].(map[string]any)["mayRename"] != false {
				t.Fatalf("inbox: %v", mm)
			}
		}
	}
	r = f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{inboxID, "nope"}, "properties": []string{"name", "role"}}), "1"})
	res = first(r, t, "Mailbox/get")
	if len(res["list"].([]any)) != 1 || len(res["notFound"].([]any)) != 1 {
		t.Fatalf("ids: %v", res)
	}
	if obj := res["list"].([]any)[0].(map[string]any); obj["name"] != "INBOX" || obj["id"] != inboxID || obj["totalEmails"] != nil {
		t.Fatalf("properties filter: %v", obj)
	}
	// Create nested with #ref to creation in the same call.
	r = f.do(call{"Mailbox/set", f.acctArgs(map[string]any{"create": map[string]any{
		"a": map[string]any{"name": "Projects"},
		"b": map[string]any{"name": "2026", "parentId": "#a", "isSubscribed": false},
	}}), "2"})
	res = first(r, t, "Mailbox/set")
	created := res["created"].(map[string]any)
	if len(created) != 2 || len(res["notCreated"].(map[string]any)) != 0 {
		t.Fatalf("create: %v", res)
	}
	projID := created["a"].(map[string]any)["id"].(string)
	subID := created["b"].(map[string]any)["id"].(string)
	r = f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{subID}}), "3"})
	sub := first(r, t, "Mailbox/get")["list"].([]any)[0].(map[string]any)
	if sub["name"] != "2026" || sub["parentId"] != projID || sub["isSubscribed"] != false {
		t.Fatalf("nested: %v", sub)
	}
	// Query by parentId and name sort.
	r = f.do(call{"Mailbox/query", f.acctArgs(map[string]any{"filter": map[string]any{"parentId": projID}}), "4"})
	if got := ids(first(r, t, "Mailbox/query")["ids"]); len(got) != 1 || got[0] != subID {
		t.Fatalf("query parentId: %v", got)
	}
	r = f.do(call{"Mailbox/query", f.acctArgs(map[string]any{"filter": map[string]any{"parentId": nil}, "sort": []any{map[string]any{"property": "name"}}, "calculateTotal": true}), "4b"})
	q := first(r, t, "Mailbox/query")
	if q["total"].(float64) != 7 {
		t.Fatalf("top-level total: %v", q)
	}
	// Rename and reparent.
	r = f.do(call{"Mailbox/set", f.acctArgs(map[string]any{"update": map[string]any{
		subID: map[string]any{"name": "Q4", "parentId": nil},
	}}), "5"})
	res = first(r, t, "Mailbox/set")
	if len(res["updated"].(map[string]any)) != 1 || len(res["notUpdated"].(map[string]any)) != 0 {
		t.Fatalf("rename: %v", res)
	}
	r = f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{subID}}), "6"})
	sub = first(r, t, "Mailbox/get")["list"].([]any)[0].(map[string]any)
	if sub["name"] != "Q4" || sub["parentId"] != nil {
		t.Fatalf("after rename: %v", sub)
	}
	// Non-settable property.
	r = f.do(call{"Mailbox/set", f.acctArgs(map[string]any{"update": map[string]any{subID: map[string]any{"totalEmails": 5}}}), "7"})
	if nu := first(r, t, "Mailbox/set")["notUpdated"].(map[string]any); nu[subID].(map[string]any)["type"] != "invalidProperties" {
		t.Fatalf("not settable: %v", nu)
	}
	// Destroy with email inside: refused, then with onDestroyRemoveEmails.
	d := f.deliver(msg("to move", "a@b.c", "mv@x", ""))
	f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{d.MessageID.String(): map[string]any{"mailboxIds": map[string]any{subID: true}}}}), "8"})
	r = f.do(call{"Mailbox/set", f.acctArgs(map[string]any{"destroy": []string{subID}}), "9"})
	if nd := first(r, t, "Mailbox/set")["notDestroyed"].(map[string]any); nd[subID].(map[string]any)["type"] != "mailboxHasEmail" {
		t.Fatalf("destroy non-empty: %v", nd)
	}
	r = f.do(call{"Mailbox/set", f.acctArgs(map[string]any{"destroy": []string{subID, projID}, "onDestroyRemoveEmails": true}), "10"})
	res = first(r, t, "Mailbox/set")
	if len(ids(res["destroyed"])) != 2 {
		t.Fatalf("destroy: %v", res)
	}
	// Inbox cannot be destroyed.
	r = f.do(call{"Mailbox/set", f.acctArgs(map[string]any{"destroy": []string{inboxID}}), "11"})
	if nd := first(r, t, "Mailbox/set")["notDestroyed"].(map[string]any); nd[inboxID] == nil {
		t.Fatal("inbox destroyed")
	}
	// Mailbox/changes sees created and destroyed.
	r = f.do(call{"Mailbox/changes", f.acctArgs(map[string]any{"sinceState": "0"}), "12"})
	ch := first(r, t, "Mailbox/changes")
	if len(ids(ch["created"])) != 6 || len(ids(ch["destroyed"])) != 0 {
		// Created-then-destroyed within window are omitted entirely.
		t.Fatalf("mailbox changes: %v", ch)
	}
	if ch["newState"] == "0" || ch["hasMoreChanges"] != false {
		t.Fatalf("changes state: %v", ch)
	}
}

func TestEmailQueryGetAndBlobs(t *testing.T) {
	f := newFixture(t)
	d1 := f.deliver(msg("Invoice March", "billing@acme.example", "inv@acme", ""))
	time.Sleep(2 * time.Millisecond)
	d2 := f.deliver(msg("Re: Invoice March", "wido@example.org", "re@x", "inv@acme"))
	time.Sleep(2 * time.Millisecond)
	d3 := f.deliver(mixedMsg())
	inbox := f.inbox()

	// Query default sort (receivedAt desc), with inMailbox and total.
	r := f.do(call{"Email/query", f.acctArgs(map[string]any{"filter": map[string]any{"inMailbox": inbox}, "calculateTotal": true}), "q"})
	q := first(r, t, "Email/query")
	got := ids(q["ids"])
	if len(got) != 3 || got[0] != d3.MessageID.String() || q["total"].(float64) != 3 || q["position"].(float64) != 0 {
		t.Fatalf("query: %v", q)
	}
	// Text filter + asc sort.
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"filter": map[string]any{"text": "invoice"}, "sort": []any{map[string]any{"property": "receivedAt", "isAscending": true}}}), "q2"})
	if got = ids(first(r, t, "Email/query")["ids"]); len(got) != 2 || got[0] != d1.MessageID.String() {
		t.Fatalf("text filter: %v", got)
	}
	// AND operator.
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"filter": map[string]any{"operator": "AND", "conditions": []any{map[string]any{"inMailbox": inbox}, map[string]any{"from": "acme"}}}}), "q2b"})
	if got = ids(first(r, t, "Email/query")["ids"]); len(got) != 1 {
		t.Fatalf("AND filter: %v", got)
	}
	// position/limit and negative position.
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"position": 1, "limit": 1}), "q3"})
	q = first(r, t, "Email/query")
	if got = ids(q["ids"]); len(got) != 1 || got[0] != d2.MessageID.String() || q["limit"].(float64) != 1 {
		t.Fatalf("paging: %v", q)
	}
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"position": -1}), "q4"})
	if got = ids(first(r, t, "Email/query")["ids"]); len(got) != 1 || got[0] != d1.MessageID.String() {
		t.Fatalf("negative position: %v", got)
	}
	// collapseThreads.
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"collapseThreads": true, "calculateTotal": true}), "q5"})
	if q = first(r, t, "Email/query"); q["total"].(float64) != 2 {
		t.Fatalf("collapse: %v", q)
	}
	// Unsupported sort / filter.
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"sort": []any{map[string]any{"property": "cc"}}}), "q6"})
	if r[0].args["type"] != "unsupportedSort" {
		t.Fatalf("unsupported sort: %v", r[0])
	}
	r = f.do(call{"Email/query", f.acctArgs(map[string]any{"filter": map[string]any{"operator": "OR", "conditions": []any{}}}), "q7"})
	if r[0].args["type"] != "unsupportedFilter" {
		t.Fatalf("unsupported filter: %v", r[0])
	}

	// Back-reference Email/query -> Email/get.
	r = f.do(
		call{"Email/query", f.acctArgs(map[string]any{"filter": map[string]any{"subject": "Café"}}), "a"},
		call{"Email/get", f.acctArgs(map[string]any{"#ids": map[string]any{"resultOf": "a", "name": "Email/query", "path": "/ids"},
			"properties":         []string{"subject", "from", "hasAttachment", "bodyStructure", "textBody", "htmlBody", "attachments", "bodyValues", "headers", "header:X-Custom:all", "header:Subject:asText", "header:From:asAddresses", "header:Message-ID:asMessageIds", "keywords", "mailboxIds", "threadId", "blobId", "receivedAt", "messageId"},
			"fetchAllBodyValues": true, "maxBodyValueBytes": 5}), "b"},
	)
	if len(r) != 2 || r[1].name != "Email/get" {
		t.Fatalf("backref: %v", r)
	}
	eg := r[1].args
	if len(eg["list"].([]any)) != 1 {
		t.Fatalf("get list: %v", eg)
	}
	e := eg["list"].([]any)[0].(map[string]any)
	if e["subject"] != "Café report" || e["hasAttachment"] != true || e["blobId"] != "m-"+d3.MessageID.String() || e["id"] != d3.MessageID.String() {
		t.Fatalf("email: %v", e)
	}
	if e["mailboxIds"].(map[string]any)[inbox] != true || e["messageId"].([]any)[0] != "mixed@example.com" {
		t.Fatalf("mailboxIds/messageId: %v", e)
	}
	from := e["from"].([]any)[0].(map[string]any)
	if from["name"] != "Alice" || from["email"] != "alice@example.com" {
		t.Fatalf("from: %v", from)
	}
	if e["header:Subject:asText"] != "Café report" {
		t.Fatalf("header asText: %v", e["header:Subject:asText"])
	}
	if xc := e["header:X-Custom:all"].([]any); len(xc) != 2 || strings.TrimSpace(xc[1].(string)) != "two" {
		t.Fatalf("header all: %v", xc)
	}
	if ha := e["header:From:asAddresses"].([]any); len(ha) != 1 || ha[0].(map[string]any)["email"] != "alice@example.com" {
		t.Fatalf("header asAddresses: %v", ha)
	}
	if mi := e["header:Message-ID:asMessageIds"].([]any); mi[0] != "mixed@example.com" {
		t.Fatalf("asMessageIds: %v", mi)
	}
	if hdrs := e["headers"].([]any); len(hdrs) < 7 || hdrs[0].(map[string]any)["name"] != "From" {
		t.Fatalf("headers: %v", hdrs)
	}
	bs := e["bodyStructure"].(map[string]any)
	if bs["type"] != "multipart/mixed" || len(bs["subParts"].([]any)) != 2 {
		t.Fatalf("bodyStructure: %v", bs)
	}
	tb := e["textBody"].([]any)[0].(map[string]any)
	hb := e["htmlBody"].([]any)[0].(map[string]any)
	if tb["type"] != "text/plain" || hb["type"] != "text/html" || tb["partId"] != "1.1" || tb["charset"] != "utf-8" {
		t.Fatalf("bodies: %v %v", tb, hb)
	}
	att := e["attachments"].([]any)[0].(map[string]any)
	attBlob := att["blobId"].(string)
	if att["name"] != "report.pdf" || att["type"] != "application/pdf" || !strings.HasPrefix(attBlob, "a-") || att["disposition"] != "attachment" {
		t.Fatalf("attachment: %v", att)
	}
	bv := e["bodyValues"].(map[string]any)
	if v := bv["1.1"].(map[string]any); v["value"] != "Plain" || v["isTruncated"] != true {
		t.Fatalf("bodyValues: %v", bv)
	}
	if v := bv["1.2"].(map[string]any); !strings.Contains(v["value"].(string), "<p>") {
		t.Fatalf("html bodyValue: %v", bv)
	}
	// Only index properties: no document fetch needed.
	r = f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{d1.MessageID.String(), "bogus"}, "properties": []string{"size", "receivedAt"}}), "c"})
	eg = first(r, t, "Email/get")
	if len(eg["list"].([]any)) != 1 || len(eg["notFound"].([]any)) != 1 {
		t.Fatalf("partial get: %v", eg)
	}

	// Downloads: attachment, raw message (byte-exact), part.
	resp, body := f.get("/jmap/download/"+f.mb.ID.String()+"/"+attBlob+"/report.pdf", nil)
	if resp.StatusCode != 200 || string(body) != pdfData || resp.Header.Get("Content-Type") != "application/pdf" || !strings.Contains(resp.Header.Get("Content-Disposition"), "report.pdf") {
		t.Fatalf("attachment download: %d %q %v", resp.StatusCode, body, resp.Header)
	}
	resp, body = f.get("/jmap/download/"+f.mb.ID.String()+"/m-"+d3.MessageID.String()+"/msg.eml?accept=text/plain", nil)
	if resp.StatusCode != 200 || string(body) != rendered(t, mixedMsg()) || resp.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("raw download: %d", resp.StatusCode)
	}
	resp, body = f.get("/jmap/download/"+f.mb.ID.String()+"/"+hb["blobId"].(string)+"/part.html", nil)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "<b>body</b>") {
		t.Fatalf("part download: %d %s", resp.StatusCode, body)
	}
	resp, _ = f.get("/jmap/download/"+f.mb.ID.String()+"/a-"+strings.Repeat("0", 64)+"/x", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("missing blob: %d", resp.StatusCode)
	}
	resp, _ = f.get("/jmap/download/"+uuid.NewString()+"/"+attBlob+"/x", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("other account: %d", resp.StatusCode)
	}

	// Thread/get.
	r = f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{d2.MessageID.String()}, "properties": []string{"threadId"}}), "t0"})
	tid := first(r, t, "Email/get")["list"].([]any)[0].(map[string]any)["threadId"].(string)
	r = f.do(call{"Thread/get", f.acctArgs(map[string]any{"ids": []string{tid, "nothread"}}), "t1"})
	tg := first(r, t, "Thread/get")
	th := tg["list"].([]any)[0].(map[string]any)
	if em := ids(th["emailIds"]); len(em) != 2 || em[0] != d1.MessageID.String() || em[1] != d2.MessageID.String() {
		t.Fatalf("thread: %v", th)
	}
	if len(tg["notFound"].([]any)) != 1 {
		t.Fatalf("thread notFound: %v", tg)
	}
	// Quota/get.
	r = f.do(call{"Quota/get", f.acctArgs(nil), "qu"})
	ql := first(r, t, "Quota/get")["list"].([]any)
	if len(ql) != 2 {
		t.Fatalf("quota: %v", ql)
	}
	for _, q := range ql {
		qq := q.(map[string]any)
		if qq["id"] == "count" && (qq["used"].(float64) != 3 || qq["hardLimit"].(float64) != 1000) {
			t.Fatalf("quota count: %v", qq)
		}
	}
	// Identity/get.
	r = f.do(call{"Identity/get", f.acctArgs(nil), "id"})
	if il := first(r, t, "Identity/get")["list"].([]any); len(il) != 1 || il[0].(map[string]any)["email"] != "wido@example.org" {
		t.Fatalf("identity: %v", il)
	}
}

func TestEmailSetAndChanges(t *testing.T) {
	f := newFixture(t)
	d := f.deliver(msg("one", "a@b.c", "one@x", ""))
	id := d.MessageID.String()
	inbox := f.inbox()
	archive := f.folderByRole(index.RoleArchive)
	r := f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{inbox}, "properties": []string{"unreadEmails"}}), "s0"})
	state := first(r, t, "Mailbox/get")["state"].(string)

	// Keyword patch.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{id: map[string]any{"keywords/$seen": true, "keywords/$label1": true}}}), "s1"})
	es := first(r, t, "Email/set")
	if _, ok := es["updated"].(map[string]any)[id]; !ok || len(es["notUpdated"].(map[string]any)) != 0 {
		t.Fatalf("set keywords: %v", es)
	}
	r = f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{id}, "properties": []string{"keywords"}}), "s2"})
	kw := first(r, t, "Email/get")["list"].([]any)[0].(map[string]any)["keywords"].(map[string]any)
	if kw["$seen"] != true || kw["$label1"] != true {
		t.Fatalf("keywords: %v", kw)
	}
	r = f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{inbox}, "properties": []string{"unreadEmails"}}), "s2b"})
	if first(r, t, "Mailbox/get")["list"].([]any)[0].(map[string]any)["unreadEmails"].(float64) != 0 {
		t.Fatal("unread count after $seen")
	}
	// Full keywords replace.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{id: map[string]any{"keywords": map[string]any{"$flagged": true}}}}), "s3"})
	first(r, t, "Email/set")
	r = f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{id}, "properties": []string{"keywords"}}), "s4"})
	kw = first(r, t, "Email/get")["list"].([]any)[0].(map[string]any)["keywords"].(map[string]any)
	if len(kw) != 1 || kw["$flagged"] != true {
		t.Fatalf("replace keywords: %v", kw)
	}
	// Move via mailboxIds patch.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{id: map[string]any{"mailboxIds/" + archive: true, "mailboxIds/" + inbox: nil}}}), "s5"})
	es = first(r, t, "Email/set")
	if len(es["notUpdated"].(map[string]any)) != 0 {
		t.Fatalf("move: %v", es)
	}
	r = f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{id}, "properties": []string{"mailboxIds"}}), "s6"})
	if mi := first(r, t, "Email/get")["list"].([]any)[0].(map[string]any)["mailboxIds"].(map[string]any); mi[archive] != true || len(mi) != 1 {
		t.Fatalf("after move: %v", mi)
	}
	// Two mailboxes refused.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{id: map[string]any{"mailboxIds": map[string]any{archive: true, inbox: true}}}}), "s7"})
	if nu := first(r, t, "Email/set")["notUpdated"].(map[string]any); nu[id].(map[string]any)["type"] != "invalidProperties" {
		t.Fatalf("two mailboxes: %v", nu)
	}
	// ifInState mismatch.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"ifInState": state, "update": map[string]any{}}), "s8"})
	if r[0].args["type"] != "stateMismatch" {
		t.Fatalf("stateMismatch: %v", r[0])
	}
	// Create refused.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"create": map[string]any{"x": map[string]any{}}}), "s9"})
	if nc := first(r, t, "Email/set")["notCreated"].(map[string]any); nc["x"] == nil {
		t.Fatal("create should be notCreated")
	}
	// Destroy.
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"destroy": []string{id, uuid.NewString()}}), "s10"})
	es = first(r, t, "Email/set")
	if len(ids(es["destroyed"])) != 1 || len(es["notDestroyed"].(map[string]any)) != 1 {
		t.Fatalf("destroy: %v", es)
	}
	// Changes since `state` (taken after d existed): d was updated then
	// destroyed -> destroyed only; a fresh one shows as created.
	d2 := f.deliver(msg("two", "a@b.c", "two@x", ""))
	r = f.do(call{"Email/changes", f.acctArgs(map[string]any{"sinceState": state}), "s11"})
	ch := first(r, t, "Email/changes")
	if c := ids(ch["created"]); len(c) != 1 || c[0] != d2.MessageID.String() {
		t.Fatalf("changes created: %v", ch)
	}
	if dd := ids(ch["destroyed"]); len(dd) != 1 || dd[0] != id || len(ids(ch["updated"])) != 0 {
		t.Fatalf("changes destroyed: %v", ch)
	}
	// Created then destroyed within the window is omitted entirely.
	d4 := f.deliver(msg("four", "a@b.c", "four@x", ""))
	f.do(call{"Email/set", f.acctArgs(map[string]any{"destroy": []string{d4.MessageID.String()}}), "s11b"})
	r = f.do(call{"Email/changes", f.acctArgs(map[string]any{"sinceState": state}), "s11c"})
	ch = first(r, t, "Email/changes")
	if c := ids(ch["created"]); len(c) != 1 || len(ids(ch["destroyed"])) != 1 {
		t.Fatalf("created+destroyed should be omitted: %v", ch)
	}
	// Changes from a state after creation: updated then destroyed -> destroyed only.
	d3 := f.deliver(msg("three", "a@b.c", "three@x", ""))
	r = f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{}}), "s12"})
	state2 := first(r, t, "Mailbox/get")["state"].(string)
	f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{d3.MessageID.String(): map[string]any{"keywords/$seen": true}}}), "s13"})
	f.do(call{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{d2.MessageID.String(): map[string]any{"keywords/$seen": true}}}), "s14"})
	f.do(call{"Email/set", f.acctArgs(map[string]any{"destroy": []string{d3.MessageID.String()}}), "s15"})
	r = f.do(call{"Email/changes", f.acctArgs(map[string]any{"sinceState": state2}), "s16"})
	ch = first(r, t, "Email/changes")
	if u := ids(ch["updated"]); len(u) != 1 || u[0] != d2.MessageID.String() {
		t.Fatalf("updated: %v", ch)
	}
	if dd := ids(ch["destroyed"]); len(dd) != 1 || dd[0] != d3.MessageID.String() {
		t.Fatalf("destroyed: %v", ch)
	}
	// maxChanges paging.
	r = f.do(call{"Email/changes", f.acctArgs(map[string]any{"sinceState": state2, "maxChanges": 1}), "s17"})
	ch = first(r, t, "Email/changes")
	if ch["hasMoreChanges"] != true || len(ids(ch["updated"]))+len(ids(ch["destroyed"])) != 1 {
		t.Fatalf("maxChanges: %v", ch)
	}
	r = f.do(call{"Email/changes", f.acctArgs(map[string]any{"sinceState": ch["newState"]}), "s18"})
	ch2 := first(r, t, "Email/changes")
	if ch2["hasMoreChanges"] != false || len(ids(ch2["updated"]))+len(ids(ch2["destroyed"])) < 1 {
		t.Fatalf("maxChanges page 2: %v", ch2)
	}
	// Bad state.
	r = f.do(call{"Email/changes", f.acctArgs(map[string]any{"sinceState": "x"}), "s19"})
	if r[0].args["type"] != "invalidArguments" {
		t.Fatalf("bad state: %v", r[0])
	}
}

func TestImportViaUpload(t *testing.T) {
	f := newFixture(t)
	raw := msg("imported", "me@example.org", "imp@x", "")
	resp, body := f.post("/jmap/upload/"+f.mb.ID.String(), "message/rfc822", []byte(raw), nil)
	if resp.StatusCode != 201 {
		t.Fatalf("upload: %d %s", resp.StatusCode, body)
	}
	var up map[string]any
	_ = json.Unmarshal(body, &up)
	blobID := up["blobId"].(string)
	if !strings.HasPrefix(blobID, "u-") || up["size"].(float64) != float64(len(raw)) || up["type"] != "message/rfc822" {
		t.Fatalf("upload resp: %v", up)
	}
	drafts := f.folderByRole(index.RoleDrafts)
	r := f.do(
		call{"Email/import", f.acctArgs(map[string]any{"emails": map[string]any{
			"i1": map[string]any{"blobId": blobID, "mailboxIds": map[string]any{drafts: true}, "keywords": map[string]any{"$draft": true, "$seen": true}, "receivedAt": "2020-01-02T03:04:05Z"},
			"i2": map[string]any{"blobId": "u-nope", "mailboxIds": map[string]any{drafts: true}},
			"i3": map[string]any{"blobId": blobID, "mailboxIds": map[string]any{drafts: true, f.inbox(): true}},
		}}), "i"},
		call{"Email/get", f.acctArgs(map[string]any{"ids": []string{"#i1"}, "properties": []string{"keywords", "receivedAt", "mailboxIds", "subject"}}), "g"},
	)
	imp := first(r, t, "Email/import")
	created := imp["created"].(map[string]any)
	nc := imp["notCreated"].(map[string]any)
	if len(created) != 1 || nc["i2"].(map[string]any)["type"] != "blobNotFound" || nc["i3"].(map[string]any)["type"] != "invalidProperties" {
		t.Fatalf("import: %v", imp)
	}
	c := created["i1"].(map[string]any)
	if !strings.HasPrefix(c["blobId"].(string), "m-") || c["threadId"] == nil {
		t.Fatalf("created: %v", c)
	}
	e := r[1].args["list"].([]any)[0].(map[string]any)
	if e["subject"] != "imported" || e["receivedAt"] != "2020-01-02T03:04:05Z" || e["keywords"].(map[string]any)["$draft"] != true || e["mailboxIds"].(map[string]any)[drafts] != true {
		t.Fatalf("imported email: %v", e)
	}
	// Download of an upload blob and other-account denial.
	resp, body = f.get("/jmap/download/"+f.mb.ID.String()+"/"+blobID+"/x.eml", nil)
	if resp.StatusCode != 200 || string(body) != raw {
		t.Fatalf("download upload: %d", resp.StatusCode)
	}
	resp, _ = f.post("/jmap/upload/"+uuid.NewString(), "text/plain", []byte("x"), nil)
	if resp.StatusCode != 404 {
		t.Fatalf("upload other account: %d", resp.StatusCode)
	}
}

func TestForbiddenAndHold(t *testing.T) {
	f := newFixture(t)
	d := f.deliver(msg("held", "a@b.c", "held@x", ""))
	other, _ := json.Marshal(actor.Actor{AccountID: uuid.New(), Username: "other@example.org", MailboxID: uuid.New(), Roles: []string{"read", "write"}, Protocol: "jmap"})
	body, _ := json.Marshal(map[string]any{"using": []string{CapCore, CapMail}, "methodCalls": []any{[]any{"Mailbox/get", f.acctArgs(nil), "x"}}})
	resp, out := f.post("/jmap/api", "application/json", body, map[string]string{"X-Test-Actor": string(other)})
	if resp.StatusCode != 200 || !strings.Contains(string(out), "accountNotFound") {
		t.Fatalf("other actor: %d %s", resp.StatusCode, out)
	}
	// Admin support access works.
	adm, _ := json.Marshal(actor.Actor{AccountID: uuid.New(), Username: "root@example.org", Roles: []string{"admin"}, Protocol: "admin"})
	resp, out = f.post("/jmap/api", "application/json", body, map[string]string{"X-Test-Actor": string(adm)})
	if resp.StatusCode != 200 || !strings.Contains(string(out), `"INBOX"`) {
		t.Fatalf("admin access: %d %s", resp.StatusCode, out)
	}
	// Read-only role: writes forbidden.
	ro, _ := json.Marshal(actor.Actor{AccountID: f.acct.ID, Username: f.acct.Username, MailboxID: f.mb.ID, Roles: []string{"read"}, Protocol: "jmap"})
	setBody, _ := json.Marshal(map[string]any{"using": []string{CapCore, CapMail}, "methodCalls": []any{[]any{"Email/set", f.acctArgs(map[string]any{"update": map[string]any{d.MessageID.String(): map[string]any{"keywords/$seen": true}}}), "x"}}})
	_, out = f.post("/jmap/api", "application/json", setBody, map[string]string{"X-Test-Actor": string(ro)})
	if !strings.Contains(string(out), `"forbidden"`) {
		t.Fatalf("read-only set: %s", out)
	}
	// Legal hold: session read-only, Mailbox rights false, set forbidden, reads fine.
	_, _ = f.ids.UpdateMailbox(context.Background(), f.mb.ID, func(m *identity.Mailbox) error {
		m.Hold = &identity.Hold{Reason: "case", AcceptDelivery: true}
		return nil
	})
	_, sb := f.get("/.well-known/jmap", nil)
	var sess map[string]any
	_ = json.Unmarshal(sb, &sess)
	if sess["accounts"].(map[string]any)[f.mb.ID.String()].(map[string]any)["isReadOnly"] != true {
		t.Fatal("hold not read-only in session")
	}
	r := f.do(call{"Mailbox/get", f.acctArgs(map[string]any{"ids": []string{f.inbox()}}), "h"})
	rights := first(r, t, "Mailbox/get")["list"].([]any)[0].(map[string]any)["myRights"].(map[string]any)
	if rights["mayAddItems"] != false || rights["mayReadItems"] != true {
		t.Fatalf("rights on hold: %v", rights)
	}
	r = f.do(call{"Email/set", f.acctArgs(map[string]any{"destroy": []string{d.MessageID.String()}}), "h2"})
	nd := first(r, t, "Email/set")["notDestroyed"].(map[string]any)
	if nd[d.MessageID.String()].(map[string]any)["type"] != "forbidden" {
		t.Fatalf("destroy on hold: %v", nd)
	}
	r = f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{d.MessageID.String()}, "properties": []string{"subject"}}), "h3"})
	if first(r, t, "Email/get")["list"].([]any)[0].(map[string]any)["subject"] != "held" {
		t.Fatal("read on hold")
	}
}

func TestEventSource(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.ts.URL+"/jmap/eventsource?types=Email&closeafter=state&ping=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content type: %s", resp.Header.Get("Content-Type"))
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		f.deliver(msg("push", "a@b.c", "push@x", ""))
	}()
	body, err := io.ReadAll(resp.Body) // closes after the state event
	if err != nil && ctx.Err() != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(body), "event: state") || !strings.Contains(string(body), `"StateChange"`) || !strings.Contains(string(body), f.mb.ID.String()) || strings.Contains(string(body), `"Mailbox"`) {
		t.Fatalf("eventsource body: %s", body)
	}
}

func TestJSONPointer(t *testing.T) {
	v := map[string]any{"list": []any{map[string]any{"id": "a", "tags": []any{"x", "y"}}, map[string]any{"id": "b", "tags": []any{"z"}}}, "ids": []any{"1", "2"}}
	got, err := jsonPointer(v, "/list/*/id")
	if err != nil || len(got.([]any)) != 2 || got.([]any)[1] != "b" {
		t.Fatalf("wildcard: %v %v", got, err)
	}
	got, err = jsonPointer(v, "/list/*/tags")
	if err != nil || len(got.([]any)) != 3 {
		t.Fatalf("flatten: %v %v", got, err)
	}
	got, err = jsonPointer(v, "/ids/1")
	if err != nil || got != "2" {
		t.Fatalf("index: %v %v", got, err)
	}
	if _, err := jsonPointer(v, "/nope"); err == nil {
		t.Fatal("missing")
	}
	if _, err := jsonPointer(v, "ids"); err == nil {
		t.Fatal("no leading slash")
	}
}

// rendered returns what Kittiwake serves for a raw message: the document
// rendered back to RFC 5322 (the original bytes are not kept).
func rendered(t *testing.T, raw string) string {
	t.Helper()
	p, err := mail.Parse([]byte(raw), mail.Options{InlineMax: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	out, err := mail.Render(p.Doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
