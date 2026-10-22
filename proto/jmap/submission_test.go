package jmap

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"dovetail/service/actor"
	"dovetail/service/submit"
	"dovetail/store/identity"
	"dovetail/store/index"
)

type fakeRelay struct {
	from string
	to   []string
	data []byte
	err  error
}

func (f *fakeRelay) Send(_ context.Context, from string, to []string, data []byte) error {
	f.from, f.to, f.data = from, to, data
	return f.err
}

// withSubmit reconfigures the fixture's server with a submit service over a
// fake relay and adds an alias so the mailbox has two identities.
func (f *fixture) withSubmit() *fakeRelay {
	f.t.Helper()
	ctx := context.Background()
	if _, err := f.ids.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: f.mb.ID}}}); err != nil {
		f.t.Fatal(err)
	}
	relay := &fakeRelay{}
	sub := submit.New(submit.Config{Hostname: "mx.test"}, f.ids, f.idx, f.del, f.aud, relay, f.srv.log)
	cfg := f.srv.cfg
	cfg.Submit = sub
	f.srv = New(cfg, f.mbsvc, f.del, f.srv.log)
	return relay
}

func (f *fixture) usingSubmission(calls ...call) []invocation {
	f.t.Helper()
	var mc []any
	for _, c := range calls {
		mc = append(mc, []any{c.name, c.args, c.id})
	}
	body, _ := json.Marshal(map[string]any{"using": []string{CapCore, CapMail, CapSubmission}, "methodCalls": mc})
	resp, b := f.post("/jmap/api", "application/json", body, nil)
	if resp.StatusCode != 200 {
		f.t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	var out response
	if err := json.Unmarshal(b, &out); err != nil {
		f.t.Fatal(err)
	}
	return out.MethodResponses
}

// importDraft uploads raw and imports it into Drafts, returning the email id.
func (f *fixture) importDraft(raw string) string {
	f.t.Helper()
	resp, body := f.post("/jmap/upload/"+f.mb.ID.String(), "message/rfc822", []byte(raw), nil)
	if resp.StatusCode != 201 {
		f.t.Fatalf("upload: %d %s", resp.StatusCode, body)
	}
	var up map[string]any
	_ = json.Unmarshal(body, &up)
	drafts := f.folderByRole(index.RoleDrafts)
	r := f.do(call{"Email/import", f.acctArgs(map[string]any{"emails": map[string]any{
		"d": map[string]any{"blobId": up["blobId"], "mailboxIds": map[string]any{drafts: true}, "keywords": map[string]any{"$draft": true}},
	}}), "i"})
	created := first(r, f.t, "Email/import")["created"].(map[string]any)
	return created["d"].(map[string]any)["id"].(string)
}

const draftFromAlias = "From: info@example.org\r\nTo: bob@other.example\r\nSubject: outgoing\r\nMessage-ID: <d1@example.org>\r\n\r\nhello\r\n"

func TestSubmissionNotConfigured(t *testing.T) {
	f := newFixture(t)
	r := f.do(call{"EmailSubmission/set", f.acctArgs(nil), "s"})
	if r[0].name != "error" || r[0].args["type"] != "unknownMethod" {
		t.Fatalf("expected unknownMethod: %v", r[0])
	}
	resp, _ := f.post("/jmap/api", "application/json", []byte(`{"using":["`+CapSubmission+`"],"methodCalls":[]}`), nil)
	if resp.StatusCode != 400 {
		t.Fatalf("submission capability should be unknown: %d", resp.StatusCode)
	}
	_, body := f.get("/.well-known/jmap", nil)
	if strings.Contains(string(body), CapSubmission) {
		t.Fatal("session advertises submission without a submit service")
	}
}

func TestIdentitiesAndSessionWithSubmit(t *testing.T) {
	f := newFixture(t)
	f.withSubmit()
	_, body := f.get("/.well-known/jmap", nil)
	var sess map[string]any
	_ = json.Unmarshal(body, &sess)
	if sess["capabilities"].(map[string]any)[CapSubmission] == nil || sess["primaryAccounts"].(map[string]any)[CapSubmission] != f.mb.ID.String() {
		t.Fatalf("session: %v", sess)
	}
	ac := sess["accounts"].(map[string]any)[f.mb.ID.String()].(map[string]any)["accountCapabilities"].(map[string]any)
	if ac[CapSubmission].(map[string]any)["maxDelayedSend"] != float64(0) {
		t.Fatalf("account capabilities: %v", ac)
	}
	r := f.usingSubmission(call{"Identity/get", f.acctArgs(nil), "id"})
	list := first(r, t, "Identity/get")["list"].([]any)
	if len(list) != 2 || list[0].(map[string]any)["email"] != "wido@example.org" || list[1].(map[string]any)["id"] != "info@example.org" {
		t.Fatalf("identities: %v", list)
	}
	if list[0].(map[string]any)["name"] != "" || list[1].(map[string]any)["name"] != "" || list[1].(map[string]any)["mayDelete"] != false {
		t.Fatalf("identity fields: %v", list)
	}
	// Identity/set may change the name only; it is the account's display
	// name, so every identity shows it afterwards.
	r = f.usingSubmission(call{"Identity/set", f.acctArgs(map[string]any{"update": map[string]any{"info@example.org": map[string]any{"name": "Wido"}}}), "s"})
	if u := first(r, t, "Identity/set")["updated"].(map[string]any); len(u) != 1 {
		t.Fatalf("Identity/set update: %v", r[0].args)
	}
	r = f.usingSubmission(call{"Identity/get", f.acctArgs(nil), "id"})
	list = first(r, t, "Identity/get")["list"].([]any)
	if list[0].(map[string]any)["name"] != "Wido" || list[1].(map[string]any)["name"] != "Wido" {
		t.Fatalf("name after Identity/set: %v", list)
	}
	r = f.usingSubmission(call{"Identity/set", f.acctArgs(map[string]any{"update": map[string]any{"info@example.org": map[string]any{"email": "x@example.org"}}}), "s"})
	if nu := first(r, t, "Identity/set")["notUpdated"].(map[string]any); nu["info@example.org"].(map[string]any)["type"] != "invalidProperties" {
		t.Fatalf("Identity/set email change: %v", r[0].args)
	}
	r = f.usingSubmission(call{"Identity/get", f.acctArgs(map[string]any{"ids": []string{"info@example.org", "nope"}}), "id"})
	ig := first(r, t, "Identity/get")
	if len(ig["list"].([]any)) != 1 || ig["notFound"].([]any)[0] != "nope" {
		t.Fatalf("identity by id: %v", ig)
	}
	r = f.usingSubmission(call{"Identity/set", f.acctArgs(map[string]any{"create": map[string]any{"x": map[string]any{"email": "x@example.org"}}}), "s"})
	if nc := first(r, t, "Identity/set")["notCreated"].(map[string]any); nc["x"].(map[string]any)["type"] != "forbidden" {
		t.Fatalf("Identity/set create: %v", r[0].args)
	}
	r = f.usingSubmission(call{"Identity/changes", f.acctArgs(map[string]any{"sinceState": "0"}), "c"})
	if r[0].name != "error" || r[0].args["type"] != "cannotCalculateChanges" {
		t.Fatalf("Identity/changes: %v", r[0])
	}
}

func TestEmailSubmissionSetSuccess(t *testing.T) {
	f := newFixture(t)
	relay := f.withSubmit()
	emailID := f.importDraft(draftFromAlias)
	sent := f.folderByRole(index.RoleSent)
	drafts := f.folderByRole(index.RoleDrafts)
	r := f.usingSubmission(call{"EmailSubmission/set", f.acctArgs(map[string]any{
		"create": map[string]any{"k1": map[string]any{"identityId": "info@example.org", "emailId": emailID}},
		"onSuccessUpdateEmail": map[string]any{"#k1": map[string]any{
			"mailboxIds": map[string]any{sent: true}, "keywords/$draft": nil, "keywords/$seen": true,
		}},
	}), "s"})
	if len(r) != 2 {
		t.Fatalf("expected EmailSubmission/set + implicit Email/set, got %d: %v", len(r), r)
	}
	es := first(r, t, "EmailSubmission/set")
	created := es["created"].(map[string]any)
	c, ok := created["k1"].(map[string]any)
	if !ok {
		t.Fatalf("not created: %v", es["notCreated"])
	}
	if c["emailId"] != emailID || c["undoStatus"] != "final" || c["identityId"] != "info@example.org" || c["threadId"] == nil || c["sendAt"] == nil {
		t.Fatalf("created: %v", c)
	}
	env := c["envelope"].(map[string]any)
	if env["mailFrom"].(map[string]any)["email"] != "info@example.org" || env["rcptTo"].([]any)[0].(map[string]any)["email"] != "bob@other.example" {
		t.Fatalf("envelope: %v", env)
	}
	if relay.from != "info@example.org" || len(relay.to) != 1 || relay.to[0] != "bob@other.example" || !strings.Contains(string(relay.data), "Subject: outgoing") || !strings.HasPrefix(string(relay.data), "Received: ") {
		t.Fatalf("relay: %s %v %q", relay.from, relay.to, relay.data)
	}
	if r[1].name != "Email/set" || r[1].callID != "s" {
		t.Fatalf("implicit response: %v", r[1])
	}
	if _, ok := r[1].args["updated"].(map[string]any)[emailID]; !ok {
		t.Fatalf("implicit Email/set did not update: %v", r[1].args)
	}
	// The draft now lives in Sent, seen and no longer a draft; no extra copy.
	g := f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{emailID}, "properties": []string{"mailboxIds", "keywords"}}), "g"})
	e := first(g, t, "Email/get")["list"].([]any)[0].(map[string]any)
	kw := e["keywords"].(map[string]any)
	if e["mailboxIds"].(map[string]any)[sent] != true || kw["$seen"] != true || kw["$draft"] != nil {
		t.Fatalf("email after send: %v", e)
	}
	root, _ := f.mbsvc.Root(actor.With(context.Background(), f.act), f.mb.ID)
	if root.FolderByID(sent).Total != 1 || root.FolderByID(drafts).Total != 0 {
		t.Fatalf("counts: sent=%d drafts=%d", root.FolderByID(sent).Total, root.FolderByID(drafts).Total)
	}
	// Submissions are not persisted.
	r = f.usingSubmission(call{"EmailSubmission/get", f.acctArgs(map[string]any{"ids": []string{c["id"].(string)}}), "g"})
	if nf := first(r, t, "EmailSubmission/get")["notFound"].([]any); len(nf) != 1 {
		t.Fatalf("get: %v", r[0].args)
	}
	r = f.usingSubmission(call{"EmailSubmission/query", f.acctArgs(nil), "q"})
	if first(r, t, "EmailSubmission/query")["total"] != float64(0) {
		t.Fatal("query")
	}
}

func TestEmailSubmissionDestroyAndExplicitEnvelope(t *testing.T) {
	f := newFixture(t)
	relay := f.withSubmit()
	emailID := f.importDraft(draftFromAlias)
	r := f.usingSubmission(call{"EmailSubmission/set", f.acctArgs(map[string]any{
		"create": map[string]any{"k1": map[string]any{
			"identityId": "info@example.org", "emailId": emailID,
			"envelope": map[string]any{"mailFrom": map[string]any{"email": "info@example.org"}, "rcptTo": []any{map[string]any{"email": "carol@other.example"}, map[string]any{"email": "dave@other.example"}}},
		}},
		"onSuccessDestroyEmail": []string{"#k1"},
	}), "s"})
	es := first(r, t, "EmailSubmission/set")
	if _, ok := es["created"].(map[string]any)["k1"]; !ok {
		t.Fatalf("not created: %v", es["notCreated"])
	}
	if len(relay.to) != 2 || relay.to[1] != "dave@other.example" {
		t.Fatalf("explicit rcptTo: %v", relay.to)
	}
	if len(r) != 2 || r[1].name != "Email/set" || r[1].args["destroyed"].([]any)[0] != emailID {
		t.Fatalf("implicit destroy: %v", r)
	}
	g := f.do(call{"Email/get", f.acctArgs(map[string]any{"ids": []string{emailID}}), "g"})
	if nf := first(g, t, "Email/get")["notFound"].([]any); len(nf) != 1 {
		t.Fatal("email should be destroyed")
	}
}

func TestEmailSubmissionSetErrors(t *testing.T) {
	f := newFixture(t)
	relay := f.withSubmit()
	emailID := f.importDraft(draftFromAlias)
	ceoDraft := strings.Replace(draftFromAlias, "From: info@example.org", "From: ceo@example.org", 1)
	badFrom := f.importDraft(ceoDraft)
	noRcpt := f.importDraft("From: info@example.org\r\nSubject: none\r\n\r\nx\r\n")
	r := f.usingSubmission(call{"EmailSubmission/set", f.acctArgs(map[string]any{
		"create": map[string]any{
			"bad-ident": map[string]any{"identityId": "nope@example.org", "emailId": emailID},
			"bad-from":  map[string]any{"identityId": "wido@example.org", "emailId": badFrom},
			"no-rcpt":   map[string]any{"identityId": "info@example.org", "emailId": noRcpt},
			"no-email":  map[string]any{"identityId": "info@example.org", "emailId": "00000000-0000-0000-0000-000000000001"},
			"spoof-env": map[string]any{"identityId": "info@example.org", "emailId": emailID, "envelope": map[string]any{"mailFrom": map[string]any{"email": "spoof@example.org"}}},
		},
		"update":  map[string]any{"x": map[string]any{}},
		"destroy": []string{"y"},
	}), "s"})
	es := first(r, t, "EmailSubmission/set")
	if len(es["created"].(map[string]any)) != 0 {
		t.Fatalf("unexpected creations: %v", es["created"])
	}
	nc := es["notCreated"].(map[string]any)
	typ := func(k string) string { return nc[k].(map[string]any)["type"].(string) }
	if typ("bad-ident") != "invalidProperties" || typ("bad-from") != "invalidProperties" || typ("no-rcpt") != "invalidProperties" || typ("no-email") != "invalidProperties" || typ("spoof-env") != "invalidProperties" {
		t.Fatalf("notCreated: %v", nc)
	}
	if nc["bad-ident"].(map[string]any)["properties"].([]any)[0] != "identityId" {
		t.Fatalf("bad-ident properties: %v", nc["bad-ident"])
	}
	if es["notUpdated"].(map[string]any)["x"] == nil || es["notDestroyed"].(map[string]any)["y"] == nil {
		t.Fatalf("update/destroy must be refused: %v", es)
	}
	if relay.data != nil {
		t.Fatal("relay should not have been used")
	}
	if len(r) != 1 {
		t.Fatalf("no implicit Email/set expected on failure: %d", len(r))
	}
	// Relay failures: permanent -> invalidEmail, transient -> serverFail.
	relay.err = errTemp("452 try again later")
	r = f.usingSubmission(call{"EmailSubmission/set", f.acctArgs(map[string]any{"create": map[string]any{"k": map[string]any{"identityId": "info@example.org", "emailId": emailID}}}), "s"})
	if typ := first(r, t, "EmailSubmission/set")["notCreated"].(map[string]any)["k"].(map[string]any)["type"]; typ != "serverFail" {
		t.Fatalf("transient relay error: %v", typ)
	}
	relay.err = errTemp("550 5.1.1 no such user")
	r = f.usingSubmission(call{"EmailSubmission/set", f.acctArgs(map[string]any{"create": map[string]any{"k": map[string]any{"identityId": "info@example.org", "emailId": emailID}}}), "s"})
	if typ := first(r, t, "EmailSubmission/set")["notCreated"].(map[string]any)["k"].(map[string]any)["type"]; typ != "invalidEmail" {
		t.Fatalf("permanent relay error: %v", typ)
	}
	relay.err = nil
	// Legal hold: forbidden.
	_, _ = f.ids.UpdateMailbox(context.Background(), f.mb.ID, func(m *identity.Mailbox) error {
		m.Hold = &identity.Hold{Reason: "r", AcceptDelivery: true}
		return nil
	})
	r = f.usingSubmission(call{"EmailSubmission/set", f.acctArgs(map[string]any{"create": map[string]any{"k": map[string]any{"identityId": "info@example.org", "emailId": emailID}}}), "s"})
	if typ := first(r, t, "EmailSubmission/set")["notCreated"].(map[string]any)["k"].(map[string]any)["type"]; typ != "forbidden" {
		t.Fatalf("hold: %v", typ)
	}
}

type errTemp string

func (e errTemp) Error() string { return string(e) }
