package admin

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cryptoauth "dovetail/auth"
	"dovetail/proto/rest"
	"dovetail/rados"
	"dovetail/service/actor"
	adminsvc "dovetail/service/admin"
	"dovetail/service/auditor"
	"dovetail/service/auth"
	"dovetail/service/delivery"
	"dovetail/service/export"
	"dovetail/store/attach"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
	"dovetail/store/message"
	"dovetail/store/queue"
	"dovetail/store/session"
	"dovetail/worker"
)

type fixture struct {
	t       *testing.T
	c       *Client
	ts      *httptest.Server
	ids     *identity.Store
	idx     *index.Store
	del     *delivery.Service
	w       *worker.Worker
	mb      identity.Mailbox
	user    identity.Account
	userTk  string
	adminTk string
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	f := &fixture{t: t, now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	now := func() time.Time { return f.now }
	f.ids = identity.New(meta)
	f.ids.Now = now
	sess := session.New(idxp)
	sess.Now = now
	auditStore := audit.New(idxp)
	auditStore.Now = now
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(auditStore, log, nil)
	aud.Now = now
	authSvc := auth.New(f.ids, sess, aud, auth.Config{AccessTokenLifetime: 15 * time.Minute, RefreshTokenLifetime: time.Hour, Issuer: "test"})
	authSvc.Now = now
	q := queue.New(idxp)
	f.idx = index.New(idxp)
	adm := adminsvc.New(f.ids, f.idx, q, aud)
	adm.Now = now
	msgs, atts := message.New(msgp), attach.New(attp)
	f.del = delivery.New(f.ids, f.idx, msgs, atts, q, aud, log, delivery.Config{})
	exp := export.New(f.ids, f.idx, msgs, atts, auditStore, f.del, aud, log)
	dir := t.TempDir()
	srv := rest.New(rest.Config{Realm: "test", DefaultQuota: identity.QuotaLimits{HardBytes: 1 << 30}, Identity: f.ids, Export: exp, ExportDir: dir}, authSvc, adm, auditStore, q, log)
	f.ts = httptest.NewServer(srv.Handler())
	t.Cleanup(f.ts.Close)
	f.w = worker.New(worker.Config{ExportDir: dir, PollInterval: time.Millisecond}, worker.Stores{Identity: f.ids, Index: f.idx, Messages: msgs, Attachments: atts, Audit: auditStore, Queue: q}, aud, idxp, log)
	f.w.Export = exp

	ctx := context.Background()
	_, _ = f.ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	f.mb, _ = f.ids.CreateMailbox(ctx, identity.Mailbox{Enabled: true})
	h, _ := cryptoauth.HashPassword("correct horse")
	f.user, _ = f.ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: f.mb.ID, Enabled: true, Roles: []string{"read", "write", "send"}, Passwords: []identity.Password{{Hash: h, Kind: identity.PasswordPrimary}}})
	_, f.userTk, _ = f.ids.CreateToken(ctx, f.user.ID, nil, f.now.Add(24*time.Hour), "user token")
	admin, _ := f.ids.CreateAccount(ctx, identity.Account{Username: "admin@example.org", Roles: []string{"admin"}, Enabled: true})
	_, f.adminTk, _ = f.ids.CreateToken(ctx, admin.ID, nil, f.now.Add(24*time.Hour), "test admin")
	f.c = New(f.ts.URL, f.adminTk, WithTimeout(10*time.Second), WithUserAgent("test"))
	return f
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStatusAndErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	st, err := f.c.Status(ctx)
	must(t, err)
	if st.Version == "" || st.Time.IsZero() {
		t.Fatalf("status: %+v", st)
	}
	if _, err := New(f.ts.URL, "dvt_bogus").Status(ctx); !IsUnauthorized(err) {
		t.Fatalf("bad token: %v", err)
	}
	if _, err := New(f.ts.URL, f.userTk).Status(ctx); !IsForbidden(err) {
		t.Fatalf("non-admin: %v", err)
	}
	if _, err := f.c.GetDomain(ctx, "nope.example"); !IsNotFound(err) {
		t.Fatalf("not found: %v", err)
	}
	var ae *APIError
	err = f.c.DeleteDomain(ctx, "nope.example")
	if !errorsAs(err, &ae) || ae.Status != 404 || !strings.HasPrefix(err.Error(), "404 ") {
		t.Fatalf("api error shape: %v", err)
	}
	if _, err := New("http://127.0.0.1:1", f.adminTk, WithTimeout(time.Second)).Status(ctx); err == nil || IsNotFound(err) {
		t.Fatalf("network error expected, got %v", err)
	}
}

func errorsAs(err error, target **APIError) bool {
	for err != nil {
		if ae, ok := err.(*APIError); ok {
			*target = ae
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestDomainsAndPagination(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	d, err := f.c.CreateDomain(ctx, DomainRequest{Name: "A.Example", Settings: map[string]string{"k": "v"}})
	must(t, err)
	if d.Name != "a.example" || !d.Enabled || d.Settings["k"] != "v" {
		t.Fatalf("create: %+v", d)
	}
	if _, err := f.c.CreateDomain(ctx, DomainRequest{Name: "a.example"}); !IsConflict(err) {
		t.Fatalf("conflict: %v", err)
	}
	off := false
	d, err = f.c.UpdateDomain(ctx, "a.example", DomainRequest{Enabled: &off})
	must(t, err)
	if d.Enabled {
		t.Fatal("update")
	}
	for _, n := range []string{"b.example", "c.example"} {
		_, err := f.c.CreateDomain(ctx, DomainRequest{Name: n})
		must(t, err)
	}
	p, err := f.c.ListDomains(ctx, "", 2)
	must(t, err)
	if len(p.Items) != 2 || !p.More || p.Next != "b.example" {
		t.Fatalf("page: %+v", p)
	}
	all, err := f.c.ListDomainsAll(ctx)
	must(t, err)
	if len(all) != 4 || all[3] != "example.org" {
		t.Fatalf("all: %v", all)
	}
	must(t, f.c.DeleteDomain(ctx, "c.example"))
	if _, err := f.c.GetDomain(ctx, "c.example"); !IsNotFound(err) {
		t.Fatal("not deleted")
	}
	if err := f.c.DeleteDomain(ctx, "example.org"); !IsConflict(err) {
		t.Fatalf("delete in-use domain: %v", err)
	}
}

func TestMailboxesAccountsAliasesTokens(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	mb, err := f.c.CreateMailbox(ctx, MailboxRequest{Quota: &QuotaLimits{HardBytes: 5}})
	must(t, err)
	if mb.ID == "" || !mb.Enabled || mb.Quota.HardBytes != 5 {
		t.Fatalf("mailbox: %+v", mb)
	}
	list, err := f.c.ListMailboxes(ctx)
	must(t, err)
	if len(list) != 2 {
		t.Fatalf("list: %+v", list)
	}
	off := false
	mb, err = f.c.UpdateMailbox(ctx, mb.ID, MailboxRequest{Enabled: &off, Quota: &QuotaLimits{HardMessages: 7}})
	must(t, err)
	if mb.Enabled || mb.Quota.HardMessages != 7 {
		t.Fatalf("patch: %+v", mb)
	}
	acct, err := f.c.CreateAccount(ctx, AccountRequest{Username: "bob@example.org", MailboxID: mb.ID, Roles: []string{"read", "write"}, Password: "longenough1"})
	must(t, err)
	if acct.Username != "bob@example.org" || len(acct.Passwords) != 1 || acct.Passwords[0].Kind != "primary" || acct.TOTPEnabled {
		t.Fatalf("account: %+v", acct)
	}
	if _, err := f.c.CreateAccount(ctx, AccountRequest{Username: "x@example.org", MailboxID: mb.ID, Password: "short"}); !IsBadRequest(err) {
		t.Fatalf("short pw: %v", err)
	}
	info, err := f.c.InspectMailbox(ctx, mb.ID)
	must(t, err)
	if len(info.Accounts) != 1 || info.Accounts[0] != "bob@example.org" || info.Limits.HardBytes != 1<<30 || info.Limits.HardMessages != 7 || info.Indexed {
		t.Fatalf("inspect: %+v", info)
	}
	got, err := f.c.GetAccount(ctx, "bob@example.org")
	must(t, err)
	if got.ID != acct.ID {
		t.Fatal("get by username")
	}
	on := true
	got, err = f.c.UpdateAccount(ctx, acct.ID, AccountRequest{Roles: []string{"read"}, Enabled: &on, MailboxID: f.mb.ID.String()})
	must(t, err)
	if len(got.Roles) != 1 || got.MailboxID != f.mb.ID.String() {
		t.Fatalf("patch account: %+v", got)
	}
	ps, err := f.c.SetPassword(ctx, acct.ID, PasswordRequest{Password: "app-pw-123456", Kind: "application", Comment: "phone"})
	must(t, err)
	if ps.Kind != "application" || ps.ID == "" {
		t.Fatalf("password: %+v", ps)
	}
	must(t, f.c.DisableTOTP(ctx, acct.ID))
	sessions, err := f.c.ListSessions(ctx, acct.ID)
	must(t, err)
	if len(sessions) != 0 {
		t.Fatal("sessions")
	}
	tk, err := f.c.CreateToken(ctx, acct.ID, TokenRequest{Roles: []string{"read"}, ExpiresAt: f.now.Add(time.Hour), Comment: "ci"})
	must(t, err)
	if !strings.HasPrefix(tk.Secret, "dvt_") || tk.Token.Comment != "ci" || tk.Token.AccountID != acct.ID {
		t.Fatalf("token: %+v", tk)
	}
	if _, err := f.c.CreateToken(ctx, acct.ID, TokenRequest{ExpiresAt: f.now.Add(time.Hour)}); !IsBadRequest(err) {
		t.Fatalf("token without comment: %v", err)
	}
	toks, err := f.c.ListTokens(ctx, acct.ID)
	must(t, err)
	if len(toks) != 1 {
		t.Fatal("tokens")
	}
	must(t, f.c.DeleteToken(ctx, acct.ID, tk.Token.ID))
	if err := f.c.DeleteToken(ctx, acct.ID, tk.Token.ID); !IsNotFound(err) {
		t.Fatalf("double delete: %v", err)
	}
	// Aliases.
	al, err := f.c.CreateAlias(ctx, AliasRequest{Address: "Info@example.org", Destinations: []Destination{{MailboxID: f.mb.ID.String()}, {Address: "bob@example.org"}}})
	must(t, err)
	if al.Address != "info@example.org" || len(al.Destinations) != 2 || !al.Enabled {
		t.Fatalf("alias: %+v", al)
	}
	_, err = f.c.CreateAlias(ctx, AliasRequest{Address: "@example.org", Destinations: []Destination{{MailboxID: f.mb.ID.String()}}})
	must(t, err)
	al, err = f.c.UpdateAlias(ctx, "info@example.org", AliasRequest{Enabled: &off})
	must(t, err)
	if al.Enabled {
		t.Fatal("alias patch")
	}
	names, err := f.c.ListAliasesAll(ctx, "example.org")
	must(t, err)
	if len(names) != 2 {
		t.Fatalf("aliases: %v", names)
	}
	must(t, f.c.DeleteAlias(ctx, "info@example.org"))
	// Accounts pagination.
	for _, u := range []string{"c1", "c2", "c3"} {
		_, err := f.c.CreateAccount(ctx, AccountRequest{Username: u + "@example.org", MailboxID: f.mb.ID.String(), Roles: []string{"read"}})
		must(t, err)
	}
	p, err := f.c.ListAccounts(ctx, "example.org", "c", "", 2)
	must(t, err)
	if len(p.Items) != 2 || !p.More {
		t.Fatalf("accounts page: %+v", p)
	}
	all, err := f.c.ListAccountsAll(ctx, "example.org", "c")
	must(t, err)
	if len(all) != 3 {
		t.Fatalf("accounts all: %v", all)
	}
	all, _ = f.c.ListAccountsAll(ctx, "example.org", "")
	if len(all) != 6 {
		t.Fatalf("all accounts: %v", all)
	}
	must(t, f.c.DeleteAccount(ctx, "bob@example.org"))
	if _, err := f.c.GetAccount(ctx, "bob@example.org"); !IsNotFound(err) {
		t.Fatal("account not deleted")
	}
	// Delete mailbox: accounts gone, 202.
	must(t, f.c.DeleteMailbox(ctx, mb.ID))
	st, err := f.c.QueueStats(ctx, "remove-mailbox")
	must(t, err)
	if st.Pending != 1 {
		t.Fatalf("removal not scheduled: %+v", st)
	}
}

func TestHoldAuditQueuesKeysIdentity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	no := false
	mb, err := f.c.HoldMailbox(ctx, f.mb.ID.String(), HoldRequest{Reason: "subpoena", CaseRef: "42", AcceptDelivery: &no, Sealed: true})
	must(t, err)
	if mb.Hold == nil || mb.Hold.AcceptDelivery || !mb.Hold.Sealed || mb.Hold.SetBy != "admin@example.org" {
		t.Fatalf("hold: %+v", mb.Hold)
	}
	if _, err := f.c.HoldMailbox(ctx, f.mb.ID.String(), HoldRequest{Reason: "again"}); !IsConflict(err) {
		t.Fatalf("double hold: %v", err)
	}
	holds, err := f.c.ListHolds(ctx)
	must(t, err)
	if len(holds) != 1 {
		t.Fatal("holds")
	}
	mb, err = f.c.ReleaseMailbox(ctx, f.mb.ID.String(), "closed")
	must(t, err)
	if mb.Hold != nil || mb.Released == nil {
		t.Fatal("release")
	}
	u, err := f.c.RecalculateMailbox(ctx, f.mb.ID.String())
	if err != nil && !IsNotFound(err) { // unindexed mailbox → 404 is the server's behaviour
		t.Fatal(err)
	}
	_ = u
	// Audit (admin actions land in the mailbox log and the admin log).
	page, err := f.c.Audit(ctx, AuditQuery{Mailbox: f.mb.ID.String(), From: f.now, To: f.now, Limit: 1})
	must(t, err)
	if len(page.Items) != 1 || page.Next == "" {
		t.Fatalf("audit page: %+v", page)
	}
	all, err := f.c.AuditAll(ctx, AuditQuery{Mailbox: f.mb.ID.String(), From: f.now, To: f.now, Limit: 1})
	must(t, err)
	actions := map[string]bool{}
	for _, r := range all {
		actions[r.Action] = true
	}
	if !actions["hold"] || !actions["release"] || all[0].Admin == "" && all[0].Account == "" {
		t.Fatalf("audit all: %+v", all)
	}
	if _, err := f.c.Audit(ctx, AuditQuery{Mailbox: "not-a-uuid"}); !IsBadRequest(err) {
		t.Fatalf("bad mailbox: %v", err)
	}
	// Queues.
	st, err := f.c.QueueStats(ctx, "purge-trash")
	must(t, err)
	if st.Type != "purge-trash" || st.Pending != 0 {
		t.Fatalf("stats: %+v", st)
	}
	dead, err := f.c.QueueDead(ctx, "purge-trash")
	must(t, err)
	if len(dead) != 0 {
		t.Fatal("dead")
	}
	if err := f.c.QueueRetry(ctx, "purge-trash", "nope"); !IsNotFound(err) {
		t.Fatalf("retry: %v", err)
	}
	must(t, f.c.RotateKeys(ctx))
	// Identity export/import.
	dump, err := f.c.ExportIdentity(ctx)
	must(t, err)
	if !bytes.Contains(dump, []byte(`"domains"`)) || !bytes.Contains(dump, []byte("example.org")) {
		t.Fatalf("dump: %s", dump[:min(len(dump), 80)])
	}
	summary, err := f.c.ImportIdentity(ctx, dump)
	must(t, err)
	if !strings.HasPrefix(summary, "created=0 skipped=") {
		t.Fatalf("import: %s", summary)
	}
	if _, err := f.c.ImportIdentity(ctx, []byte("{bad")); !IsBadRequest(err) {
		t.Fatalf("bad import: %v", err)
	}
}

func TestExportDownloadImport(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	raw := "From: a@b\r\nTo: wido@example.org\r\nSubject: s\r\n\r\nx\r\n"
	if _, err := f.del.Deliver(ctx, delivery.Request{Recipient: "wido@example.org", Raw: []byte(raw)}); err != nil {
		t.Fatal(err)
	}
	sched, err := f.c.ScheduleExport(ctx, f.mb.ID.String(), false)
	must(t, err)
	if sched.TaskID == "" || sched.Gzip {
		t.Fatalf("schedule: %+v", sched)
	}
	f.w.RunOnce(actor.With(ctx, actor.System("cli")))
	items, err := f.c.ListExports(ctx, f.mb.ID.String())
	must(t, err)
	if len(items) != 1 || items[0].Result == nil || items[0].Result.Messages != 1 || !strings.HasSuffix(items[0].Name, ".tar") {
		t.Fatalf("exports: %+v", items)
	}
	rc, err := f.c.DownloadExport(ctx, items[0].Name)
	must(t, err)
	archive, err := io.ReadAll(rc)
	rc.Close()
	must(t, err)
	if int64(len(archive)) != items[0].Bytes {
		t.Fatalf("download size %d want %d", len(archive), items[0].Bytes)
	}
	if _, err := f.c.DownloadExport(ctx, "../etc/passwd"); !IsNotFound(err) {
		t.Fatalf("traversal: %v", err)
	}
	mb2, err := f.c.CreateMailbox(ctx, MailboxRequest{})
	must(t, err)
	sum, err := f.c.ImportMailbox(ctx, mb2.ID, bytes.NewReader(archive), false)
	must(t, err)
	if sum.Messages != 1 || sum.Folders == 0 {
		t.Fatalf("import: %+v", sum)
	}
	info, err := f.c.InspectMailbox(ctx, mb2.ID)
	must(t, err)
	if info.Usage.Messages != 1 || !info.Indexed {
		t.Fatalf("after import: %+v", info)
	}
	if _, err := f.c.OpenAPI(ctx); !IsNotFound(err) && err != nil {
		t.Fatalf("openapi (not served by the test mux): %v", err)
	}
}
