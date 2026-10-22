package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to the Dovetail admin API with a long-lived admin API token.
type Client struct {
	base      string
	token     string
	http      *http.Client
	userAgent string
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the underlying HTTP client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithTimeout sets the per-request timeout (default 60s). Ignored when
// WithHTTPClient is also given.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if c.http == nil || c.http == http.DefaultClient {
			c.http = &http.Client{Timeout: d}
		} else {
			c.http.Timeout = d
		}
	}
}

// WithUserAgent sets the User-Agent header.
func WithUserAgent(ua string) Option { return func(c *Client) { c.userAgent = ua } }

// New returns a client for the admin listener at baseURL (e.g.
// "http://[::1]:8081") authenticating with token.
func New(baseURL, token string, opts ...Option) *Client {
	c := &Client{base: strings.TrimRight(baseURL, "/"), token: token, http: &http.Client{Timeout: 60 * time.Second}, userAgent: "dovetail-admin-client"}
	for _, o := range opts {
		o(c)
	}
	return c
}

// BaseURL returns the configured base URL.
func (c *Client) BaseURL() string { return c.base }

// APIError is a problem+json error response.
type APIError struct {
	Status int    `json:"status"`
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func (e *APIError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%d %s", e.Status, e.Detail)
	}
	return fmt.Sprintf("%d %s", e.Status, e.Title)
}

func isStatus(err error, code int) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == code
}

// IsNotFound reports a 404.
func IsNotFound(err error) bool { return isStatus(err, http.StatusNotFound) }

// IsConflict reports a 409.
func IsConflict(err error) bool { return isStatus(err, http.StatusConflict) }

// IsForbidden reports a 403.
func IsForbidden(err error) bool { return isStatus(err, http.StatusForbidden) }

// IsUnauthorized reports a 401.
func IsUnauthorized(err error) bool { return isStatus(err, http.StatusUnauthorized) }

// IsBadRequest reports a 400.
func IsBadRequest(err error) bool { return isStatus(err, http.StatusBadRequest) }

// do performs a request. body may be nil, an io.Reader (sent as-is with
// contentType) or any value (JSON encoded). out, when non-nil, receives
// the decoded JSON response. Returns the response for callers that need
// headers; its body is already closed unless out is an *io.ReadCloser.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, contentType string, out any) error {
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case io.Reader:
		rdr = b
	default:
		j, err := json.Marshal(b)
		if err != nil {
			return fmt.Errorf("admin client: encode: %w", err)
		}
		rdr = bytes.NewReader(j)
		if contentType == "" {
			contentType = "application/json"
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("admin client: %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return readError(resp)
	}
	if rc, ok := out.(*io.ReadCloser); ok {
		*rc = resp.Body
		return nil
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if raw, ok := out.(*[]byte); ok {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("admin client: read: %w", err)
		}
		*raw = b
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("admin client: decode %s %s: %w", method, path, err)
	}
	return nil
}

func readError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	ae := &APIError{Status: resp.StatusCode, Title: http.StatusText(resp.StatusCode)}
	if strings.Contains(resp.Header.Get("Content-Type"), "json") {
		_ = json.Unmarshal(b, ae)
		ae.Status = resp.StatusCode
	}
	if ae.Detail == "" && len(b) > 0 && !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		ae.Detail = strings.TrimSpace(string(b))
	}
	return ae
}

func pathEscape(s string) string { return url.PathEscape(s) }

func pageQuery(after string, limit int) url.Values {
	q := url.Values{}
	if after != "" {
		q.Set("after", after)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	return q
}

// ---- status / spec ----

// Status returns version and server time.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var out Status
	return out, c.do(ctx, http.MethodGet, "/v1/admin/status", nil, nil, "", &out)
}

// OpenAPI fetches the OpenAPI spec from the admin listener. It tries the
// authenticated /v1/admin/openapi.yaml first and falls back to the public
// /openapi.yaml.
func (c *Client) OpenAPI(ctx context.Context) ([]byte, error) {
	var out []byte
	err := c.do(ctx, http.MethodGet, "/v1/admin/openapi.yaml", nil, nil, "", &out)
	if err == nil {
		return out, nil
	}
	if !IsNotFound(err) {
		return nil, err
	}
	return out, c.do(ctx, http.MethodGet, "/openapi.yaml", nil, nil, "", &out)
}

// ---- domains ----

// CreateDomain creates a domain (enabled unless req.Enabled is false).
func (c *Client) CreateDomain(ctx context.Context, req DomainRequest) (Domain, error) {
	var out Domain
	return out, c.do(ctx, http.MethodPost, "/v1/admin/domains", nil, req, "", &out)
}

// ListDomains returns one page of domain names.
func (c *Client) ListDomains(ctx context.Context, after string, limit int) (Page, error) {
	var out Page
	return out, c.do(ctx, http.MethodGet, "/v1/admin/domains", pageQuery(after, limit), nil, "", &out)
}

// ListDomainsAll follows pages and returns every domain name.
func (c *Client) ListDomainsAll(ctx context.Context) ([]string, error) {
	return c.collect(ctx, func(after string) (Page, error) { return c.ListDomains(ctx, after, 500) })
}

// GetDomain returns a domain.
func (c *Client) GetDomain(ctx context.Context, name string) (Domain, error) {
	var out Domain
	return out, c.do(ctx, http.MethodGet, "/v1/admin/domains/"+pathEscape(name), nil, nil, "", &out)
}

// UpdateDomain patches enabled/settings.
func (c *Client) UpdateDomain(ctx context.Context, name string, req DomainRequest) (Domain, error) {
	var out Domain
	req.Name = ""
	return out, c.do(ctx, http.MethodPatch, "/v1/admin/domains/"+pathEscape(name), nil, req, "", &out)
}

// DeleteDomain deletes an empty domain.
func (c *Client) DeleteDomain(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/domains/"+pathEscape(name), nil, nil, "", nil)
}

func (c *Client) collect(ctx context.Context, page func(after string) (Page, error)) ([]string, error) {
	var out []string
	after := ""
	for {
		p, err := page(after)
		if err != nil {
			return out, err
		}
		out = append(out, p.Items...)
		if !p.More || len(p.Items) == 0 {
			return out, nil
		}
		after = p.Next
		if after == "" {
			after = p.Items[len(p.Items)-1]
		}
		if err := ctx.Err(); err != nil {
			return out, err
		}
	}
}

// ---- mailboxes ----

// CreateMailbox creates a mailbox.
func (c *Client) CreateMailbox(ctx context.Context, req MailboxRequest) (Mailbox, error) {
	var out Mailbox
	return out, c.do(ctx, http.MethodPost, "/v1/admin/mailboxes", nil, req, "", &out)
}

// ListMailboxes lists every mailbox (walks the meta pool; operator tool).
func (c *Client) ListMailboxes(ctx context.Context) ([]MailboxListItem, error) {
	var out struct {
		Items []MailboxListItem `json:"items"`
	}
	return out.Items, c.do(ctx, http.MethodGet, "/v1/admin/mailboxes", nil, nil, "", &out)
}

// InspectMailbox returns usage, folders, accounts, hold and limits.
func (c *Client) InspectMailbox(ctx context.Context, id string) (MailboxInfo, error) {
	var out MailboxInfo
	return out, c.do(ctx, http.MethodGet, "/v1/admin/mailboxes/"+pathEscape(id), nil, nil, "", &out)
}

// UpdateMailbox patches enabled/quota.
func (c *Client) UpdateMailbox(ctx context.Context, id string, req MailboxRequest) (Mailbox, error) {
	var out Mailbox
	return out, c.do(ctx, http.MethodPatch, "/v1/admin/mailboxes/"+pathEscape(id), nil, req, "", &out)
}

// DeleteMailbox removes the mailbox record and schedules data removal (202).
func (c *Client) DeleteMailbox(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/mailboxes/"+pathEscape(id), nil, nil, "", nil)
}

// RecalculateMailbox rebuilds the usage counters.
func (c *Client) RecalculateMailbox(ctx context.Context, id string) (Usage, error) {
	var out RecalculateResult
	return out.Usage, c.do(ctx, http.MethodPost, "/v1/admin/mailboxes/"+pathEscape(id)+"/recalculate", nil, nil, "", &out)
}

// HoldMailbox places a mailbox on legal hold.
func (c *Client) HoldMailbox(ctx context.Context, id string, req HoldRequest) (Mailbox, error) {
	var out Mailbox
	return out, c.do(ctx, http.MethodPost, "/v1/admin/mailboxes/"+pathEscape(id)+"/hold", nil, req, "", &out)
}

// ReleaseMailbox lifts a hold.
func (c *Client) ReleaseMailbox(ctx context.Context, id, note string) (Mailbox, error) {
	var out Mailbox
	return out, c.do(ctx, http.MethodDelete, "/v1/admin/mailboxes/"+pathEscape(id)+"/hold", nil, map[string]string{"note": note}, "", &out)
}

// ListHolds lists mailboxes on hold.
func (c *Client) ListHolds(ctx context.Context) ([]Mailbox, error) {
	var out struct {
		Items []Mailbox `json:"items"`
	}
	return out.Items, c.do(ctx, http.MethodGet, "/v1/admin/holds", nil, nil, "", &out)
}

// ScheduleExport asks a worker to export the mailbox as a Maildir archive.
func (c *Client) ScheduleExport(ctx context.Context, id string, gzip bool) (ExportScheduled, error) {
	var out ExportScheduled
	return out, c.do(ctx, http.MethodPost, "/v1/admin/mailboxes/"+pathEscape(id)+"/export", nil, map[string]bool{"gzip": gzip}, "", &out)
}

// ImportMailbox uploads a Maildir archive (tar or tar.gz) into the mailbox.
func (c *Client) ImportMailbox(ctx context.Context, id string, archive io.Reader, gzipped bool) (ImportSummary, error) {
	ct := "application/x-tar"
	if gzipped {
		ct = "application/gzip"
	}
	var out ImportSummary
	return out, c.do(ctx, http.MethodPost, "/v1/admin/mailboxes/"+pathEscape(id)+"/import", nil, archive, ct, &out)
}

// ListExports lists finished exports, optionally for one mailbox.
func (c *Client) ListExports(ctx context.Context, mailbox string) ([]ExportItem, error) {
	q := url.Values{}
	if mailbox != "" {
		q.Set("mailbox", mailbox)
	}
	var out struct {
		Items []ExportItem `json:"items"`
	}
	return out.Items, c.do(ctx, http.MethodGet, "/v1/admin/exports", q, nil, "", &out)
}

// DownloadExport streams an export archive. The caller closes the reader.
func (c *Client) DownloadExport(ctx context.Context, name string) (io.ReadCloser, error) {
	var rc io.ReadCloser
	return rc, c.do(ctx, http.MethodGet, "/v1/admin/exports/"+pathEscape(name), nil, nil, "", &rc)
}

// ---- accounts ----

// CreateAccount creates an account.
func (c *Client) CreateAccount(ctx context.Context, req AccountRequest) (Account, error) {
	var out Account
	return out, c.do(ctx, http.MethodPost, "/v1/admin/accounts", nil, req, "", &out)
}

// ListAccounts returns one page of usernames in a domain.
func (c *Client) ListAccounts(ctx context.Context, domain, prefix, after string, limit int) (Page, error) {
	q := pageQuery(after, limit)
	q.Set("domain", domain)
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	var out Page
	return out, c.do(ctx, http.MethodGet, "/v1/admin/accounts", q, nil, "", &out)
}

// ListAccountsAll follows pages and returns every username in a domain.
func (c *Client) ListAccountsAll(ctx context.Context, domain, prefix string) ([]string, error) {
	return c.collect(ctx, func(after string) (Page, error) { return c.ListAccounts(ctx, domain, prefix, after, 500) })
}

// GetAccount returns an account by uuid or username.
func (c *Client) GetAccount(ctx context.Context, idOrUsername string) (Account, error) {
	var out Account
	return out, c.do(ctx, http.MethodGet, "/v1/admin/accounts/"+pathEscape(idOrUsername), nil, nil, "", &out)
}

// UpdateAccount patches roles, enabled and/or mailbox.
func (c *Client) UpdateAccount(ctx context.Context, idOrUsername string, req AccountRequest) (Account, error) {
	var out Account
	req.Username, req.Password = "", ""
	return out, c.do(ctx, http.MethodPatch, "/v1/admin/accounts/"+pathEscape(idOrUsername), nil, req, "", &out)
}

// DeleteAccount deletes an account and its tokens.
func (c *Client) DeleteAccount(ctx context.Context, idOrUsername string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/accounts/"+pathEscape(idOrUsername), nil, nil, "", nil)
}

// SetPassword sets a primary or application password.
func (c *Client) SetPassword(ctx context.Context, idOrUsername string, req PasswordRequest) (PasswordSet, error) {
	var out PasswordSet
	return out, c.do(ctx, http.MethodPut, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/password", nil, req, "", &out)
}

// DisableTOTP removes TOTP from an account.
func (c *Client) DisableTOTP(ctx context.Context, idOrUsername string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/totp", nil, nil, "", nil)
}

// ListSessions lists an account's live sessions.
func (c *Client) ListSessions(ctx context.Context, idOrUsername string) ([]Session, error) {
	var out struct {
		Items []Session `json:"items"`
	}
	return out.Items, c.do(ctx, http.MethodGet, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/sessions", nil, nil, "", &out)
}

// RevokeSession ends a session.
func (c *Client) RevokeSession(ctx context.Context, idOrUsername, sessionID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/sessions/"+pathEscape(sessionID), nil, nil, "", nil)
}

// CreateToken creates an API token; the secret is returned once.
func (c *Client) CreateToken(ctx context.Context, idOrUsername string, req TokenRequest) (TokenCreated, error) {
	var out TokenCreated
	return out, c.do(ctx, http.MethodPost, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/tokens", nil, req, "", &out)
}

// ListTokens lists an account's tokens.
func (c *Client) ListTokens(ctx context.Context, idOrUsername string) ([]Token, error) {
	var out struct {
		Items []Token `json:"items"`
	}
	return out.Items, c.do(ctx, http.MethodGet, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/tokens", nil, nil, "", &out)
}

// DeleteToken revokes a token.
func (c *Client) DeleteToken(ctx context.Context, idOrUsername, tokenID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/accounts/"+pathEscape(idOrUsername)+"/tokens/"+pathEscape(tokenID), nil, nil, "", nil)
}

// ---- aliases ----

// CreateAlias creates an alias ("@domain" address = catch-all).
func (c *Client) CreateAlias(ctx context.Context, req AliasRequest) (Alias, error) {
	var out Alias
	return out, c.do(ctx, http.MethodPost, "/v1/admin/aliases", nil, req, "", &out)
}

// ListAliases returns one page of alias addresses in a domain.
func (c *Client) ListAliases(ctx context.Context, domain, after string, limit int) (Page, error) {
	q := pageQuery(after, limit)
	q.Set("domain", domain)
	var out Page
	return out, c.do(ctx, http.MethodGet, "/v1/admin/aliases", q, nil, "", &out)
}

// ListAliasesAll follows pages.
func (c *Client) ListAliasesAll(ctx context.Context, domain string) ([]string, error) {
	return c.collect(ctx, func(after string) (Page, error) { return c.ListAliases(ctx, domain, after, 500) })
}

// GetAlias returns an alias.
func (c *Client) GetAlias(ctx context.Context, address string) (Alias, error) {
	var out Alias
	return out, c.do(ctx, http.MethodGet, "/v1/admin/aliases/"+pathEscape(address), nil, nil, "", &out)
}

// UpdateAlias patches enabled/destinations.
func (c *Client) UpdateAlias(ctx context.Context, address string, req AliasRequest) (Alias, error) {
	var out Alias
	req.Address = ""
	return out, c.do(ctx, http.MethodPatch, "/v1/admin/aliases/"+pathEscape(address), nil, req, "", &out)
}

// DeleteAlias deletes an alias.
func (c *Client) DeleteAlias(ctx context.Context, address string) error {
	return c.do(ctx, http.MethodDelete, "/v1/admin/aliases/"+pathEscape(address), nil, nil, "", nil)
}

// ---- audit ----

// Audit returns one page of audit records.
func (c *Client) Audit(ctx context.Context, q AuditQuery) (AuditPage, error) {
	v := url.Values{}
	set := func(k, s string) {
		if s != "" {
			v.Set(k, s)
		}
	}
	set("mailbox", q.Mailbox)
	set("account", q.Account)
	set("action", q.Action)
	set("client_ip", q.ClientIP)
	set("after", q.After)
	if !q.From.IsZero() {
		v.Set("from", q.From.UTC().Format("2006-01-02"))
	}
	if !q.To.IsZero() {
		v.Set("to", q.To.UTC().Format("2006-01-02"))
	}
	if q.Limit > 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	var out AuditPage
	return out, c.do(ctx, http.MethodGet, "/v1/admin/audit", v, nil, "", &out)
}

// AuditAll follows cursors and returns every matching record.
func (c *Client) AuditAll(ctx context.Context, q AuditQuery) ([]AuditRecord, error) {
	var out []AuditRecord
	for {
		p, err := c.Audit(ctx, q)
		if err != nil {
			return out, err
		}
		out = append(out, p.Items...)
		if p.Next == "" {
			return out, nil
		}
		q.After = p.Next
	}
}

// ---- queues / keys / identity ----

// QueueStats counts a queue's tasks.
func (c *Client) QueueStats(ctx context.Context, typ string) (QueueStats, error) {
	var out QueueStats
	return out, c.do(ctx, http.MethodGet, "/v1/admin/queues/"+pathEscape(typ), nil, nil, "", &out)
}

// QueueDead lists dead-lettered tasks.
func (c *Client) QueueDead(ctx context.Context, typ string) ([]Task, error) {
	var out struct {
		Items []Task `json:"items"`
	}
	return out.Items, c.do(ctx, http.MethodGet, "/v1/admin/queues/"+pathEscape(typ)+"/dead", nil, nil, "", &out)
}

// QueueRetry requeues a dead task.
func (c *Client) QueueRetry(ctx context.Context, typ, taskID string) error {
	return c.do(ctx, http.MethodPost, "/v1/admin/queues/"+pathEscape(typ)+"/dead/"+pathEscape(taskID)+"/retry", nil, nil, "", nil)
}

// RotateKeys installs a new JWT signing key.
func (c *Client) RotateKeys(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/admin/keys/rotate", nil, nil, "", nil)
}

// ExportIdentity returns the identity dump as JSON bytes.
func (c *Client) ExportIdentity(ctx context.Context) ([]byte, error) {
	var out []byte
	return out, c.do(ctx, http.MethodGet, "/v1/admin/export", nil, nil, "", &out)
}

// ImportIdentity loads an identity dump; returns the server's summary.
func (c *Client) ImportIdentity(ctx context.Context, dump []byte) (string, error) {
	var out struct {
		Summary string `json:"summary"`
	}
	return out.Summary, c.do(ctx, http.MethodPost, "/v1/admin/import", nil, bytes.NewReader(dump), "application/json", &out)
}
