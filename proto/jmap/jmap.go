// Package jmap is the JMAP protocol adapter (RFC 8620 core, RFC 8621 mail,
// RFC 9425 quotas) on top of the mailbox and delivery services. It is a
// thin translation layer: authorisation, hold enforcement and audit live
// in the services.
package jmap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"dovetail/service/actor"
	"dovetail/service/delivery"
	"dovetail/service/mailbox"
	"dovetail/service/submit"
	"dovetail/store/cluster"
)

// Capability URNs.
const (
	CapCore  = "urn:ietf:params:jmap:core"
	CapMail  = "urn:ietf:params:jmap:mail"
	CapQuota = "urn:ietf:params:jmap:quota"
)

// Config for the server.
type Config struct {
	// BaseURL is the public base, e.g. "https://mail.example.org".
	BaseURL           string
	MaxUploadSize     int64
	MaxCallsInRequest int
	MaxObjectsInGet   int
	MaxObjectsInSet   int
	// HoldCheck optionally reports whether a mailbox is on legal hold so the
	// session resource can mark the account read-only. Writes are refused by
	// the mailbox service regardless.
	HoldCheck func(ctx context.Context, mb uuid.UUID) bool
	// Submit enables sending (Identity and EmailSubmission methods, the
	// submission capability). nil = sending unsupported.
	Submit *submit.Service
}

func (c *Config) defaults() {
	if c.MaxUploadSize <= 0 {
		c.MaxUploadSize = 50 << 20
	}
	if c.MaxCallsInRequest <= 0 {
		c.MaxCallsInRequest = 16
	}
	if c.MaxObjectsInGet <= 0 {
		c.MaxObjectsInGet = 500
	}
	if c.MaxObjectsInSet <= 0 {
		c.MaxObjectsInSet = 500
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
}

// Server is the JMAP HTTP handler.
type Server struct {
	cfg     Config
	mb      *mailbox.Service
	del     *delivery.Service
	log     *slog.Logger
	uploads *uploadStore
	methods map[string]methodFunc
}

// New returns a server.
func New(cfg Config, mb *mailbox.Service, del *delivery.Service, log *slog.Logger) *Server {
	cfg.defaults()
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, mb: mb, del: del, log: log, uploads: newUploadStore(256 << 20)}
	s.methods = map[string]methodFunc{
		"Core/echo":          s.coreEcho,
		"Mailbox/get":        s.mailboxGet,
		"Mailbox/changes":    s.mailboxChanges,
		"Mailbox/query":      s.mailboxQuery,
		"Mailbox/set":        s.mailboxSet,
		"Email/get":          s.emailGet,
		"Email/changes":      s.emailChanges,
		"Email/query":        s.emailQuery,
		"Email/queryChanges": s.emailQueryChanges,
		"Email/set":          s.emailSet,
		"Email/import":       s.emailImport,
		"Thread/get":         s.threadGet,
		"Thread/changes":     s.threadChanges,
		"Quota/get":          s.quotaGet,
		"Identity/get":       s.identityGet,
	}
	s.registerSubmission()
	return s
}

// ServeHTTP routes /jmap/api, /jmap/download/..., /jmap/upload/..., /jmap/eventsource.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if i := strings.Index(path, "/jmap/"); i >= 0 {
		path = path[i+len("/jmap/"):]
	} else {
		path = strings.TrimPrefix(path, "/")
	}
	switch {
	case path == "api":
		s.serveAPI(w, r)
	case strings.HasPrefix(path, "download/"):
		s.serveDownload(w, r, strings.TrimPrefix(path, "download/"))
	case strings.HasPrefix(path, "upload/"):
		s.serveUpload(w, r, strings.TrimPrefix(path, "upload/"))
	case path == "eventsource":
		s.serveEventSource(w, r)
	default:
		http.NotFound(w, r)
	}
}

// SessionHandler serves the session resource (/.well-known/jmap).
func (s *Server) SessionHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, s.session(r.Context()))
	})
}

func (s *Server) url(suffix string) string { return s.cfg.BaseURL + suffix }

// session builds the session resource for the actor in ctx.
func (s *Server) session(ctx context.Context) map[string]any {
	act := actor.From(ctx)
	coreCap := map[string]any{
		"maxSizeUpload":         s.cfg.MaxUploadSize,
		"maxConcurrentUpload":   4,
		"maxSizeRequest":        10 << 20,
		"maxConcurrentRequests": 4,
		"maxCallsInRequest":     s.cfg.MaxCallsInRequest,
		"maxObjectsInGet":       s.cfg.MaxObjectsInGet,
		"maxObjectsInSet":       s.cfg.MaxObjectsInSet,
		"collationAlgorithms":   []string{"i;ascii-numeric", "i;ascii-casemap", "i;unicode-casemap"},
	}
	mailCap := map[string]any{
		"maxMailboxesPerEmail":       1,
		"maxMailboxDepth":            16,
		"maxSizeMailboxName":         255,
		"maxSizeAttachmentsPerEmail": s.cfg.MaxUploadSize,
		"emailQuerySortOptions":      []string{"receivedAt", "sentAt", "subject", "from", "size"},
		"mayCreateTopLevelMailbox":   true,
	}
	accounts := map[string]any{}
	primary := map[string]any{}
	state := "0"
	if act.MailboxID != uuid.Nil {
		id := act.MailboxID.String()
		readOnly := !act.HasRole("write")
		if s.cfg.HoldCheck != nil && s.cfg.HoldCheck(ctx, act.MailboxID) {
			readOnly = true
		}
		acctCaps := map[string]any{
			CapMail:  mailCap,
			CapQuota: map[string]any{},
		}
		if s.submissionEnabled() {
			acctCaps[CapSubmission] = map[string]any{"maxDelayedSend": 0, "submissionExtensions": map[string]any{}}
			primary[CapSubmission] = id
		}
		accounts[id] = map[string]any{
			"name":                act.Username,
			"isPersonal":          true,
			"isReadOnly":          readOnly,
			"accountCapabilities": acctCaps,
		}
		primary[CapMail] = id
		primary[CapQuota] = id
		state = s.state(ctx, act.MailboxID)
	}
	caps := map[string]any{
		CapCore:  coreCap,
		CapMail:  map[string]any{},
		CapQuota: map[string]any{},
	}
	if s.submissionEnabled() {
		caps[CapSubmission] = map[string]any{"maxDelayedSend": 0, "submissionExtensions": map[string]any{}}
	}
	return map[string]any{
		"capabilities":    caps,
		"accounts":        accounts,
		"primaryAccounts": primary,
		"username":        act.Username,
		"apiUrl":          s.url("/jmap/api"),
		"downloadUrl":     s.url("/jmap/download/{accountId}/{blobId}/{name}?accept={type}"),
		"uploadUrl":       s.url("/jmap/upload/{accountId}"),
		"eventSourceUrl":  s.url("/jmap/eventsource?types={types}&closeafter={closeafter}&ping={ping}"),
		"state":           state,
	}
}

// state returns the mailbox modseq as a state string ("0" on error). It
// uses the root as this process last saw it: our own writes refresh that
// copy, so a method's newState is exact, and another process's writes are
// announced by push (freshState) rather than discovered here.
func (s *Server) state(ctx context.Context, mb uuid.UUID) string {
	root, err := s.mb.RootCached(ctx, mb)
	if err != nil {
		return "0"
	}
	return strconv.FormatUint(root.ModSeq, 10)
}

// freshState reads the root (one RADOS read); used when a change
// notification from another process arrived.
func (s *Server) freshState(ctx context.Context, mb uuid.UUID) string {
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return "0"
	}
	return strconv.FormatUint(root.ModSeq, 10)
}

// accountFor validates the accountId argument against the actor.
func (s *Server) accountFor(ctx context.Context, args map[string]any) (uuid.UUID, *methodError) {
	idStr, ok := args["accountId"].(string)
	if !ok || idStr == "" {
		return uuid.Nil, invalidArguments("accountId is required")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return uuid.Nil, &methodError{Type: "accountNotFound"}
	}
	act := actor.From(ctx)
	if id != act.MailboxID && !act.IsAdmin() {
		return uuid.Nil, &methodError{Type: "accountNotFound"}
	}
	return id, nil
}

// mapErr converts service errors to JMAP method errors.
func mapErr(err error) *methodError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mailbox.ErrForbidden):
		return &methodError{Type: "forbidden"}
	case errors.Is(err, mailbox.ErrHold):
		return &methodError{Type: "forbidden", Description: "mailbox is on legal hold and read-only"}
	case errors.Is(err, mailbox.ErrSealed):
		return &methodError{Type: "forbidden", Description: "mailbox is sealed"}
	case errors.Is(err, mailbox.ErrNotFound):
		return &methodError{Type: "notFound"}
	case errors.Is(err, mailbox.ErrNotEmpty):
		if strings.Contains(err.Error(), "children") {
			return &methodError{Type: "mailboxHasChild"}
		}
		return &methodError{Type: "mailboxHasEmail"}
	case errors.Is(err, mailbox.ErrInvalid):
		return &methodError{Type: "invalidProperties", Description: err.Error()}
	case errors.Is(err, mailbox.ErrNoHorizon):
		return &methodError{Type: "cannotCalculateChanges"}
	case errors.Is(err, delivery.ErrOverQuota):
		return &methodError{Type: "overQuota"}
	case errors.Is(err, delivery.ErrTooLarge):
		return &methodError{Type: "tooLarge"}
	case errors.Is(err, delivery.ErrHold):
		return &methodError{Type: "forbidden", Description: "mailbox is on legal hold and read-only"}
	case errors.Is(err, delivery.ErrNoFolder):
		return &methodError{Type: "notFound", Description: "mailbox not found"}
	case errors.Is(err, cluster.ErrUnavailable), errors.Is(err, delivery.ErrUnavailable):
		return &methodError{Type: "serverUnavailable", Description: "mailbox storage temporarily unavailable"}
	}
	return &methodError{Type: "serverFail", Description: err.Error()}
}

func serverFail(format string, a ...any) *methodError {
	return &methodError{Type: "serverFail", Description: fmt.Sprintf(format, a...)}
}
