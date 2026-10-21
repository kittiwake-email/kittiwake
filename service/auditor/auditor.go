// Package auditor writes audit records to the structured log and to the
// audit store, taking the actor from the context. See README.md "Auditing".
package auditor

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"dovetail/service/actor"
	"dovetail/store/audit"
)

// Detail levels per protocol.
const (
	DetailFull    = "full"
	DetailSession = "session"
)

// Auditor is the audit sink.
type Auditor struct {
	store  *audit.Store
	log    *slog.Logger
	detail map[string]string
	Now    func() time.Time
}

// New returns an auditor. detail maps protocol -> DetailFull/DetailSession;
// missing protocols are DetailFull.
func New(store *audit.Store, log *slog.Logger, detail map[string]string) *Auditor {
	if log == nil {
		log = slog.Default()
	}
	return &Auditor{store: store, log: log, detail: detail, Now: time.Now}
}

// Session-level actions are always recorded even at DetailSession.
var sessionActions = map[string]bool{
	"login": true, "login-failed": true, "logout": true, "refresh": true, "revoke": true,
	"send": true, "hold": true, "release": true, "export": true, "delete": true, "destroy": true,
	"expunge": true, "folder-create": true, "folder-rename": true, "folder-delete": true,
}

// Event is the input to Log.
type Event struct {
	Mailbox uuid.UUID // target mailbox; zero for platform-level actions
	Action  string
	Target  string
	Detail  string
	Result  string
	// Subject overrides the account fields with the acted-upon user when an
	// admin acts on behalf of someone (support access, maintenance).
	Subject string
}

// Log records an event with the actor from the context.
func (a *Auditor) Log(ctx context.Context, ev Event) {
	act := actor.From(ctx)
	if lvl := a.detail[act.Protocol]; lvl == DetailSession && !sessionActions[ev.Action] && ev.Result == audit.ResultOK {
		return
	}
	rec := audit.Record{
		Time: a.Now().UTC(), Mailbox: ev.Mailbox, Account: act.Username, AccountID: act.AccountID,
		TokenID: act.TokenID, TokenNote: act.TokenNote, SessionID: act.SessionID, Protocol: act.Protocol,
		ClientIP: act.ClientIP, Action: ev.Action, Target: ev.Target, Detail: ev.Detail, Result: ev.Result, Admin: act.Admin,
	}
	if ev.Result == "" {
		rec.Result = audit.ResultOK
	}
	if ev.Subject != "" {
		rec.Admin = act.Username
		rec.Account = ev.Subject
		rec.AccountID = uuid.Nil
	}
	a.log.LogAttrs(ctx, slog.LevelInfo, "audit",
		slog.String("action", rec.Action), slog.String("result", rec.Result), slog.String("protocol", rec.Protocol),
		slog.String("account", rec.Account), slog.String("client_ip", rec.ClientIP), slog.String("mailbox", mbString(rec.Mailbox)),
		slog.String("target", rec.Target), slog.String("detail", rec.Detail), slog.String("admin", rec.Admin), slog.String("token_id", rec.TokenID))
	if a.store != nil {
		if err := a.store.Write(ctx, rec); err != nil {
			a.log.ErrorContext(ctx, "audit store write failed", slog.String("error", err.Error()), slog.String("action", rec.Action))
		}
	}
}

func mbString(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
