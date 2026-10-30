// Package actor carries "who is doing this, from where, over which
// protocol" through a context. Services use it for authorisation and audit
// so every protocol adapter gets both without doing anything itself.
package actor

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Actor identifies the principal of a request.
type Actor struct {
	AccountID uuid.UUID
	Username  string
	MailboxID uuid.UUID // zero for admin accounts and unauthenticated contexts
	Roles     []string
	// Credential used: "password", "app-password", "token", "session", "none".
	ExpiresAt  time.Time // credential expiry; zero for non-expiring connection credentials
	Credential string
	TokenID    string
	TokenNote  string
	SessionID  string
	// Protocol: imap, pop3, jmap, lmtp, rest, admin, cli.
	Protocol string
	ClientIP string
	// Admin is the admin username when an admin acts on someone else's
	// mailbox (support access); empty otherwise.
	Admin string
	// Namespace confines an admin token to one namespace (a tenant admin);
	// empty for platform admins and for every non-admin actor.
	Namespace string
}

// HasRole reports whether the actor carries the role.
func (a Actor) HasRole(role string) bool {
	for _, r := range a.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Admin roles (mirrors store/identity).
const (
	RoleAdmin     = "admin"
	RoleAdminRead = "admin-ro"
)

// IsAdmin reports whether the actor is a full administrator.
func (a Actor) IsAdmin() bool { return a.HasRole(RoleAdmin) }

// IsAdminReader reports whether the actor may perform read-only
// administration (full admins included).
func (a Actor) IsAdminReader() bool { return a.HasRole(RoleAdmin) || a.HasRole(RoleAdminRead) }

// Administers reports whether an admin actor's scope covers the namespace:
// platform admins cover every namespace, tenant admins only their own.
// Non-admin actors never administer anything.
func (a Actor) Administers(namespace string) bool {
	return a.IsAdmin() && (a.Namespace == "" || a.Namespace == namespace)
}

// Reads reports whether a (read-only or full) admin actor's scope covers
// the namespace.
func (a Actor) Reads(namespace string) bool {
	return a.IsAdminReader() && (a.Namespace == "" || a.Namespace == namespace)
}

// IsSystem reports the CLI/worker system actor.
func (a Actor) IsSystem() bool {
	return a.Username == "system" && a.AccountID == uuid.Nil && a.Credential == "none"
}

// Anonymous reports whether no account is attached.
func (a Actor) Anonymous() bool { return a.AccountID == uuid.Nil }

type ctxKey struct{}

// With returns a context carrying the actor.
func With(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, ctxKey{}, a)
}

// From returns the actor in the context, or a zero Actor.
func From(ctx context.Context) Actor {
	a, _ := ctx.Value(ctxKey{}).(Actor)
	return a
}

// System returns an actor for internal/background work.
func System(protocol string) Actor {
	return Actor{Username: "system", Credential: "none", Protocol: protocol}
}
