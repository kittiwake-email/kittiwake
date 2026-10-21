// Package actor carries "who is doing this, from where, over which
// protocol" through a context. Services use it for authorisation and audit
// so every protocol adapter gets both without doing anything itself.
package actor

import (
	"context"

	"github.com/google/uuid"
)

// Actor identifies the principal of a request.
type Actor struct {
	AccountID uuid.UUID
	Username  string
	MailboxID uuid.UUID // zero for admin accounts and unauthenticated contexts
	Roles     []string
	// Credential used: "password", "app-password", "token", "session", "none".
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

// IsAdmin reports whether the actor carries the admin role.
func (a Actor) IsAdmin() bool { return a.HasRole("admin") }

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
