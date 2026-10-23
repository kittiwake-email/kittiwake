package actor

import (
	"context"
	"errors"
	"kittiwake/store/identity"
	"time"
)

var ErrForbidden = errors.New("actor: forbidden")

// CheckAccount revalidates a user against the bounded identity cache. The
// actor's roles are a ceiling: a later account grant cannot widen a token.
func CheckAccount(ctx context.Context, ids *identity.Store, a Actor, role string, now time.Time) error {
	if !a.ExpiresAt.IsZero() && !now.Before(a.ExpiresAt) {
		return ErrForbidden
	}
	if a.IsAdminReader() || a.IsSystem() {
		return nil
	}
	if a.Anonymous() || (role != "" && !a.HasRole(role)) {
		return ErrForbidden
	}
	acct, err := ids.GetAccountByUsernameCached(ctx, a.Username)
	if err != nil {
		return ErrForbidden
	}
	if acct.ID != a.AccountID || !acct.Enabled || acct.MailboxID != a.MailboxID {
		return ErrForbidden
	}
	if role != "" && !acct.HasRole(role) {
		return ErrForbidden
	}
	if svc := identity.ServiceFor(a.Protocol); svc != "" && !acct.HasService(svc) {
		return ErrForbidden
	}
	return nil
}
