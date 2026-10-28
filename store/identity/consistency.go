package identity

import "context"

type primaryReadKey struct{}

// WithPrimaryReads bypasses replica routing and identity caches for decisions
// that must observe committed registry state, particularly destructive work.
func WithPrimaryReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, primaryReadKey{}, true)
}
func PrimaryReads(ctx context.Context) bool {
	v, _ := ctx.Value(primaryReadKey{}).(bool)
	return v
}
