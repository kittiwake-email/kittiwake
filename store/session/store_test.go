package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"dovetail/rados"
)

func TestSessionsLifecycle(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	s := New(io)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	ctx := context.Background()
	acct := uuid.New()

	sess, plain, err := s.Create(ctx, acct, []string{"read"}, "phone", "::1", time.Hour)
	if err != nil || plain[:4] != "dvr_" {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, acct, sess.ID)
	if err != nil || got.Client != "phone" {
		t.Fatal(err)
	}
	// Wrong token.
	if _, _, err := s.Refresh(ctx, acct, sess.ID, "dvr_wrong", "::1", time.Hour); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong token: %v", err)
	}
	// Rotate.
	now = now.Add(time.Minute)
	sess2, plain2, err := s.Refresh(ctx, acct, sess.ID, plain, "::2", time.Hour)
	if err != nil || plain2 == plain || sess2.LastIP != "::2" || !sess2.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("rotate: %+v %v", sess2, err)
	}
	// Reuse of the old token revokes the session.
	if _, _, err := s.Refresh(ctx, acct, sess.ID, plain, "::1", time.Hour); !errors.Is(err, ErrReused) {
		t.Fatalf("reuse: %v", err)
	}
	if _, err := s.Get(ctx, acct, sess.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("session should be revoked")
	}
	// Expiry.
	sess3, plain3, _ := s.Create(ctx, acct, nil, "laptop", "", time.Minute)
	now = now.Add(2 * time.Minute)
	if _, _, err := s.Refresh(ctx, acct, sess3.ID, plain3, "", time.Hour); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired: %v", err)
	}
	// List drops expired.
	s4, _, _ := s.Create(ctx, acct, nil, "a", "", time.Hour)
	_, _, _ = s.Create(ctx, acct, nil, "b", "", time.Second)
	now = now.Add(10 * time.Second)
	list, _ := s.List(ctx, acct)
	if len(list) != 1 || list[0].ID != s4.ID {
		t.Fatalf("list: %+v", list)
	}
	if err := s.Touch(ctx, acct, s4.ID, "::9"); err != nil {
		t.Fatal(err)
	}
	g, _ := s.Get(ctx, acct, s4.ID)
	if g.LastIP != "::9" {
		t.Fatal("touch")
	}
	if err := s.RevokeAll(ctx, acct); err != nil {
		t.Fatal(err)
	}
	list, _ = s.List(ctx, acct)
	if len(list) != 0 {
		t.Fatal("revoke all")
	}
	if err := s.Revoke(ctx, acct, "nope"); err != nil {
		t.Fatal("revoke missing should be nil")
	}
}

func TestKeysAndTOTP(t *testing.T) {
	c := rados.NewMemoryClient()
	io, _ := c.Pool("index")
	s := New(io)
	ctx := context.Background()
	ks, err := s.Keys(ctx)
	if err != nil || ks.Current() == nil || len(ks.Keys) != 1 {
		t.Fatal(err)
	}
	ks2, _ := s.Keys(ctx)
	if ks2.CurrentID != ks.CurrentID {
		t.Fatal("keys not stable")
	}
	rot, err := s.Rotate(ctx, 1)
	if err != nil || rot.CurrentID == ks.CurrentID || len(rot.Keys) != 2 || rot.ByID(ks.CurrentID) == nil {
		t.Fatalf("rotate: %+v %v", rot, err)
	}
	rot2, _ := s.Rotate(ctx, 1)
	if len(rot2.Keys) != 2 || rot2.ByID(ks.CurrentID) != nil {
		t.Fatal("old key should be dropped")
	}
	acct := uuid.New()
	ok, err := s.ClaimTOTPStep(ctx, acct, 100)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimTOTPStep(ctx, acct, 100); ok {
		t.Fatal("replay accepted")
	}
	if ok, _ := s.ClaimTOTPStep(ctx, acct, 99); ok {
		t.Fatal("older step accepted")
	}
	if ok, _ := s.ClaimTOTPStep(ctx, acct, 101); !ok {
		t.Fatal("next step rejected")
	}
	if err := s.ResetTOTP(ctx, acct); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimTOTPStep(ctx, acct, 50); !ok {
		t.Fatal("reset failed")
	}
}
