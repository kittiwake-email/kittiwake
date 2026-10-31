package auth

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHandoff(t *testing.T) {
	priv, err := NewHandoffKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := priv.Public().(ed25519.PublicKey)
	// Config and API encodings round-trip.
	p2, err := ParseHandoffPrivate(EncodeHandoffPrivate(priv))
	if err != nil || !p2.Equal(priv) {
		t.Fatalf("private round trip: %v", err)
	}
	pk, err := ParseHandoffPublic(EncodeHandoffPublic(pub))
	if err != nil || !pk.Equal(pub) {
		t.Fatalf("public round trip: %v", err)
	}
	if _, err := ParseHandoffPrivate("not base64!"); err == nil {
		t.Fatal("garbage accepted")
	}
	if _, err := ParseHandoffPublic("AAAA"); err == nil {
		t.Fatal("short key accepted")
	}
	if len(HandoffKeyID(pub)) != 16 {
		t.Fatal("key id")
	}

	now := time.Now()
	keys := map[string]ed25519.PublicKey{HandoffKeyID(pub): pub}
	keyFor := func(kid string) (ed25519.PublicKey, bool) { k, ok := keys[kid]; return k, ok }
	h := Handoff{AccountID: uuid.New(), Username: "wido@example.org", PasswordID: "p1", Credential: "password", Service: "imap", ClientIP: "2001:db8::1", IssuedAt: now, ExpiresAt: now.Add(HandoffLifetime)}
	tok, err := SignHandoff(priv, h)
	if err != nil || !strings.HasPrefix(tok, HandoffPrefix) {
		t.Fatalf("sign: %v", err)
	}
	got, err := VerifyHandoff(tok, keyFor, now)
	if err != nil || got.AccountID != h.AccountID || got.Username != h.Username || got.PasswordID != "p1" || got.Credential != "password" || got.Service != "imap" || got.ClientIP != h.ClientIP || got.KeyID != HandoffKeyID(pub) {
		t.Fatalf("verify: %+v %v", got, err)
	}
	if _, err := VerifyHandoff(tok, keyFor, now.Add(HandoffLifetime+time.Second)); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expired: %v", err)
	}
	// Unknown key, tampered body, tampered signature.
	if _, err := VerifyHandoff(tok, func(string) (ed25519.PublicKey, bool) { return nil, false }, now); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown key: %v", err)
	}
	other, _ := NewHandoffKey()
	wrong := map[string]ed25519.PublicKey{HandoffKeyID(pub): other.Public().(ed25519.PublicKey)}
	if _, err := VerifyHandoff(tok, func(kid string) (ed25519.PublicKey, bool) { k, ok := wrong[kid]; return k, ok }, now); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("key id mismatch: %v", err)
	}
	parts := strings.SplitN(tok, ".", 3)
	if _, err := VerifyHandoff(parts[0]+"."+parts[1]+"x."+parts[2], keyFor, now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("tampered body: %v", err)
	}
	if _, err := VerifyHandoff(parts[0]+"."+parts[1]+"."+parts[2][:len(parts[2])-2]+"AA", keyFor, now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("tampered signature: %v", err)
	}
	if _, err := VerifyHandoff("kwt_nope", keyFor, now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("not a handoff: %v", err)
	}
	if _, err := SignHandoff(priv, Handoff{Username: "x"}); err == nil {
		t.Fatal("incomplete handoff signed")
	}
}
