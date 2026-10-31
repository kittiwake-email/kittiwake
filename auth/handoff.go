package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Handoff tokens: a proxy that verified a password itself tells the
// backend so with a short-lived token signed by its site's Ed25519 key
// (README "Proxies"). The backend knows the public keys of the proxy
// sites (registered through the admin API) and accepts the token in
// place of the password on IMAP, POP3, submission (XOAUTH2) and HTTP
// (Bearer). The token is bound to the account, the client address and
// the service, and lives for seconds, so a captured one is worthless.

// HandoffPrefix marks handoff tokens.
const HandoffPrefix = "kwh1."

// HandoffLifetime is how long a handoff token stays valid.
const HandoffLifetime = 30 * time.Second

// Handoff is what the token carries.
type Handoff struct {
	KeyID      string    // the proxy key that signed it
	AccountID  uuid.UUID // the verified account
	Username   string
	PasswordID string // which of the account's passwords matched
	Credential string // "password" or "app-password"
	Service    string // imap, pop3, submission, jmap
	ClientIP   string // the client the proxy saw
	IssuedAt   time.Time
	ExpiresAt  time.Time
}

type handoffPayload struct {
	Kid string `json:"kid"`
	Sub string `json:"sub"`
	Usr string `json:"usr"`
	Pid string `json:"pid"`
	Crd string `json:"crd"`
	Svc string `json:"svc"`
	IP  string `json:"ip"`
	Iat int64  `json:"iat"`
	Exp int64  `json:"exp"`
}

// NewHandoffKey generates a proxy site key.
func NewHandoffKey() (ed25519.PrivateKey, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	return priv, err
}

// HandoffKeyID names a key by its public half: 16 hex characters of its
// SHA-256, so the proxy and the registry derive the same id without
// agreeing on one.
func HandoffKeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:8])
}

// EncodeHandoffPrivate renders a private key for a config file: the
// base64 of its 32-byte seed.
func EncodeHandoffPrivate(priv ed25519.PrivateKey) string {
	return base64.StdEncoding.EncodeToString(priv.Seed())
}

// ParseHandoffPrivate reads a private key written by EncodeHandoffPrivate
// (a base64 64-byte key is accepted too).
func ParseHandoffPrivate(s string) (ed25519.PrivateKey, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("auth: handoff key: %w", err)
	}
	switch len(b) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(b), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(b), nil
	}
	return nil, errors.New("auth: handoff key: not an Ed25519 seed")
}

// EncodeHandoffPublic renders a public key for the admin API (base64).
func EncodeHandoffPublic(pub ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(pub)
}

// ParseHandoffPublic reads a public key written by EncodeHandoffPublic.
func ParseHandoffPublic(s string) (ed25519.PublicKey, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("auth: public key: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, errors.New("auth: public key: not an Ed25519 key")
	}
	return ed25519.PublicKey(b), nil
}

// SignHandoff produces "kwh1.<payload>.<signature>" (base64url, no
// padding). KeyID is derived from the key.
func SignHandoff(priv ed25519.PrivateKey, h Handoff) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", errors.New("auth: invalid handoff key")
	}
	if h.AccountID == uuid.Nil || h.Username == "" || h.Service == "" || h.ExpiresAt.IsZero() {
		return "", errors.New("auth: incomplete handoff")
	}
	p := handoffPayload{
		Kid: HandoffKeyID(priv.Public().(ed25519.PublicKey)), Sub: h.AccountID.String(), Usr: h.Username, Pid: h.PasswordID,
		Crd: h.Credential, Svc: h.Service, IP: h.ClientIP, Iat: h.IssuedAt.Unix(), Exp: h.ExpiresAt.Unix(),
	}
	pb, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	body := b64url.EncodeToString(pb)
	sig := ed25519.Sign(priv, []byte(body))
	return HandoffPrefix + body + "." + b64url.EncodeToString(sig), nil
}

// VerifyHandoff checks a token against the key keyFor names and returns
// its contents. Expiry is checked here; binding to the client address
// and service is the caller's, who knows them.
func VerifyHandoff(token string, keyFor func(kid string) (ed25519.PublicKey, bool), now time.Time) (Handoff, error) {
	if !strings.HasPrefix(token, HandoffPrefix) {
		return Handoff{}, ErrTokenInvalid
	}
	body, sigText, ok := strings.Cut(token[len(HandoffPrefix):], ".")
	if !ok {
		return Handoff{}, ErrTokenInvalid
	}
	pb, err := b64url.DecodeString(body)
	if err != nil {
		return Handoff{}, ErrTokenInvalid
	}
	var p handoffPayload
	if err := json.Unmarshal(pb, &p); err != nil || p.Kid == "" {
		return Handoff{}, ErrTokenInvalid
	}
	pub, ok := keyFor(p.Kid)
	if !ok || len(pub) != ed25519.PublicKeySize || HandoffKeyID(pub) != p.Kid {
		return Handoff{}, ErrUnknownKey
	}
	sig, err := b64url.DecodeString(sigText)
	if err != nil || !ed25519.Verify(pub, []byte(body), sig) {
		return Handoff{}, ErrTokenInvalid
	}
	if p.Exp == 0 || !now.Before(time.Unix(p.Exp, 0)) {
		return Handoff{}, ErrTokenExpired
	}
	acct, err := uuid.Parse(p.Sub)
	if err != nil || acct == uuid.Nil || p.Usr == "" || p.Svc == "" {
		return Handoff{}, ErrTokenInvalid
	}
	return Handoff{
		KeyID: p.Kid, AccountID: acct, Username: p.Usr, PasswordID: p.Pid, Credential: p.Crd, Service: p.Svc, ClientIP: p.IP,
		IssuedAt: time.Unix(p.Iat, 0).UTC(), ExpiresAt: time.Unix(p.Exp, 0).UTC(),
	}, nil
}
