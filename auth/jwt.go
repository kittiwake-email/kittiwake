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

// SigningKey is an Ed25519 key pair for access tokens.
type SigningKey struct {
	ID      string             `json:"id"`
	Private ed25519.PrivateKey `json:"private"`
	Public  ed25519.PublicKey  `json:"public"`
	Created time.Time          `json:"created"`
}

// NewSigningKey generates a key with a random 16-hex-char id.
func NewSigningKey(now time.Time) (*SigningKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	return &SigningKey{ID: hex.EncodeToString(id[:]), Private: priv, Public: pub, Created: now.UTC()}, nil
}

// Claims carried in an access token.
type Claims struct {
	AccountID uuid.UUID
	MailboxID uuid.UUID
	Username  string
	Roles     []string
	SessionID string
	Issuer    string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// Token errors.
var (
	ErrTokenInvalid = errors.New("auth: invalid token")
	ErrTokenExpired = errors.New("auth: token expired")
	ErrUnknownKey   = errors.New("auth: unknown signing key")
)

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

type jwtPayload struct {
	Iss   string   `json:"iss"`
	Sub   string   `json:"sub"`
	Iat   int64    `json:"iat"`
	Exp   int64    `json:"exp"`
	Jti   string   `json:"jti"`
	Mbx   string   `json:"mbx,omitempty"`
	Usr   string   `json:"usr"`
	Roles []string `json:"roles"`
}

var b64url = base64.RawURLEncoding

// SignAccessToken produces a compact EdDSA JWT.
func SignAccessToken(key *SigningKey, c Claims) (string, error) {
	if key == nil || len(key.Private) != ed25519.PrivateKeySize {
		return "", errors.New("auth: invalid signing key")
	}
	if c.AccountID == uuid.Nil || c.SessionID == "" || c.ExpiresAt.IsZero() {
		return "", errors.New("auth: incomplete claims")
	}
	hb, err := json.Marshal(jwtHeader{Alg: "EdDSA", Typ: "JWT", Kid: key.ID})
	if err != nil {
		return "", err
	}
	p := jwtPayload{
		Iss:   c.Issuer,
		Sub:   c.AccountID.String(),
		Iat:   c.IssuedAt.Unix(),
		Exp:   c.ExpiresAt.Unix(),
		Jti:   c.SessionID,
		Usr:   c.Username,
		Roles: c.Roles,
	}
	if p.Roles == nil {
		p.Roles = []string{}
	}
	if c.MailboxID != uuid.Nil {
		p.Mbx = c.MailboxID.String()
	}
	pb, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	signing := b64url.EncodeToString(hb) + "." + b64url.EncodeToString(pb)
	sig := ed25519.Sign(key.Private, []byte(signing))
	return signing + "." + b64url.EncodeToString(sig), nil
}

// VerifyAccessToken parses and verifies a token.
func VerifyAccessToken(token string, keyFor func(kid string) (ed25519.PublicKey, bool), expectedIssuer string, now time.Time) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, ErrTokenInvalid
	}
	hb, err := b64url.DecodeString(parts[0])
	if err != nil {
		return Claims{}, ErrTokenInvalid
	}
	var h jwtHeader
	if err := json.Unmarshal(hb, &h); err != nil {
		return Claims{}, ErrTokenInvalid
	}
	if h.Alg != "EdDSA" || h.Kid == "" {
		return Claims{}, ErrTokenInvalid
	}
	pub, ok := keyFor(h.Kid)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return Claims{}, ErrUnknownKey
	}
	sig, err := b64url.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrTokenInvalid
	}
	if !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) {
		return Claims{}, ErrTokenInvalid
	}
	pb, err := b64url.DecodeString(parts[1])
	if err != nil {
		return Claims{}, ErrTokenInvalid
	}
	var p jwtPayload
	if err := json.Unmarshal(pb, &p); err != nil {
		return Claims{}, ErrTokenInvalid
	}
	if p.Exp == 0 || !now.Before(time.Unix(p.Exp, 0)) {
		return Claims{}, ErrTokenExpired
	}
	if expectedIssuer != "" && p.Iss != expectedIssuer {
		return Claims{}, fmt.Errorf("%w: issuer mismatch", ErrTokenInvalid)
	}
	acct, err := uuid.Parse(p.Sub)
	if err != nil || acct == uuid.Nil {
		return Claims{}, ErrTokenInvalid
	}
	c := Claims{
		AccountID: acct,
		Username:  p.Usr,
		Roles:     p.Roles,
		SessionID: p.Jti,
		Issuer:    p.Iss,
		IssuedAt:  time.Unix(p.Iat, 0).UTC(),
		ExpiresAt: time.Unix(p.Exp, 0).UTC(),
	}
	if p.Mbx != "" {
		if c.MailboxID, err = uuid.Parse(p.Mbx); err != nil {
			return Claims{}, ErrTokenInvalid
		}
	}
	if c.SessionID == "" {
		return Claims{}, ErrTokenInvalid
	}
	return c, nil
}

// NewRefreshToken returns a random "kwr_"-prefixed secret and its hash.
func NewRefreshToken() (plain, hash string, err error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", "", err
	}
	plain = "kwr_" + b64url.EncodeToString(b[:])
	return plain, HashRefreshToken(plain), nil
}

// HashRefreshToken returns the hex SHA-256 of a refresh token.
func HashRefreshToken(plain string) string {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:])
}
