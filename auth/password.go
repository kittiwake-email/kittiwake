// Package auth holds Dovetail's credential primitives: Dovecot-style
// scheme-prefixed password hashes, TOTP, recovery codes, Ed25519-signed JWT
// access tokens and opaque refresh tokens. It does no I/O and keeps no state.
package auth

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

// ErrUnknownScheme is returned for hashes whose scheme is not supported.
var ErrUnknownScheme = errors.New("auth: unknown password scheme")

// Default argon2id parameters.
const (
	argonTime    = 1
	argonMemory  = 64 * 1024
	argonThreads = 4
	argonSaltLen = 16
	argonKeyLen  = 32
)

// HashPassword hashes plain with the default scheme {ARGON2ID}.
func HashPassword(plain string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(plain), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return fmt.Sprintf("{ARGON2ID}$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

// Scheme returns the upper-case scheme name of a prefixed hash, or "".
func Scheme(h string) string {
	if !strings.HasPrefix(h, "{") {
		return ""
	}
	i := strings.IndexByte(h, '}')
	if i < 0 {
		return ""
	}
	return strings.ToUpper(h[1:i])
}

// splitScheme returns scheme (upper, without encoding suffix), encoding
// ("", "HEX" or "B64") and the remainder.
func splitScheme(h string) (scheme, enc, rest string) {
	if !strings.HasPrefix(h, "{") {
		return "", "", h
	}
	i := strings.IndexByte(h, '}')
	if i < 0 {
		return "", "", h
	}
	scheme = strings.ToUpper(h[1:i])
	rest = h[i+1:]
	if j := strings.LastIndexByte(scheme, '.'); j >= 0 {
		suffix := scheme[j+1:]
		if suffix == "HEX" || suffix == "B64" || suffix == "BASE64" {
			enc = suffix
			if enc == "BASE64" {
				enc = "B64"
			}
			scheme = scheme[:j]
		}
	}
	return scheme, enc, rest
}

// NeedsRehash reports whether a hash should be upgraded to the default
// scheme and parameters.
func NeedsRehash(h string) bool {
	scheme, _, rest := splitScheme(h)
	if scheme != "ARGON2ID" {
		return true
	}
	p, err := parseArgon(rest)
	if err != nil {
		return true
	}
	return p.time != argonTime || p.memory != argonMemory || p.threads != argonThreads || len(p.key) != argonKeyLen
}

// VerifyPassword checks plain against a scheme-prefixed hash.
func VerifyPassword(h, plain string) (bool, error) {
	scheme, enc, rest := splitScheme(h)
	switch scheme {
	case "":
		if strings.Contains(rest, "$") {
			return false, ErrUnknownScheme
		}
		return ConstantTimeEqual(rest, plain), nil
	case "PLAIN", "CLEARTEXT":
		return ConstantTimeEqual(rest, plain), nil
	case "ARGON2ID", "ARGON2I":
		p, err := parseArgon(rest)
		if err != nil {
			return false, err
		}
		if (scheme == "ARGON2ID") != (p.variant == "argon2id") {
			return false, fmt.Errorf("auth: scheme %s does not match hash variant %s", scheme, p.variant)
		}
		var key []byte
		if p.variant == "argon2id" {
			key = argon2.IDKey([]byte(plain), p.salt, p.time, p.memory, p.threads, uint32(len(p.key)))
		} else {
			key = argon2.Key([]byte(plain), p.salt, p.time, p.memory, p.threads, uint32(len(p.key)))
		}
		return subtle.ConstantTimeCompare(key, p.key) == 1, nil
	case "BLF-CRYPT", "BCRYPT":
		err := bcrypt.CompareHashAndPassword([]byte(rest), []byte(plain))
		if err == nil {
			return true, nil
		}
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return false, nil
		}
		return false, fmt.Errorf("auth: malformed bcrypt hash: %w", err)
	case "SHA512-CRYPT", "SHA256-CRYPT":
		return verifyShaCrypt(rest, plain)
	case "SSHA", "SSHA256", "SSHA512":
		raw, err := decodeDigest(rest, enc)
		if err != nil {
			return false, err
		}
		h := newHashFor(scheme)
		if len(raw) < h.Size() {
			return false, errors.New("auth: malformed salted hash: too short")
		}
		digest, salt := raw[:h.Size()], raw[h.Size():]
		h.Write([]byte(plain))
		h.Write(salt)
		return subtle.ConstantTimeCompare(h.Sum(nil), digest) == 1, nil
	case "SHA", "SHA1", "SHA256", "SHA512":
		raw, err := decodeDigest(rest, enc)
		if err != nil {
			return false, err
		}
		h := newHashFor(scheme)
		if len(raw) != h.Size() {
			return false, errors.New("auth: malformed hash: wrong digest length")
		}
		h.Write([]byte(plain))
		return subtle.ConstantTimeCompare(h.Sum(nil), raw) == 1, nil
	}
	return false, ErrUnknownScheme
}

func newHashFor(scheme string) hash.Hash {
	switch scheme {
	case "SHA", "SHA1", "SSHA":
		return sha1.New()
	case "SHA256", "SSHA256":
		return sha256.New()
	default:
		return sha512.New()
	}
}

func decodeDigest(s, enc string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if enc == "HEX" {
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("auth: malformed hex hash: %w", err)
		}
		return b, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		b, err = base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("auth: malformed base64 hash: %w", err)
		}
	}
	return b, nil
}

type argonParams struct {
	variant string
	version int
	memory  uint32
	time    uint32
	threads uint8
	salt    []byte
	key     []byte
}

// parseArgon parses a PHC argon2 string: $argon2id$v=19$m=65536,t=1,p=4$salt$hash
func parseArgon(s string) (argonParams, error) {
	var p argonParams
	parts := strings.Split(s, "$")
	if len(parts) != 6 || parts[0] != "" {
		return p, errors.New("auth: malformed argon2 hash")
	}
	p.variant = parts[1]
	if p.variant != "argon2id" && p.variant != "argon2i" {
		return p, fmt.Errorf("auth: unsupported argon2 variant %q", p.variant)
	}
	if !strings.HasPrefix(parts[2], "v=") {
		return p, errors.New("auth: malformed argon2 version")
	}
	v, err := strconv.Atoi(parts[2][2:])
	if err != nil {
		return p, errors.New("auth: malformed argon2 version")
	}
	p.version = v
	for _, kv := range strings.Split(parts[3], ",") {
		k, val, ok := strings.Cut(kv, "=")
		if !ok {
			return p, errors.New("auth: malformed argon2 params")
		}
		n, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return p, errors.New("auth: malformed argon2 params")
		}
		switch k {
		case "m":
			p.memory = uint32(n)
		case "t":
			p.time = uint32(n)
		case "p":
			if n > 255 {
				return p, errors.New("auth: malformed argon2 params")
			}
			p.threads = uint8(n)
		default:
			return p, fmt.Errorf("auth: unknown argon2 param %q", k)
		}
	}
	if p.memory == 0 || p.time == 0 || p.threads == 0 {
		return p, errors.New("auth: malformed argon2 params")
	}
	if p.salt, err = base64.RawStdEncoding.DecodeString(parts[4]); err != nil {
		return p, errors.New("auth: malformed argon2 salt")
	}
	if p.key, err = base64.RawStdEncoding.DecodeString(parts[5]); err != nil {
		return p, errors.New("auth: malformed argon2 key")
	}
	if len(p.key) == 0 {
		return p, errors.New("auth: malformed argon2 key")
	}
	return p, nil
}

// ConstantTimeEqual compares two strings in constant time.
func ConstantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
