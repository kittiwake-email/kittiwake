package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"math"
	"net/url"
	"strings"
	"time"
)

// TOTPParams are the RFC 6238 parameters.
type TOTPParams struct {
	Digits    int
	Period    int
	Algorithm string // SHA1 (default), SHA256, SHA512
}

// DefaultTOTPParams returns 6 digits, 30 seconds, SHA1.
func DefaultTOTPParams() TOTPParams { return TOTPParams{Digits: 6, Period: 30, Algorithm: "SHA1"} }

func (p TOTPParams) normalized() TOTPParams {
	if p.Digits <= 0 {
		p.Digits = 6
	}
	if p.Period <= 0 {
		p.Period = 30
	}
	p.Algorithm = strings.ToUpper(p.Algorithm)
	if p.Algorithm == "" {
		p.Algorithm = "SHA1"
	}
	return p
}

func (p TOTPParams) hashFunc() func() hash.Hash {
	switch p.Algorithm {
	case "SHA256":
		return sha256.New
	case "SHA512":
		return sha512.New
	default:
		return sha1.New
	}
}

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// NewTOTPSecret returns a 20-byte random secret as unpadded upper-case base32.
func NewTOTPSecret() (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return b32.EncodeToString(b), nil
}

func decodeSecret(secret string) ([]byte, error) {
	s := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(secret, " ", ""), "-", ""))
	s = strings.TrimRight(s, "=")
	return b32.DecodeString(s)
}

// TOTPProvisioningURI returns the otpauth:// URI for authenticator apps.
func TOTPProvisioningURI(issuer, account, secret string, p TOTPParams) string {
	p = p.normalized()
	q := url.Values{}
	q.Set("secret", secret)
	q.Set("issuer", issuer)
	q.Set("algorithm", p.Algorithm)
	q.Set("digits", fmt.Sprint(p.Digits))
	q.Set("period", fmt.Sprint(p.Period))
	label := url.PathEscape(issuer + ":" + account)
	return "otpauth://totp/" + label + "?" + q.Encode()
}

// hotp computes the RFC 4226 value for a counter.
func hotp(key []byte, counter uint64, p TOTPParams) string {
	var msg [8]byte
	for i := 7; i >= 0; i-- {
		msg[i] = byte(counter)
		counter >>= 8
	}
	mac := hmac.New(p.hashFunc(), key)
	mac.Write(msg[:])
	sum := mac.Sum(nil)
	offset := sum[len(sum)-1] & 0x0f
	code := (uint32(sum[offset])&0x7f)<<24 | uint32(sum[offset+1])<<16 | uint32(sum[offset+2])<<8 | uint32(sum[offset+3])
	mod := uint32(math.Pow10(p.Digits))
	return fmt.Sprintf("%0*d", p.Digits, code%mod)
}

// VerifyTOTP checks code at time `at`, allowing +/- skew steps. It returns
// the matched time step so callers can reject replays (step <= last used).
func VerifyTOTP(secret, code string, at time.Time, p TOTPParams, skew int) (bool, int64) {
	p = p.normalized()
	key, err := decodeSecret(secret)
	if err != nil || len(key) == 0 {
		return false, 0
	}
	code = strings.ReplaceAll(strings.TrimSpace(code), " ", "")
	if len(code) != p.Digits {
		return false, 0
	}
	if skew < 0 {
		skew = 0
	}
	step := at.Unix() / int64(p.Period)
	// Always evaluate the whole window so timing does not reveal the offset.
	matched := int64(0)
	ok := false
	for d := -int64(skew); d <= int64(skew); d++ {
		s := step + d
		if s < 0 {
			continue
		}
		if ConstantTimeEqual(hotp(key, uint64(s), p), code) && !ok {
			ok = true
			matched = s
		}
	}
	return ok, matched
}

// Recovery codes.

const recoveryAlphabet = "abcdefghjkmnpqrstuvwxyz23456789"

// NewRecoveryCodes returns n codes "xxxxx-xxxxx" and their {SSHA256} hashes.
func NewRecoveryCodes(n int) (codes []string, hashes []string, err error) {
	if n <= 0 {
		return nil, nil, errors.New("auth: n must be positive")
	}
	for i := 0; i < n; i++ {
		raw := make([]byte, 10)
		if _, err := rand.Read(raw); err != nil {
			return nil, nil, err
		}
		var sb strings.Builder
		for j, b := range raw {
			if j == 5 {
				sb.WriteByte('-')
			}
			sb.WriteByte(recoveryAlphabet[int(b)%len(recoveryAlphabet)])
		}
		code := sb.String()
		h, err := ssha256(normalizeRecovery(code))
		if err != nil {
			return nil, nil, err
		}
		codes = append(codes, code)
		hashes = append(hashes, h)
	}
	return codes, hashes, nil
}

func ssha256(plain string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(plain))
	h.Write(salt)
	return "{SSHA256}" + base64.StdEncoding.EncodeToString(append(h.Sum(nil), salt...)), nil
}

func normalizeRecovery(code string) string {
	code = strings.ToLower(strings.TrimSpace(code))
	code = strings.ReplaceAll(code, "-", "")
	code = strings.ReplaceAll(code, " ", "")
	return code
}

// VerifyRecoveryCode returns the index of the hash matching code, or -1.
func VerifyRecoveryCode(hashes []string, code string) int {
	norm := normalizeRecovery(code)
	found := -1
	for i, h := range hashes {
		ok, err := VerifyPassword(h, norm)
		if err == nil && ok && found < 0 {
			found = i
		}
	}
	return found
}
