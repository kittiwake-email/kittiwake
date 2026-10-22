package auth

import (
	"crypto/ed25519"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

func TestHashAndVerifyArgon2id(t *testing.T) {
	h, err := HashPassword("correct horse")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "{ARGON2ID}$argon2id$v=19$m=65536,t=1,p=4$") {
		t.Fatalf("unexpected format %s", h)
	}
	if Scheme(h) != "ARGON2ID" {
		t.Fatalf("scheme %q", Scheme(h))
	}
	ok, err := VerifyPassword(h, "correct horse")
	if err != nil || !ok {
		t.Fatalf("verify: %v %v", ok, err)
	}
	ok, err = VerifyPassword(h, "wrong")
	if err != nil || ok {
		t.Fatalf("mismatch: %v %v", ok, err)
	}
	if NeedsRehash(h) {
		t.Fatal("fresh argon2id should not need rehash")
	}
	// Different params -> rehash.
	weak := "{ARGON2ID}$argon2id$v=19$m=1024,t=1,p=1$c2FsdHNhbHRzYWx0c2Fs$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if !NeedsRehash(weak) {
		t.Fatal("weak params should need rehash")
	}
	// Variant mismatch.
	if _, err := VerifyPassword("{ARGON2I}"+h[len("{ARGON2ID}"):], "x"); err == nil {
		t.Fatal("expected variant mismatch error")
	}
}

func TestVerifySchemes(t *testing.T) {
	plain := "Hello world!"
	bc, _ := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.MinCost)
	sha1sum := sha1.Sum([]byte(plain))
	sha256sum := sha256.Sum256([]byte(plain))
	salt := []byte("0123456789abcdef")
	s1 := sha1.New()
	s1.Write([]byte(plain))
	s1.Write(salt)
	ssha := base64.StdEncoding.EncodeToString(append(s1.Sum(nil), salt...))
	s2 := sha256.New()
	s2.Write([]byte(plain))
	s2.Write(salt)
	ssha256 := base64.StdEncoding.EncodeToString(append(s2.Sum(nil), salt...))

	cases := []struct {
		name string
		hash string
	}{
		{"BLF-CRYPT", "{BLF-CRYPT}" + string(bc)},
		{"BCRYPT", "{BCRYPT}" + string(bc)},
		{"bcrypt lowercase scheme", "{bcrypt}" + string(bc)},
		{"SHA512-CRYPT", "{SHA512-CRYPT}$6$saltstring$svn8UoSVapNtMuq1ukKS4tPQd8iKwSMHWjl/O817G3uBnIFNjnQJuesI68u4OTLiBFdcbYEdFCoEOfaS35inz1"},
		{"SHA512-CRYPT rounds", "{SHA512-CRYPT}$6$rounds=10000$saltstringsaltstring$OW1/O6BYHV6BcXZu8QVeXbDWra3Oeqh0sbHbbMCVNSnCM/UrjmM0Dp8vOuZeHBy/YTBmSK6H9qs/y3RnOaw5v."},
		{"SHA256-CRYPT", "{SHA256-CRYPT}$5$saltstring$5B8vYYiY.CVt1RlTTf8KbXBH3hsxY/GNooZaBBGWEc5"},
		{"SHA256-CRYPT rounds", "{SHA256-CRYPT}$5$rounds=10000$saltstringsaltstring$3xv.VbSHBb41AL9AvLeujZkZRBAwqFMz2.opqey6IcA"},
		{"SSHA", "{SSHA}" + ssha},
		{"SSHA256", "{SSHA256}" + ssha256},
		{"SHA", "{SHA}" + base64.StdEncoding.EncodeToString(sha1sum[:])},
		{"SHA256", "{SHA256}" + base64.StdEncoding.EncodeToString(sha256sum[:])},
		{"SHA256.HEX", "{SHA256.HEX}" + hex.EncodeToString(sha256sum[:])},
		{"SHA.HEX", "{SHA.HEX}" + hex.EncodeToString(sha1sum[:])},
		{"PLAIN", "{PLAIN}" + plain},
		{"CLEARTEXT", "{CLEARTEXT}" + plain},
		{"no scheme", plain},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, err := VerifyPassword(c.hash, plain)
			if err != nil || !ok {
				t.Fatalf("verify %s: ok=%v err=%v", c.hash, ok, err)
			}
			ok, err = VerifyPassword(c.hash, plain+"x")
			if err != nil || ok {
				t.Fatalf("mismatch %s: ok=%v err=%v", c.hash, ok, err)
			}
			if !NeedsRehash(c.hash) {
				t.Fatalf("%s should need rehash", c.name)
			}
		})
	}
}

func TestVerifyErrors(t *testing.T) {
	if _, err := VerifyPassword("{MD5-CRYPT}$1$abc$def", "x"); !errors.Is(err, ErrUnknownScheme) {
		t.Fatalf("unknown scheme: %v", err)
	}
	if _, err := VerifyPassword("$6$salt$hash", "x"); !errors.Is(err, ErrUnknownScheme) {
		t.Fatalf("unprefixed with $: %v", err)
	}
	for _, bad := range []string{
		"{ARGON2ID}$argon2id$v=19$m=65536,t=1,p=4$salt",
		"{ARGON2ID}$argon2id$v=19$m=0,t=1,p=4$c2FsdA$aGFzaA",
		"{BLF-CRYPT}not-bcrypt",
		"{SHA512-CRYPT}$6$nodollar",
		"{SHA512-CRYPT}$6$rounds=abc$salt$x",
		"{SSHA}!!!",
		"{SSHA}" + base64.StdEncoding.EncodeToString([]byte("short")),
		"{SHA256}" + base64.StdEncoding.EncodeToString([]byte("wronglen")),
		"{SHA256.HEX}zz",
	} {
		if _, err := VerifyPassword(bad, "x"); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
	if Scheme("nope") != "" || Scheme("{x") != "" {
		t.Fatal("Scheme on unprefixed")
	}
}

func TestShaCryptRoundsClamp(t *testing.T) {
	// rounds below the minimum are clamped to 1000 and still verify.
	h := shaCrypt('5', "pw", "salt", 10, true)
	if !strings.HasPrefix(h, "$5$rounds=1000$salt$") {
		t.Fatalf("clamp: %s", h)
	}
	ok, err := VerifyPassword("{SHA256-CRYPT}"+h, "pw")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Salt longer than 16 chars is truncated.
	h = shaCrypt('6', "pw", "abcdefghijklmnopqrstuvwxyz", 5000, false)
	if !strings.HasPrefix(h, "$6$abcdefghijklmnop$") {
		t.Fatalf("salt trunc: %s", h)
	}
}

func TestTOTPVectors(t *testing.T) {
	// RFC 6238 Appendix B: 8 digits, T0=0, step 30; secrets per algorithm.
	cases := []struct {
		alg    string
		secret string
		at     int64
		want   string
	}{
		{"SHA1", "12345678901234567890", 59, "94287082"},
		{"SHA256", "12345678901234567890123456789012", 59, "46119246"},
		{"SHA512", "1234567890123456789012345678901234567890123456789012345678901234", 59, "90693936"},
		{"SHA1", "12345678901234567890", 1111111109, "07081804"},
		{"SHA1", "12345678901234567890", 1234567890, "89005924"},
		{"SHA256", "12345678901234567890123456789012", 20000000000, "77737706"},
	}
	for _, c := range cases {
		p := TOTPParams{Digits: 8, Period: 30, Algorithm: c.alg}
		secret := b32.EncodeToString([]byte(c.secret))
		ok, step := VerifyTOTP(secret, c.want, time.Unix(c.at, 0), p, 0)
		if !ok || step != c.at/30 {
			t.Errorf("%s@%d: ok=%v step=%d", c.alg, c.at, ok, step)
		}
	}
}

func TestTOTPSkewAndFormat(t *testing.T) {
	secret, err := NewTOTPSecret()
	if err != nil || len(secret) != 32 || secret != strings.ToUpper(secret) || strings.Contains(secret, "=") {
		t.Fatalf("secret %q %v", secret, err)
	}
	p := DefaultTOTPParams()
	key, _ := decodeSecret(secret)
	now := time.Unix(1_700_000_000, 0)
	step := now.Unix() / 30
	prev := hotp(key, uint64(step-1), p)
	cur := hotp(key, uint64(step), p)
	far := hotp(key, uint64(step+3), p)

	if ok, s := VerifyTOTP(secret, cur, now, p, 1); !ok || s != step {
		t.Fatal("current")
	}
	if ok, s := VerifyTOTP(secret, prev, now, p, 1); !ok || s != step-1 {
		t.Fatal("previous within skew")
	}
	if ok, _ := VerifyTOTP(secret, prev, now, p, 0); ok && prev != cur {
		t.Fatal("previous outside skew")
	}
	if ok, _ := VerifyTOTP(secret, far, now, p, 1); ok && far != cur && far != prev {
		t.Fatal("far outside skew")
	}
	if ok, _ := VerifyTOTP(secret, "12345", now, p, 1); ok {
		t.Fatal("wrong length accepted")
	}
	if ok, _ := VerifyTOTP("not base32!!", cur, now, p, 1); ok {
		t.Fatal("bad secret accepted")
	}
	// Lower-case / spaced secret is normalised.
	if ok, _ := VerifyTOTP(strings.ToLower(secret[:8])+" "+secret[8:], cur, now, p, 0); !ok {
		t.Fatal("normalised secret")
	}
	uri := TOTPProvisioningURI("Kittiwake Mail", "wido@example.org", secret, p)
	if !strings.HasPrefix(uri, "otpauth://totp/Kittiwake%20Mail:wido@example.org?") || !strings.Contains(uri, "secret="+secret) || !strings.Contains(uri, "issuer=Kittiwake+Mail") {
		t.Fatalf("uri %s", uri)
	}
}

func TestRecoveryCodes(t *testing.T) {
	codes, hashes, err := NewRecoveryCodes(5)
	if err != nil || len(codes) != 5 || len(hashes) != 5 {
		t.Fatal(err)
	}
	for i, c := range codes {
		if len(c) != 11 || c[5] != '-' {
			t.Fatalf("format %q", c)
		}
		if !strings.HasPrefix(hashes[i], "{SSHA256}") {
			t.Fatalf("hash %q", hashes[i])
		}
		if VerifyRecoveryCode(hashes, c) != i {
			t.Fatalf("code %d not matched", i)
		}
		if VerifyRecoveryCode(hashes, "  "+strings.ToUpper(strings.ReplaceAll(c, "-", " "))+" ") != i {
			t.Fatalf("normalised code %d not matched", i)
		}
	}
	if VerifyRecoveryCode(hashes, "aaaaa-aaaaa") != -1 {
		t.Fatal("bogus code matched")
	}
	if _, _, err := NewRecoveryCodes(0); err == nil {
		t.Fatal("n=0")
	}
}

func TestJWTRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	key, err := NewSigningKey(now)
	if err != nil || len(key.ID) != 16 {
		t.Fatal(err)
	}
	// JSON round trip of the key.
	b, _ := json.Marshal(key)
	var key2 SigningKey
	if err := json.Unmarshal(b, &key2); err != nil || !key2.Public.Equal(key.Public) || !key2.Private.Equal(key.Private) {
		t.Fatal("key json round trip")
	}

	c := Claims{
		AccountID: uuid.New(), MailboxID: uuid.New(), Username: "wido@example.org",
		Roles: []string{"read", "write"}, SessionID: "sess1", Issuer: "kittiwake",
		IssuedAt: now, ExpiresAt: now.Add(15 * time.Minute),
	}
	tok, err := SignAccessToken(key, c)
	if err != nil {
		t.Fatal(err)
	}
	keyFor := func(kid string) (ed25519.PublicKey, bool) {
		if kid == key.ID {
			return key.Public, true
		}
		return nil, false
	}
	got, err := VerifyAccessToken(tok, keyFor, "kittiwake", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountID != c.AccountID || got.MailboxID != c.MailboxID || got.Username != c.Username || got.SessionID != "sess1" || len(got.Roles) != 2 || !got.ExpiresAt.Equal(c.ExpiresAt) || !got.IssuedAt.Equal(now) {
		t.Fatalf("claims %+v", got)
	}

	// Admin token without mailbox.
	adm := c
	adm.MailboxID = uuid.Nil
	atok, _ := SignAccessToken(key, adm)
	parts := strings.Split(atok, ".")
	pb, _ := b64url.DecodeString(parts[1])
	if strings.Contains(string(pb), "mbx") {
		t.Fatal("mbx should be omitted")
	}
	g2, err := VerifyAccessToken(atok, keyFor, "kittiwake", now)
	if err != nil || g2.MailboxID != uuid.Nil {
		t.Fatal(err)
	}

	// Expired.
	if _, err := VerifyAccessToken(tok, keyFor, "kittiwake", c.ExpiresAt); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expiry: %v", err)
	}
	// Issuer mismatch.
	if _, err := VerifyAccessToken(tok, keyFor, "other", now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("issuer: %v", err)
	}
	// Unknown kid.
	if _, err := VerifyAccessToken(tok, func(string) (ed25519.PublicKey, bool) { return nil, false }, "", now); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("kid: %v", err)
	}
	// Tampered payload.
	tampered := strings.Replace(string(pb), "wido", "mallory", 1)
	bad := parts[0] + "." + b64url.EncodeToString([]byte(tampered)) + "." + parts[2]
	if _, err := VerifyAccessToken(bad, keyFor, "", now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("tamper: %v", err)
	}
	// alg=none.
	hdr := b64url.EncodeToString([]byte(`{"alg":"none","typ":"JWT","kid":"` + key.ID + `"}`))
	none := hdr + "." + parts[1] + "."
	if _, err := VerifyAccessToken(none, keyFor, "", now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("none: %v", err)
	}
	// Wrong key.
	other, _ := NewSigningKey(now)
	if _, err := VerifyAccessToken(tok, func(string) (ed25519.PublicKey, bool) { return other.Public, true }, "", now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("wrong key: %v", err)
	}
	// Garbage.
	if _, err := VerifyAccessToken("a.b", keyFor, "", now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatal("garbage")
	}
	// Incomplete claims refused.
	if _, err := SignAccessToken(key, Claims{}); err == nil {
		t.Fatal("empty claims signed")
	}
}

func TestRefreshToken(t *testing.T) {
	plain, hash, err := NewRefreshToken()
	if err != nil || !strings.HasPrefix(plain, "kwr_") || len(hash) != 64 {
		t.Fatal(plain, hash, err)
	}
	if HashRefreshToken(plain) != hash {
		t.Fatal("hash mismatch")
	}
	p2, h2, _ := NewRefreshToken()
	if p2 == plain || h2 == hash {
		t.Fatal("not random")
	}
	if !ConstantTimeEqual("a", "a") || ConstantTimeEqual("a", "b") {
		t.Fatal("cte")
	}
}
