package auth

import (
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
)

// SHA-crypt (Ulrich Drepper's $5$/$6$ scheme, as in glibc crypt(3)).

const (
	shaCryptRoundsDefault = 5000
	shaCryptRoundsMin     = 1000
	shaCryptRoundsMax     = 999999999
	shaCryptSaltMax       = 16
	itoa64                = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// verifyShaCrypt verifies plain against "$5$..." or "$6$..." hashes.
func verifyShaCrypt(h, plain string) (bool, error) {
	if !strings.HasPrefix(h, "$5$") && !strings.HasPrefix(h, "$6$") {
		return false, errors.New("auth: malformed sha-crypt hash")
	}
	id := h[1]
	rest := h[3:]
	rounds := shaCryptRoundsDefault
	customRounds := false
	if strings.HasPrefix(rest, "rounds=") {
		i := strings.IndexByte(rest, '$')
		if i < 0 {
			return false, errors.New("auth: malformed sha-crypt rounds")
		}
		n, err := strconv.Atoi(rest[len("rounds="):i])
		if err != nil {
			return false, errors.New("auth: malformed sha-crypt rounds")
		}
		rounds = n
		customRounds = true
		rest = rest[i+1:]
	}
	i := strings.IndexByte(rest, '$')
	if i < 0 {
		return false, errors.New("auth: malformed sha-crypt hash")
	}
	salt := rest[:i]
	want := rest[i+1:]
	got := shaCrypt(id, plain, salt, rounds, customRounds)
	// shaCrypt returns the full string; compare only the encoded part
	// (salt/rounds normalisation is reflected in both).
	j := strings.LastIndexByte(got, '$')
	return ConstantTimeEqual(got[j+1:], want), nil
}

// shaCrypt computes the full "$id$[rounds=N$]salt$hash" string.
func shaCrypt(id byte, key, salt string, rounds int, customRounds bool) string {
	var newHash func() hash.Hash
	if id == '5' {
		newHash = sha256.New
	} else {
		newHash = sha512.New
	}
	if rounds < shaCryptRoundsMin {
		rounds = shaCryptRoundsMin
	}
	if rounds > shaCryptRoundsMax {
		rounds = shaCryptRoundsMax
	}
	if len(salt) > shaCryptSaltMax {
		salt = salt[:shaCryptSaltMax]
	}
	k := []byte(key)
	s := []byte(salt)
	hashLen := newHash().Size()

	// Digest B.
	b := newHash()
	b.Write(k)
	b.Write(s)
	b.Write(k)
	bsum := b.Sum(nil)

	// Digest A.
	a := newHash()
	a.Write(k)
	a.Write(s)
	cnt := len(k)
	for ; cnt > hashLen; cnt -= hashLen {
		a.Write(bsum)
	}
	a.Write(bsum[:cnt])
	for cnt = len(k); cnt > 0; cnt >>= 1 {
		if cnt&1 != 0 {
			a.Write(bsum)
		} else {
			a.Write(k)
		}
	}
	asum := a.Sum(nil)

	// DP -> P.
	dp := newHash()
	for i := 0; i < len(k); i++ {
		dp.Write(k)
	}
	dpsum := dp.Sum(nil)
	p := make([]byte, 0, len(k))
	for cnt = len(k); cnt >= hashLen; cnt -= hashLen {
		p = append(p, dpsum...)
	}
	p = append(p, dpsum[:cnt]...)

	// DS -> S.
	ds := newHash()
	for i := 0; i < 16+int(asum[0]); i++ {
		ds.Write(s)
	}
	dssum := ds.Sum(nil)
	sq := make([]byte, 0, len(s))
	for cnt = len(s); cnt >= hashLen; cnt -= hashLen {
		sq = append(sq, dssum...)
	}
	sq = append(sq, dssum[:cnt]...)

	// Rounds.
	c := asum
	for r := 0; r < rounds; r++ {
		h := newHash()
		if r&1 != 0 {
			h.Write(p)
		} else {
			h.Write(c)
		}
		if r%3 != 0 {
			h.Write(sq)
		}
		if r%7 != 0 {
			h.Write(p)
		}
		if r&1 != 0 {
			h.Write(c)
		} else {
			h.Write(p)
		}
		c = h.Sum(nil)
	}

	var sb strings.Builder
	sb.WriteString("$")
	sb.WriteByte(id)
	sb.WriteString("$")
	if customRounds {
		fmt.Fprintf(&sb, "rounds=%d$", rounds)
	}
	sb.WriteString(salt)
	sb.WriteString("$")
	if id == '5' {
		sb.WriteString(encodeSha256Crypt(c))
	} else {
		sb.WriteString(encodeSha512Crypt(c))
	}
	return sb.String()
}

func b64From24bit(sb *strings.Builder, b2, b1, b0 byte, n int) {
	w := uint32(b2)<<16 | uint32(b1)<<8 | uint32(b0)
	for i := 0; i < n; i++ {
		sb.WriteByte(itoa64[w&0x3f])
		w >>= 6
	}
}

func encodeSha256Crypt(c []byte) string {
	var sb strings.Builder
	triples := [][3]int{{0, 10, 20}, {21, 1, 11}, {12, 22, 2}, {3, 13, 23}, {24, 4, 14}, {15, 25, 5}, {6, 16, 26}, {27, 7, 17}, {18, 28, 8}, {9, 19, 29}}
	for _, t := range triples {
		b64From24bit(&sb, c[t[0]], c[t[1]], c[t[2]], 4)
	}
	b64From24bit(&sb, 0, c[31], c[30], 3)
	return sb.String()
}

func encodeSha512Crypt(c []byte) string {
	var sb strings.Builder
	triples := [][3]int{{0, 21, 42}, {22, 43, 1}, {44, 2, 23}, {3, 24, 45}, {25, 46, 4}, {47, 5, 26}, {6, 27, 48}, {28, 49, 7}, {50, 8, 29}, {9, 30, 51}, {31, 52, 10}, {53, 11, 32}, {12, 33, 54}, {34, 55, 13}, {56, 14, 35}, {15, 36, 57}, {37, 58, 16}, {59, 17, 38}, {18, 39, 60}, {40, 61, 19}, {62, 20, 41}}
	for _, t := range triples {
		b64From24bit(&sb, c[t[0]], c[t[1]], c[t[2]], 4)
	}
	b64From24bit(&sb, 0, 0, c[63], 2)
	return sb.String()
}
