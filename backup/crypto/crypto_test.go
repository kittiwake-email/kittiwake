package crypto

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func ring(t *testing.T, ids ...uint16) *Ring {
	t.Helper()
	var keys []Key
	for _, id := range ids {
		k := bytes.Repeat([]byte{byte(id)}, 32)
		keys = append(keys, Key{ID: id, Key: k})
	}
	r, err := NewRing(keys, ids[len(ids)-1])
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSealOpenRoundTrip(t *testing.T) {
	r := ring(t, 1)
	for _, n := range []int{0, 1, 17, 4096, 1 << 20} {
		pt := bytes.Repeat([]byte("x"), n)
		ct, err := r.Seal(pt)
		if err != nil {
			t.Fatal(err)
		}
		if len(ct) != n+Overhead {
			t.Fatalf("overhead: %d != %d", len(ct), n+Overhead)
		}
		got, err := r.Open(ct)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, pt) {
			t.Fatal("round trip mismatch")
		}
	}
}

func TestTamperAndKeyMismatch(t *testing.T) {
	r := ring(t, 1)
	ct, _ := r.Seal([]byte("secret"))
	bad := append([]byte(nil), ct...)
	bad[len(bad)-1] ^= 1
	if _, err := r.Open(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("tampered: %v", err)
	}
	hdr := append([]byte(nil), ct...)
	hdr[10] ^= 1 // salt is associated data
	if _, err := r.Open(hdr); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("header tampered: %v", err)
	}
	other := ring(t, 2)
	if _, err := other.Open(ct); !errors.Is(err, ErrNoKey) {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := r.Open([]byte("nope")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short: %v", err)
	}
}

func TestRotation(t *testing.T) {
	old := ring(t, 1)
	ct, _ := old.Seal([]byte("history"))
	rotated := ring(t, 1, 2)
	if rotated.Current() != 2 {
		t.Fatal("current")
	}
	if id, _ := KeyID(ct); id != 1 {
		t.Fatalf("key id %d", id)
	}
	if got, err := rotated.Open(ct); err != nil || string(got) != "history" {
		t.Fatalf("open with rotated ring: %v", err)
	}
	ct2, _ := rotated.Seal([]byte("new"))
	if id, _ := KeyID(ct2); id != 2 {
		t.Fatalf("new objects use the current key, got %d", id)
	}
	if _, err := old.Open(ct2); !errors.Is(err, ErrNoKey) {
		t.Fatal("old ring must not open new objects")
	}
	if _, err := NewRing([]Key{{ID: 1, Key: make([]byte, 32)}}, 9); !errors.Is(err, ErrNoCurrent) {
		t.Fatal("current must be in the ring")
	}
	if _, err := NewRing([]Key{{ID: 1, Key: make([]byte, 31)}}, 1); !errors.Is(err, ErrBadKey) {
		t.Fatal("key length")
	}
}

func TestNames(t *testing.T) {
	r := ring(t, 1)
	a := r.Name(strings.Repeat("ab", 32))
	b := r.Name(strings.ToUpper(strings.Repeat("ab", 32)))
	if a != b || len(a) != 64 {
		t.Fatal("names are deterministic and case-insensitive on the hash")
	}
	if a == strings.Repeat("ab", 32) || ring(t, 2).Name(strings.Repeat("ab", 32)) == a {
		t.Fatal("names are keyed")
	}
	if _, err := ParseKey("zz"); !errors.Is(err, ErrBadKey) {
		t.Fatal("parse")
	}
	if k, err := ParseKey(strings.Repeat("0f", 32)); err != nil || len(k) != 32 {
		t.Fatal("parse ok")
	}
}
