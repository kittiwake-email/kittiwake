// Package crypto encrypts backup objects before they leave the platform.
//
// A key ring holds one or more pre-shared 256-bit keys identified by a
// small integer; the current key seals new objects, every key in the ring
// opens old ones, so a key can be rotated without re-encrypting history.
// Each sealed object carries a header (magic, key id, random salt, random
// nonce) and an AES-256-GCM ciphertext under a key derived from the ring
// key and the salt with HKDF-SHA256. The fresh salt per object keeps the
// scheme far from the random-nonce bound of GCM however many objects a
// key seals; the header is authenticated as associated data.
//
// Object names must not leak content either: Name derives a keyed name
// (HMAC-SHA256 under the current key) from a content hash, so a provider
// listing the bucket cannot test for the presence of a known file.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"golang.org/x/crypto/hkdf"
)

// Sentinel errors.
var (
	ErrNoKey     = errors.New("backup crypto: no key for key id")
	ErrCorrupt   = errors.New("backup crypto: corrupt or tampered object")
	ErrBadKey    = errors.New("backup crypto: a key is 32 bytes (64 hex characters)")
	ErrNoCurrent = errors.New("backup crypto: the ring has no current key")
)

const (
	magic      = "KWB1"
	saltSize   = 32
	nonceSize  = 12
	headerSize = 4 + 2 + saltSize + nonceSize
	keySize    = 32
)

// Key is one ring entry.
type Key struct {
	ID  uint16
	Key []byte // 32 bytes
}

// Ring is a set of keys with one current key.
type Ring struct {
	keys    map[uint16][]byte
	current uint16
	name    []byte // HMAC key for object names, derived from the current key
}

// ParseKey decodes a hex-encoded 256-bit key.
func ParseKey(s string) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != keySize {
		return nil, ErrBadKey
	}
	return b, nil
}

// NewRing builds a ring. current must be one of the ids.
func NewRing(keys []Key, current uint16) (*Ring, error) {
	r := &Ring{keys: map[uint16][]byte{}}
	for _, k := range keys {
		if len(k.Key) != keySize {
			return nil, ErrBadKey
		}
		if _, dup := r.keys[k.ID]; dup {
			return nil, fmt.Errorf("backup crypto: duplicate key id %d", k.ID)
		}
		r.keys[k.ID] = append([]byte(nil), k.Key...)
	}
	cur, ok := r.keys[current]
	if !ok {
		return nil, ErrNoCurrent
	}
	r.current = current
	r.name = derive(cur, []byte("kittiwake backup name"), nil)
	return r, nil
}

// Current returns the id of the sealing key.
func (r *Ring) Current() uint16 { return r.current }

// IDs lists the key ids in the ring.
func (r *Ring) IDs() []uint16 {
	out := make([]uint16, 0, len(r.keys))
	for id := range r.keys {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// derive runs HKDF-SHA256.
func derive(key, info, salt []byte) []byte {
	out := make([]byte, keySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, key, salt, info), out); err != nil {
		panic(err)
	}
	return out
}

// Name returns the keyed object name for a content hash (hex SHA-256):
// 64 hex characters, deterministic under the current key.
func (r *Ring) Name(sha256hex string) string {
	m := hmac.New(sha256.New, r.name)
	m.Write([]byte(strings.ToLower(sha256hex)))
	return hex.EncodeToString(m.Sum(nil))
}

// Seal encrypts plaintext under the current key. The output is the
// header followed by the GCM ciphertext and tag.
func (r *Ring) Seal(plaintext []byte) ([]byte, error) {
	hdr := make([]byte, headerSize)
	copy(hdr, magic)
	binary.BigEndian.PutUint16(hdr[4:], r.current)
	if _, err := rand.Read(hdr[6:]); err != nil {
		return nil, err
	}
	salt := hdr[6 : 6+saltSize]
	nonce := hdr[6+saltSize:]
	aead, err := r.aead(r.current, salt)
	if err != nil {
		return nil, err
	}
	out := make([]byte, headerSize, headerSize+len(plaintext)+aead.Overhead())
	copy(out, hdr)
	return aead.Seal(out, nonce, plaintext, hdr), nil
}

// Open decrypts a sealed object with whichever ring key sealed it.
func (r *Ring) Open(sealed []byte) ([]byte, error) {
	if len(sealed) < headerSize || string(sealed[:4]) != magic {
		return nil, ErrCorrupt
	}
	hdr := sealed[:headerSize]
	id := binary.BigEndian.Uint16(hdr[4:])
	salt := hdr[6 : 6+saltSize]
	nonce := hdr[6+saltSize:]
	aead, err := r.aead(id, salt)
	if err != nil {
		return nil, err
	}
	out, err := aead.Open(nil, nonce, sealed[headerSize:], hdr)
	if err != nil {
		return nil, ErrCorrupt
	}
	return out, nil
}

// Overhead is the number of bytes Seal adds to a plaintext.
const Overhead = headerSize + 16

func (r *Ring) aead(id uint16, salt []byte) (cipher.AEAD, error) {
	key, ok := r.keys[id]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrNoKey, id)
	}
	block, err := aes.NewCipher(derive(key, []byte("kittiwake backup object"), salt))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// KeyID reports which ring key sealed an object, without opening it.
func KeyID(sealed []byte) (uint16, error) {
	if len(sealed) < headerSize || string(sealed[:4]) != magic {
		return 0, ErrCorrupt
	}
	return binary.BigEndian.Uint16(sealed[4:]), nil
}
