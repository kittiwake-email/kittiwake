package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"kittiwake/backup/crypto"
	"kittiwake/backup/target"
)

// packWriter spools sealed entries into a file and uploads the file as a
// pack object when it reaches the size cap or the run ends. Every entry
// is sealed on its own, so a single entry can be read back with one
// ranged GET.
type packWriter struct {
	ctx     context.Context
	tg      target.Target
	ring    *crypto.Ring
	prefix  string // key prefix of this run's packs, e.g. "mb/<uuid>/<run>"
	cap     int64
	spool   string
	retain  time.Time
	file    *os.File
	size    int64
	part    int
	Keys    []string // uploaded pack keys
	Written int64    // bytes uploaded
}

func newPackWriter(ctx context.Context, tg target.Target, ring *crypto.Ring, prefix, spool string, capBytes int64, retain time.Time) *packWriter {
	return &packWriter{ctx: ctx, tg: tg, ring: ring, prefix: prefix, cap: capBytes, spool: spool, retain: retain}
}

func (w *packWriter) key() string { return fmt.Sprintf("%s.%d.pack", w.prefix, w.part) }

func (w *packWriter) open() error {
	if w.file != nil {
		return nil
	}
	f, err := os.CreateTemp(w.spool, "kittiwake-pack-*")
	if err != nil {
		return err
	}
	w.file, w.size = f, 0
	return nil
}

// Add seals plaintext and appends it, returning its reference.
func (w *packWriter) Add(plaintext []byte) (Ref, error) {
	if err := w.open(); err != nil {
		return Ref{}, err
	}
	sealed, err := w.ring.Seal(plaintext)
	if err != nil {
		return Ref{}, err
	}
	sum := sha256.Sum256(plaintext)
	ref := Ref{Key: w.key(), Offset: w.size, Length: int64(len(sealed)), Size: int64(len(plaintext)), SHA256: hex.EncodeToString(sum[:])}
	if _, err := w.file.Write(sealed); err != nil {
		return Ref{}, err
	}
	w.size += int64(len(sealed))
	if w.size >= w.cap {
		if err := w.flush(); err != nil {
			return Ref{}, err
		}
	}
	return ref, nil
}

// flush uploads the current spool file as a pack and starts the next part.
func (w *packWriter) flush() error {
	if w.file == nil || w.size == 0 {
		return nil
	}
	f := w.file
	w.file = nil
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	key := w.key()
	if err := w.tg.Put(w.ctx, key, f, w.size, target.PutOptions{ContentType: "application/octet-stream", RetainUntil: w.retain}); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	w.Keys = append(w.Keys, key)
	w.Written += w.size
	w.part++
	return nil
}

// Close uploads what is left.
func (w *packWriter) Close() error {
	if w.file != nil && w.size == 0 {
		name := w.file.Name()
		_ = w.file.Close()
		_ = os.Remove(name)
		w.file = nil
		return nil
	}
	return w.flush()
}

// Abort drops the spool and deletes already uploaded packs (a run that
// is being retried).
func (w *packWriter) Abort() {
	if w.file != nil {
		name := w.file.Name()
		_ = w.file.Close()
		_ = os.Remove(name)
		w.file = nil
	}
	for _, k := range w.Keys {
		_ = w.tg.Delete(w.ctx, k)
	}
	w.Keys = nil
}

// readRef fetches and opens one reference, checking its hash.
func readRef(ctx context.Context, tg target.Target, ring *crypto.Ring, ref Ref) ([]byte, error) {
	sealed, err := target.ReadRange(ctx, tg, ref.Key, ref.Offset, ref.Length)
	if err != nil {
		return nil, fmt.Errorf("read %s@%d: %w", ref.Key, ref.Offset, err)
	}
	plain, err := ring.Open(sealed)
	if err != nil {
		return nil, fmt.Errorf("open %s@%d: %w", ref.Key, ref.Offset, err)
	}
	if int64(len(plain)) != ref.Size {
		return nil, fmt.Errorf("%s@%d: size %d, expected %d", ref.Key, ref.Offset, len(plain), ref.Size)
	}
	if ref.SHA256 != "" {
		sum := sha256.Sum256(plain)
		if hex.EncodeToString(sum[:]) != ref.SHA256 {
			return nil, fmt.Errorf("%s@%d: checksum mismatch", ref.Key, ref.Offset)
		}
	}
	return plain, nil
}

// sealTo writes a sealed object under key and returns its reference.
func sealTo(ctx context.Context, tg target.Target, ring *crypto.Ring, key string, plaintext []byte, retain time.Time) (Ref, error) {
	sealed, err := ring.Seal(plaintext)
	if err != nil {
		return Ref{}, err
	}
	if err := tg.Put(ctx, key, readerOf(sealed), int64(len(sealed)), target.PutOptions{ContentType: "application/octet-stream", RetainUntil: retain}); err != nil {
		return Ref{}, fmt.Errorf("upload %s: %w", key, err)
	}
	sum := sha256.Sum256(plaintext)
	return Ref{Key: key, Length: int64(len(sealed)), Size: int64(len(plaintext)), SHA256: hex.EncodeToString(sum[:])}, nil
}
