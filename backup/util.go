package backup

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"time"
)

func readerOf(b []byte) io.ReadSeeker { return bytes.NewReader(b) }

// newRunID is sortable by time: "20261028T120000Z-1a2b3c4d".
func newRunID(now time.Time) string {
	var r [4]byte
	_, _ = rand.Read(r[:])
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(r[:])
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
