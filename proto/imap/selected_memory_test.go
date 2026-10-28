package imap

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"kittiwake/store/index"
)

// Measure retained session memory, not total allocations made while decoding a
// folder. Both representations consume the same 150k synthetic index records.
func BenchmarkSelectedMemory150k(b *testing.B) {
	for _, compact := range []bool{false, true} {
		b.Run(fmt.Sprintf("compact=%v", compact), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				full := map[uint32]index.Record{}
				small := map[uint32]selectedRecord{}
				for i := uint32(1); i <= 150000; i++ {
					r := index.Record{UID: i, MessageID: uuid.New(), ModSeq: uint64(i), Flags: []string{index.FlagSeen}, Size: 4096, InternalDate: time.Now(), Date: time.Now(), Subject: strings.Repeat(fmt.Sprintf("subject-%d ", i), 100), From: fmt.Sprintf("Sender %d <sender%d@example.org>", i, i)}
					if compact {
						small[i] = compactRecord(r)
					} else {
						full[i] = r
					}
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/150000, "retained-B/message")
				runtime.KeepAlive(full)
				runtime.KeepAlive(small)
			}
		})
	}
}
