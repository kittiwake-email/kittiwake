//go:build integration

package index

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkCephRetentionPurge(b *testing.B) {
	s, _, mb, fid := benchmarkCeph(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < 32; j++ {
			rec, err := s.Append(ctx, mb, fid, Record{MessageID: uuid.New(), Size: 1024}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := s.Remove(ctx, mb, fid, rec.UID, "benchmark", time.Now().Add(-time.Hour)); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		_, keys, _, err := s.ListRetention(ctx, mb, time.Now(), 32)
		if err != nil {
			b.Fatal(err)
		}
		if len(keys) != 32 {
			b.Fatalf("keys: %d", len(keys))
		}
		if err := s.DropRetention(ctx, mb, keys...); err != nil {
			b.Fatal(err)
		}
	}
}
