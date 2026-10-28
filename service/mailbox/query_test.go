package mailbox

import (
	"kittiwake/store/index"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

func TestQueryWindowMatchesFullSort(t *testing.T) {
	for _, order := range []string{SortReceivedAt, SortSentAt, SortSize, SortSubject, SortFrom} {
		for _, asc := range []bool{false, true} {
			q := Query{Sort: order, Asc: asc, Position: 31, Limit: 7}
			w := queryWindow{q: q, capacity: q.Position + q.Limit}
			var all []queryItem
			for i := 0; i < 10000; i++ {
				v := rand.IntN(50) // many ties: preserve folder scan order
				r := index.Record{UID: uint32(i + 1), Size: int64(v), Subject: string(rune('a' + v)), From: string(rune('a' + v)), Date: time.Unix(int64(v), 0), InternalDate: time.Unix(int64(v), 0)}
				it := queryKey(r, "folder", i, order)
				all = append(all, it)
				w.add(it)
				if len(w.items) > w.capacity {
					t.Fatal("unbounded query window")
				}
			}
			sort.Slice(all, func(i, j int) bool { return queryLess(all[i], all[j], q) })
			sort.Slice(w.items, func(i, j int) bool { return queryLess(w.items[i], w.items[j], q) })
			for i := range w.items {
				if w.items[i] != all[i] {
					t.Fatalf("%s asc=%v position=%d", order, asc, i)
				}
			}
		}
	}
}
