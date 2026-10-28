package mailbox

import (
	"container/heap"
	"strings"
	"time"

	"kittiwake/store/index"
)

// Keep only the requested sort key and record locator while scanning. Message
// headers, flags and attachment arrays are loaded only for the output page.
type queryItem struct {
	uid     uint32
	fid     string
	text    string
	date    time.Time
	size    int64
	ordinal int
}

func queryKey(r index.Record, fid string, ordinal int, order string) queryItem {
	it := queryItem{uid: r.UID, fid: fid, ordinal: ordinal}
	switch order {
	case SortSubject:
		it.text = strings.ToLower(r.Subject)
	case SortFrom:
		it.text = strings.ToLower(r.From)
	case SortSize:
		it.size = r.Size
	case SortSentAt:
		it.date = r.Date
	default:
		it.date = r.InternalDate
	}
	return it
}
func queryLess(a, b queryItem, q Query) bool {
	var less, greater bool
	switch q.Sort {
	case SortSubject, SortFrom:
		less, greater = a.text < b.text, a.text > b.text
	case SortSize:
		less, greater = a.size < b.size, a.size > b.size
	default:
		less, greater = a.date.Before(b.date), a.date.After(b.date)
	}
	if !less && !greater {
		return a.ordinal < b.ordinal
	}
	if q.Asc {
		return less
	}
	return greater
}

// A max-heap discards matches beyond position+limit while still counting them.
// Negative offsets and unlimited requests retain compact locators instead.
type queryWindow struct {
	items    []queryItem
	q        Query
	capacity int
}

func (w queryWindow) Len() int           { return len(w.items) }
func (w queryWindow) Less(i, j int) bool { return queryLess(w.items[j], w.items[i], w.q) }
func (w queryWindow) Swap(i, j int)      { w.items[i], w.items[j] = w.items[j], w.items[i] }
func (w *queryWindow) Push(x any)        { w.items = append(w.items, x.(queryItem)) }
func (w *queryWindow) Pop() any {
	n := len(w.items) - 1
	x := w.items[n]
	w.items = w.items[:n]
	return x
}
func (w *queryWindow) add(it queryItem) {
	if w.capacity == 0 {
		w.items = append(w.items, it)
		return
	}
	if len(w.items) < w.capacity {
		heap.Push(w, it)
		return
	}
	if queryLess(it, w.items[0], w.q) {
		w.items[0] = it
		heap.Fix(w, 0)
	}
}
