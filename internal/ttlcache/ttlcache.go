// Package ttlcache is a small bounded in-process cache with per-entry age,
// used wherever a RADOS read would otherwise repeat per request (identity
// lookups, verified credentials). It caches results including a caller-
// chosen error (e.g. not-found), so negative lookups are cheap as well.
package ttlcache

import (
	"sync"
	"time"
)

// Cache holds up to Max entries. The zero value is ready to use; a Max of
// 0 means DefaultMax. When full, an arbitrary half of the entries is
// dropped (no LRU bookkeeping: entries are tiny and short-lived).
type Cache[V any] struct {
	Max   int
	mu    sync.Mutex
	items map[string]entry[V]
}

type entry[V any] struct {
	v   V
	err error
	at  time.Time
}

// DefaultMax bounds a cache whose Max is 0.
const DefaultMax = 50000

// Get returns the entry when it is younger than ttl. ttl <= 0 never hits.
func (c *Cache[V]) Get(key string, now time.Time, ttl time.Duration) (v V, err error, ok bool) {
	if ttl <= 0 {
		return v, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.items[key]
	if !found || now.Sub(e.at) >= ttl {
		return v, nil, false
	}
	return e.v, e.err, true
}

// Put stores v (and err, which may be nil) under key at time now.
func (c *Cache[V]) Put(key string, v V, err error, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil {
		c.items = map[string]entry[V]{}
	}
	max := c.Max
	if max <= 0 {
		max = DefaultMax
	}
	if len(c.items) >= max {
		for k := range c.items {
			delete(c.items, k)
			if len(c.items) < max/2 {
				break
			}
		}
	}
	c.items[key] = entry[V]{v: v, err: err, at: now}
}

// Delete drops the given keys.
func (c *Cache[V]) Delete(keys ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.items, k)
	}
}

// Clear drops everything.
func (c *Cache[V]) Clear() {
	c.mu.Lock()
	c.items = nil
	c.mu.Unlock()
}

// Len returns the number of entries (expired ones included until replaced).
func (c *Cache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
