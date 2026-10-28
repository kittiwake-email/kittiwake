package storage

import (
	"math/rand/v2"
	"sync"
	"time"

	"kittiwake/rados"
)

const capacityMaxAge = 2 * time.Minute

// Capacity describes raw cluster headroom, not effective pool quota or IOPS.
// Only placement uses this signal. Existing mailboxes retain their storage.
func (r *Registry) refreshCapacity() {
	r.mu.RLock()
	var clients []*conn
	for _, c := range r.conns {
		if _, ok := c.client.(rados.CapacityProvider); ok {
			clients = append(clients, c)
		}
	}
	r.mu.RUnlock()
	var wg sync.WaitGroup
	slots := make(chan struct{}, 4)
	for _, c := range clients {
		wg.Add(1)
		go func(c *conn) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			cap, err := c.client.(rados.CapacityProvider).Capacity()
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.conns[c.rec.ID] != c {
				return
			}
			c.capacity = cap
			c.capacityErr = err
			c.capacityAt = r.Now()
		}(c)
	}
	wg.Wait()
}
func (r *Registry) placementWeight(c *conn) float64 {
	if c.client == nil || !c.rec.Placement {
		return 0
	}
	if _, ok := c.client.(rados.CapacityProvider); !ok {
		return 1
	} // development clients
	if c.capacityErr != nil || c.capacityAt.IsZero() || r.Now().Sub(c.capacityAt) > capacityMaxAge {
		return 0
	}
	if c.capacity.TotalBytes == 0 || c.capacity.AvailableBytes > c.capacity.TotalBytes {
		return 0
	}
	headroom := float64(c.capacity.AvailableBytes) - float64(c.capacity.TotalBytes)*r.PlacementReserve
	if headroom <= 0 {
		return 0
	}
	return headroom
}
func (r *Registry) pickWeighted(ids []int64) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var total float64
	for _, id := range ids {
		if c := r.conns[id]; c != nil {
			total += r.placementWeight(c)
		}
	}
	if total <= 0 {
		return 0, ErrNoPlacement
	}
	pick := rand.Float64() * total
	var last int64
	for _, id := range ids {
		if c := r.conns[id]; c != nil {
			w := r.placementWeight(c)
			if w <= 0 {
				continue
			}
			last = id
			pick -= w
			if pick < 0 {
				return id, nil
			}
		}
	}
	return last, nil
}
