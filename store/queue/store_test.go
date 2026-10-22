package queue_test

import (
	"testing"
	"time"

	"dovetail/rados"
	"dovetail/store/queue"
	"dovetail/store/queue/queuetest"
)

func TestRadosStore(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, now *time.Time) queue.Store {
		c := rados.NewMemoryClient()
		c.Now = func() time.Time { return *now }
		io, _ := c.Pool("index")
		return queue.New(io)
	})
}
