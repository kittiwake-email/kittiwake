package queue_test

import (
	"testing"
	"time"

	"kittiwake/rados"
	"kittiwake/store/queue"
	"kittiwake/store/queue/queuetest"
)

func TestRadosStore(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, now *time.Time) queue.Store {
		c := rados.NewMemoryClient()
		c.Now = func() time.Time { return *now }
		io, _ := c.Pool("index")
		return queue.New(io)
	})
}
