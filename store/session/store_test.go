package session_test

import (
	"testing"

	"dovetail/rados"
	"dovetail/store/session"
	"dovetail/store/session/sessiontest"
)

func TestRadosStore(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) session.Store {
		c := rados.NewMemoryClient()
		io, _ := c.Pool("index")
		return session.New(io)
	})
}
