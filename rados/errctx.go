package rados

import (
	"context"
	"time"
)

// ErrorContext returns an IOContext whose every operation fails with err.
// Store code resolves a mailbox's pool once per operation; when the
// cluster is unavailable it gets this context and the error surfaces at
// the first RADOS call, without a second error path in every method.
func ErrorContext(err error) IOContext { return errIO{err: err} }

type errIO struct{ err error }

func (e errIO) Namespace(string) IOContext                                   { return e }
func (e errIO) Get(context.Context, string) ([]byte, uint64, error)          { return nil, 0, e.err }
func (e errIO) Stat(context.Context, string) (Stat, error)                   { return Stat{}, e.err }
func (e errIO) GetXattrs(context.Context, string) (map[string][]byte, error) { return nil, e.err }
func (e errIO) OmapGet(context.Context, string, []string) (map[string][]byte, error) {
	return nil, e.err
}
func (e errIO) OmapList(context.Context, string, string, string, int) ([]KV, bool, error) {
	return nil, false, e.err
}
func (e errIO) Write(context.Context, string, *WriteTx) (uint64, error)           { return 0, e.err }
func (e errIO) Delete(context.Context, string) error                              { return e.err }
func (e errIO) List(context.Context, func(string) error) error                    { return e.err }
func (e errIO) Lock(context.Context, string, string, string, time.Duration) error { return e.err }
func (e errIO) Unlock(context.Context, string, string, string) error              { return e.err }
func (e errIO) Watch(context.Context, string) (Watcher, error)                    { return nil, e.err }
func (e errIO) Notify(context.Context, string, []byte) error                      { return e.err }
