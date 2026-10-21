package rados

// WriteTx is an atomic write transaction on a single object. Steps are
// applied in order; if any assertion fails, nothing is applied.
//
//	tx := rados.NewWriteTx().CreateExclusive().WriteFull(data).SetXattr("v", []byte("1"))
//	ver, err := ioctx.Write(ctx, oid, tx)
type WriteTx struct {
	steps []txStep
}

type txKind int

const (
	txCreateExclusive txKind = iota
	txAssertExists
	txAssertVersion
	txWriteFull
	txSetXattr
	txRmXattr
	txOmapSet
	txOmapRemove
	txOmapClear
	txRemove
)

type txStep struct {
	kind    txKind
	data    []byte
	name    string
	version uint64
	kv      map[string][]byte
	keys    []string
}

// NewWriteTx returns an empty transaction.
func NewWriteTx() *WriteTx { return &WriteTx{} }

// CreateExclusive fails the transaction with ErrExists if the object exists.
func (t *WriteTx) CreateExclusive() *WriteTx {
	t.steps = append(t.steps, txStep{kind: txCreateExclusive})
	return t
}

// AssertExists fails the transaction with ErrNotFound if the object is absent.
func (t *WriteTx) AssertExists() *WriteTx {
	t.steps = append(t.steps, txStep{kind: txAssertExists})
	return t
}

// AssertVersion fails with ErrVersionMismatch unless the object's current
// version equals v. Use the version returned by Get/Stat/Write.
func (t *WriteTx) AssertVersion(v uint64) *WriteTx {
	t.steps = append(t.steps, txStep{kind: txAssertVersion, version: v})
	return t
}

// WriteFull replaces the object's data.
func (t *WriteTx) WriteFull(data []byte) *WriteTx {
	t.steps = append(t.steps, txStep{kind: txWriteFull, data: data})
	return t
}

// SetXattr sets one xattr.
func (t *WriteTx) SetXattr(name string, value []byte) *WriteTx {
	t.steps = append(t.steps, txStep{kind: txSetXattr, name: name, data: value})
	return t
}

// RmXattr removes one xattr (no error if absent).
func (t *WriteTx) RmXattr(name string) *WriteTx {
	t.steps = append(t.steps, txStep{kind: txRmXattr, name: name})
	return t
}

// OmapSet sets omap entries.
func (t *WriteTx) OmapSet(kv map[string][]byte) *WriteTx {
	t.steps = append(t.steps, txStep{kind: txOmapSet, kv: kv})
	return t
}

// OmapRemove removes omap keys (no error for absent keys).
func (t *WriteTx) OmapRemove(keys ...string) *WriteTx {
	t.steps = append(t.steps, txStep{kind: txOmapRemove, keys: keys})
	return t
}

// OmapClear removes all omap entries.
func (t *WriteTx) OmapClear() *WriteTx {
	t.steps = append(t.steps, txStep{kind: txOmapClear})
	return t
}

// Remove deletes the object.
func (t *WriteTx) Remove() *WriteTx {
	t.steps = append(t.steps, txStep{kind: txRemove})
	return t
}

// Len returns the number of steps.
func (t *WriteTx) Len() int { return len(t.steps) }
