package target

import "errors"

// ErrLocked: the object is under retention and cannot be deleted yet.
var ErrLocked = errors.New("backup target: object is locked")
