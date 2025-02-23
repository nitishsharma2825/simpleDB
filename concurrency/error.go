package concurrency

import "errors"

var ErrLockAbort = errors.New("transaction needs to abort because a lock could not be obtained")
