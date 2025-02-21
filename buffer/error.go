package buffer

import "errors"

var ErrBufferAbort = errors.New("client has timed out while waiting for buffers to be assigned")
