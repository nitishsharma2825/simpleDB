package tx

/*
TODO: fix circular dependencies:
1. Dependency Inversion Principle: Instead of direct dependency on package, define interface in one package and implement them in another
2. A Third shared package which contain common functionalities
*/
import (
	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/concurrency"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/recovery"
)

/*
Provide transaction management for clients
ensuring all txns are serializable, recoverable
and in general satisfy the ACID properties
*/

var nextTxNum = 0
var END_OF_FILE = -1

type Transaction struct {
	rm        *recovery.RecoveryManager
	cm        *concurrency.ConcurrencyManager
	bm        *buffer.Manager
	fm        *file.Manager
	txnum     int
	myBuffers *BufferList
}
