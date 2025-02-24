package tx

/*
TODO: fix circular dependencies:
1. Dependency Inversion Principle: Instead of direct dependency on package, define interface in one package and implement them in another
2. A Third shared package which contain common functionalities
*/
import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

/*
Provide transaction management for clients
ensuring all txns are serializable, recoverable
and in general satisfy the ACID properties
*/

var nextTxNum = 0
var END_OF_FILE = -1

type Transaction struct {
	rm        *RecoveryManager
	cm        *ConcurrencyManager
	bm        *buffer.Manager
	fm        *file.Manager
	txnum     int
	myBuffers *BufferList
}

/*
Creates a new txn and associated recovery and concurrency managers
*/
func NewTransaction(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *Transaction {
	txn := &Transaction{
		fm:        fm,
		bm:        bm,
		txnum:     NextTxNumber(),
		myBuffers: NewBufferList(bm),
	}

	txn.rm = NewRecoveryManager(txn, txn.txnum, lm, bm)
	txn.cm = NewConcurrencyManager()
	return txn
}

/*
Commit the current transaction
Flush all modified buffers (and their log records)
Write and flush a commit record to the log
release all locks and unpin any pinned buffers
*/
func (txn *Transaction) Commit() {
	txn.rm.Commit()
	fmt.Printf("transaction %d committed", txn.txnum)
	txn.cm.Release()
	txn.myBuffers.UnPinAll()
}

/*
Rollback the current transaction
Undo any modified values
flush those buffers
write and flush a rollback record to the log
release all locks and unpin any pinned buffers
*/
func (txn *Transaction) Rollback() {
	txn.rm.Rollback()
	fmt.Printf("transaction %d rolled back", txn.txnum)
	txn.cm.Release()
	txn.myBuffers.UnPinAll()
}

/*
Flush all modified buffers
then go through log, rolling back all uncommitted txns.
Finally, write a quiescent checkpoint record to the log.
This method is called during system startup, before user transactions begin
*/
func (txn *Transaction) Recover() {
	txn.bm.FlushAll(txn.txnum)
	txn.rm.Recover()
}

/*
Pins the specified block
the transaction manages the buffer for the client
*/
func (txn *Transaction) Pin(blockId file.BlockID) {
	txn.myBuffers.Pin(blockId)
}

/*
Unpin the specified block
the transaction looks up the buffer pinned to this block and unpins it
*/
func (txn *Transaction) UnPin(blockId file.BlockID) {
	txn.myBuffers.UnPin(blockId)
}

/*
Return the integer value stored at offset of the block
First Obtain an SLock on the block, then call its buffer to retrieve the value
*/
func (txn *Transaction) GetInt(blockId file.BlockID, offset int) int {
	txn.cm.Slock(blockId)
	buff := txn.myBuffers.GetBuffer(blockId)
	return buff.Contents().GetInt(offset)
}

/*
Return the string value stored at offset of the block
First Obtain an SLock on the block, then call its buffer to retrieve the value
*/
func (txn *Transaction) GetString(blockId file.BlockID, offset int) string {
	txn.cm.Slock(blockId)
	buff := txn.myBuffers.GetBuffer(blockId)
	return buff.Contents().GetString(offset)
}

/*
Store the integer value stored at offset of the block
First obtain an XLock on the block
Read the current value at that offset, puts it into an update log record and write that record to the log
Call the buffer to store the new value passing in the LSN of the log record and txn's id
*/
func (txn *Transaction) SetInt(blockId file.BlockID, offset int, val int, okToLog bool) {
	txn.cm.Xlock(blockId)
	buff := txn.myBuffers.GetBuffer(blockId)
	lsn := -1
	if okToLog {
		lsn = txn.rm.SetInt(buff, offset, val)
	}
	page := buff.Contents()
	page.SetInt(offset, val)
	buff.SetModified(txn.txnum, lsn)
}

/*
Store the string value stored at offset of the block
First obtain an XLock on the block
Read the current value at that offset, puts it into an update log record and write that record to the log
Call the buffer to store the new value passing in the LSN of the log record and txn's id
*/
func (txn *Transaction) SetString(blockId file.BlockID, offset int, val string, okToLog bool) {
	txn.cm.Xlock(blockId)
	buff := txn.myBuffers.GetBuffer(blockId)
	lsn := -1
	if okToLog {
		lsn = txn.rm.SetString(buff, offset, val)
	}
	page := buff.Contents()
	page.SetString(offset, val)
	buff.SetModified(txn.txnum, lsn)
}

/*
returns the number of blocks in the specified file
First obtain an SLock on the "end of the file", before asking the file manager to return the file size
*/
func (txn *Transaction) Size(filename string) int {
	dummyId := file.NewBlockID(filename, END_OF_FILE)
	txn.cm.Slock(dummyId)
	return txn.fm.Length(filename)
}

/*
Append a new block to the end of the specified file and returns a reference to it
First obtain an XLock on the "end of the file" before performing the append
*/
func (txn *Transaction) Append(filename string) file.BlockID {
	dummyId := file.NewBlockID(filename, END_OF_FILE)
	txn.cm.Xlock(dummyId)
	return txn.fm.Append(filename)
}

func (txn *Transaction) BlockSize() int {
	return txn.fm.BlockSize()
}

func (txn *Transaction) AvailableBuffs() int {
	return txn.bm.Available()
}

func NextTxNumber() int {
	nextTxNum++
	return nextTxNum
}
