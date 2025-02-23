package recovery

import (
	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Each transaction has its own recovery manager
*/
type RecoveryManager struct {
	lm    *log.Manager
	bm    *buffer.Manager
	tx    *tx.Transaction
	txnum int
}

func NewRecoveryManager(tx *tx.Transaction, txnum int, lm *log.Manager, bm *buffer.Manager) *RecoveryManager {
	WriteStartRecordToLog(lm, txnum)
	return &RecoveryManager{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txnum: txnum,
	}
}

/*
Write a commit record to the log, and flushes it to disk
Since we will implement only undo-log recovery, no redo-log recovery, we are flushing all buffers too
*/
func (rm *RecoveryManager) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCommitRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

/*
Write a rollback record to the log and flush it to disk
*/
func (rm *RecoveryManager) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteRollbackRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

/*
Recover uncompleted transactions from the log
and then write a quiescent checkpoint record to the log and flush it
*/
func (rm *RecoveryManager) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCheckpointRecordToLog(rm.lm)
	rm.lm.Flush(lsn)
}

/*
Write a setint record to the log and return its lsn
*/
func (rm *RecoveryManager) SetInt(buff *buffer.Buffer, offset int, newVal int) int {
	oldVal := buff.Contents().GetInt(offset)
	blockId := buff.Block()
	return WriteSetIntRecordToLog(rm.lm, rm.txnum, blockId, offset, oldVal)
}

/*
Write a setstring record to the log and return its lsn
*/
func (rm *RecoveryManager) SetString(buff *buffer.Buffer, offset int, newVal string) int {
	oldVal := buff.Contents().GetString(offset)
	blockId := buff.Block()
	return WriteSetStringRecordToLog(rm.lm, rm.txnum, blockId, offset, oldVal)
}

/*
Rollback the transaction by iterating through the log records
until it finds the transaction's START record,
calling undo() for each of the transaction's log records
*/
func (rm *RecoveryManager) doRollback() {
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		buf := iter.Next()
		record := CreateLogRecord(buf)
		if record.TxNumber() == rm.txnum {
			if record.Op() == START {
				return
			}
			record.Undo(rm.tx)
		}
	}
}

/*
Do a complete database recovery
Iterate through the log records
Whenever it finds a log record for an unfinished transaction, calls undo() on that record
The method stops when it encounters a CHECKPOINT record or end of the log
*/
func (rm *RecoveryManager) doRecover() {
	finishedTxns := make(map[int]bool)

	iter := rm.lm.Iterator()
	for iter.HasNext() {
		buf := iter.Next()
		record := CreateLogRecord(buf)
		if record.Op() == CHECKPOINT {
			return
		}

		if record.Op() == COMMIT || record.Op() == ROLLBACK {
			finishedTxns[record.TxNumber()] = true
		} else if !finishedTxns[record.TxNumber()] { // record type is SETINT or SETSTRING
			record.Undo(rm.tx)
		}
	}
}
