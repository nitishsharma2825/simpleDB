package recovery

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type RollbackRecord struct {
	txnum int
}

// rreates a log record by reading one other value from the log
func NewRollbackRecord(p *file.Page) *RollbackRecord {
	tpos := file.IntBytes
	return &RollbackRecord{
		txnum: p.GetInt(tpos),
	}
}

func (rr *RollbackRecord) Op() int {
	return ROLLBACK
}

func (rr *RollbackRecord) TxNumber() int {
	return rr.txnum
}

func (rr *RollbackRecord) Undo(*tx.Transaction) {}

func (rr *RollbackRecord) ToString() string {
	return fmt.Sprintf("<Rollback %d>", rr.txnum)
}

// write the Rollback record to the log
// contains the Rollback operator, followed by txn id
// returns the LSN of the last log value
func WriteRollbackRecordToLog(lm *log.Manager, txnum int) int {
	record := make([]byte, 2*file.IntBytes)
	page := file.NewPageWithSlice(record)
	page.SetInt(0, ROLLBACK)
	page.SetInt(file.IntBytes, txnum)
	return lm.Append(record)
}
