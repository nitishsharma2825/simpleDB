package tx

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

type CommitRecord struct {
	txnum int
}

// creates a log record by reading one other value from the log
func NewCommitRecord(p *file.Page) *CommitRecord {
	tpos := file.IntBytes
	return &CommitRecord{
		txnum: p.GetInt(tpos),
	}
}

func (cr *CommitRecord) Op() int {
	return COMMIT
}

func (cr *CommitRecord) TxNumber() int {
	return cr.txnum
}

func (cr *CommitRecord) Undo(*Transaction) {}

func (cr *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", cr.txnum)
}

// write the commit record to the log
// contains the COMMIT operator, followed by txn id
// returns the LSN of the last log value
func WriteCommitRecordToLog(lm *log.Manager, txnum int) int {
	record := make([]byte, 2*file.IntBytes)
	page := file.NewPageWithSlice(record)
	page.SetInt(0, COMMIT)
	page.SetInt(file.IntBytes, txnum)
	return lm.Append(record)
}
