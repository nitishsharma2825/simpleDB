package tx

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

type StartRecord struct {
	txnum int
}

// creates a log record by reading one other value from the log
func NewStartRecord(p *file.Page) *StartRecord {
	tpos := file.IntBytes
	return &StartRecord{
		txnum: p.GetInt(tpos),
	}
}

func (sr *StartRecord) Op() int {
	return START
}

func (sr *StartRecord) TxNumber() int {
	return sr.txnum
}

func (sr *StartRecord) Undo(*Transaction) {}

func (sr *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", sr.txnum)
}

// write the start record to the log
// contains the START operator, followed by txn id
// returns the LSN of the last log value
func WriteStartRecordToLog(lm *log.Manager, txnum int) int {
	record := make([]byte, 2*file.IntBytes)
	page := file.NewPageWithSlice(record)
	page.SetInt(0, START)
	page.SetInt(file.IntBytes, txnum)
	return lm.Append(record)
}
