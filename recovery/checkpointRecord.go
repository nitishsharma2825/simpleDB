package recovery

/*
TODO: fix circular dependencies:
1. Dependency Inversion Principle: Instead of direct dependency on package, define interface in one package and implement them in another
2. A Third shared package which contain common functionalities
*/
import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type CheckpointRecord struct {
}

// cpreates a log record by reading one other value from the log
func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (cpr *CheckpointRecord) Op() int {
	return CHECKPOINT
}

func (cpr *CheckpointRecord) TxNumber() int {
	return -1
}

func (cpr *CheckpointRecord) Undo(*tx.Transaction) {}

func (cpr *CheckpointRecord) ToString() string {
	return "<CHECKPOINT>"
}

// write the CHECKPOINT record to the log
// contains the CHECKPOINT operator
// returns the LSN of the last log value
func WriteCheckpointRecordToLog(lm *log.Manager) int {
	record := make([]byte, file.IntBytes)
	page := file.NewPageWithSlice(record)
	page.SetInt(0, CHECKPOINT)
	return lm.Append(record)
}
