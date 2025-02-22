package recovery

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

const (
	CHECKPOINT = 0
	START      = 1
	COMMIT     = 2
	ROLLBACK   = 3
	SETINT     = 4
	SETSTRING  = 5
)

type LogRecord interface {
	// the log record's type
	Op() int
	// transaction id stored with the log record
	TxNumber() int
	// Undoes the operation encoded by this log record
	// only applicable for SETINT and SETSTRING record type
	// takes id of the transaction performing the undo
	Undo(*tx.Transaction)

	ToString() string
}

func CreateLogRecord(record []byte) LogRecord {
	page := file.NewPageWithSlice(record)
	switch page.GetInt(0) {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(page)
	case COMMIT:
		return NewCommitRecord(page)
	case ROLLBACK:
		return NewRollbackRecord(page)
	case SETINT:
		return NewSetIntRecord(page)
	case SETSTRING:
		return NewSetStringRecord(page)
	default:
		return nil
	}
}
