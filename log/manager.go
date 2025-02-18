package log

import (
	"sync"

	"github.com/nitishsharma2825/simpleDB/file"
)

// Responsible for writing log records into a log file
// Tail of the log is kept in buffer which is flushed to disk when needed
type Manager struct {
	fm           *file.Manager
	logFile      string
	logPage      *file.Page
	currentBlock file.BlockID
	latestLSN    int
	lastSavedLSN int
	mu           sync.Mutex
}

func NewLogManager(fm *file.Manager, logFile string) *Manager {
	buf := make([]byte, fm.BlockSize())
	logSize := fm.Length(logFile)
	logPage := file.NewPageWithSlice(buf)

	logManager := &Manager{
		fm:           fm,
		logFile:      logFile,
		logPage:      logPage,
		latestLSN:    0,
		lastSavedLSN: 0,
		mu:           sync.Mutex{},
	}

	if logSize == 0 {
		// empty log, append a new disk block and assign new page
		logManager.AppendNewBlock()
	} else {
		logManager.currentBlock = file.NewBlockID(logFile, logSize-1)
		logManager.fm.Read(logManager.currentBlock, logPage)
	}

	return logManager
}

// Appends a log record to the log buffer
// The record is an arbitrary array of bytes
// Log records are written right->left in the buffer
// Size of the record is written before the bytes
// The beginning 4 bytes of buffer contain the location of last written record ("boundary")
// Storing the record backwards makes it easy to read them in reverse order

func (lm *Manager) Append(logRecord []byte) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// boundary contains the offset of the most recently added record
	boundary := lm.logPage.GetInt(0)
	recordSize := len(logRecord)

	// data + size(data)
	bytesNeeded := recordSize + 4

	// if bytes needed + page header > space left
	if bytesNeeded+4 > boundary {
		lm.flush()
		lm.AppendNewBlock()
		boundary = lm.logPage.GetInt(0)
	}

	// compute the leading byte offset where new record will start
	recordPosition := boundary - bytesNeeded

	lm.logPage.SetBytes(recordPosition, logRecord)
	lm.logPage.SetInt(0, recordPosition)
	lm.latestLSN += 1
	return lm.latestLSN
}

// helper methods

func (lm *Manager) AppendNewBlock() {
	// append an empty disk block to end of file
	lm.currentBlock = lm.fm.Append(lm.logFile)
	// set the starting offset in page
	lm.logPage.SetInt(0, lm.fm.BlockSize())
	// write to disk
	lm.fm.Write(lm.currentBlock, lm.logPage)
}

func (lm *Manager) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

func (lm *Manager) Iterator() *Iterator {
	lm.flush()
	return &Iterator{
		fm:      lm.fm,
		blockId: lm.currentBlock,
	}
}

// writes the contents of the logPage into the current block
// and updates lastSavedLSN id
func (lm *Manager) flush() {
	lm.fm.Write(lm.currentBlock, lm.logPage)
	lm.lastSavedLSN = lm.latestLSN
}
