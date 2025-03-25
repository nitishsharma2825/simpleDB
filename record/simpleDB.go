package record

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Class that configures the system
*/

// Constants
const (
	BLOCK_SIZE  = 400
	BUFFER_SIZE = 8
	LOG_FILE    = "simpledb.log"
)

type SimpleDB struct {
	fm      *file.Manager
	bm      *buffer.Manager
	lm      *log.Manager
	mdm     *MetadataManager
	planner *Planner
}

func NewSimpleDBWithBlockSize(dirname string, blockSize int, buffSize int) *SimpleDB {
	simpleDB := &SimpleDB{}
	simpleDB.fm = file.NewFileManager(dirname, blockSize)
	simpleDB.lm = log.NewLogManager(simpleDB.fm, LOG_FILE)
	simpleDB.bm = buffer.NewBufferManager(simpleDB.fm, simpleDB.lm, buffSize)
	return simpleDB
}

func NewSimpleDB(dirname string) *SimpleDB {
	simpleDB := NewSimpleDBWithBlockSize(dirname, BLOCK_SIZE, BUFFER_SIZE)
	tx := simpleDB.NewTx()
	isNew := simpleDB.fm.IsNew()
	if isNew {
		fmt.Println("Creating new database")
	} else {
		fmt.Println("recovering existing database")
		tx.Recover()
	}
	simpleDB.mdm = NewMetadataManager(isNew, tx)
	qp := NewBasicQueryPlanner(simpleDB.mdm)
	up := NewBasicUpdatePlanner(simpleDB.mdm)
	simpleDB.planner = NewPlanner(qp, up)
	tx.Commit()
	return simpleDB
}

func (s *SimpleDB) NewTx() *tx.Transaction {
	return tx.NewTransaction(s.fm, s.lm, s.bm)
}

func (s *SimpleDB) MdMgr() *MetadataManager {
	return s.mdm
}

func (s *SimpleDB) Planner() *Planner {
	return s.planner
}

func (s *SimpleDB) FileMgr() *file.Manager {
	return s.fm
}

func (s *SimpleDB) LogMgr() *log.Manager {
	return s.lm
}

func (s *SimpleDB) BufferMgr() *buffer.Manager {
	return s.bm
}
