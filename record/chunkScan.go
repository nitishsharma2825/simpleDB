package record

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Class for the chunk operator
A chunk is a K-Block portion of the materialized table having
the property that all blocks of the chunk fit into the available buffers.
Contains list of all records page in the blocks
*/
type ChunkScan struct {
	buffs                                       []*RecordPage
	tx                                          *tx.Transaction
	fileName                                    string
	layout                                      *Layout
	startBlockNum, endBlockNum, currentBlockNum int
	rp                                          *RecordPage
	currentSlot                                 int
}

/*
Create a chunk consisting of the specified pages
*/
func (cs *ChunkScan) NewChunkScan(tx *tx.Transaction, fileName string, layout *Layout, startBlockNum, endBlockNum int) *ChunkScan {
	scan := &ChunkScan{
		tx:            tx,
		fileName:      fileName,
		layout:        layout,
		startBlockNum: startBlockNum,
		endBlockNum:   endBlockNum,
		buffs:         make([]*RecordPage, 0),
	}

	for i := startBlockNum; i <= endBlockNum; i++ {
		blockId := file.NewBlockID(fileName, i)
		scan.buffs = append(scan.buffs, NewRecordPage(tx, blockId, layout)) // this pins the blocks to buffer
	}
	scan.moveToBlock(startBlockNum)
	return scan
}

func (cs *ChunkScan) Close() {
	for i := 0; i < len(cs.buffs); i++ {
		blockId := file.NewBlockID(cs.fileName, cs.startBlockNum+i)
		cs.tx.UnPin(blockId)
	}
}

func (cs *ChunkScan) BeforeFirst() {
	cs.moveToBlock(cs.startBlockNum)
}

/*
Moves to the next record in the current block of the chunk
If there are no more records, then make the next block be current block
If there are no more blocks in the chunk, return false
*/
func (cs *ChunkScan) Next() bool {
	currentSlot := cs.rp.NextAfter(cs.currentSlot)
	for currentSlot < 0 {
		if cs.currentBlockNum == cs.endBlockNum {
			return false
		}
		cs.moveToBlock(cs.rp.Block().BlockNumber() + 1)
		cs.currentSlot = cs.rp.NextAfter(cs.currentSlot)
	}
	return true
}

func (cs *ChunkScan) GetInt(fieldName string) int {
	return cs.rp.GetInt(cs.currentSlot, fieldName)
}

func (cs *ChunkScan) GetString(fieldName string) string {
	return cs.rp.GetString(cs.currentSlot, fieldName)
}

func (cs *ChunkScan) GetVal(fieldName string) Constant {
	if cs.layout.Schema().FieldType(fieldName) == INTEGER {
		return NewIntConstant(cs.GetInt(fieldName))
	} else {
		return NewStringConstant(cs.GetString(fieldName))
	}
}

func (cs *ChunkScan) HasField(fieldName string) bool {
	return cs.layout.Schema().HasField(fieldName)
}

func (cs *ChunkScan) moveToBlock(blockNum int) {
	cs.currentBlockNum = blockNum
	cs.rp = cs.buffs[cs.currentBlockNum-cs.startBlockNum]
	cs.currentSlot = -1
}
