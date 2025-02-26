package record

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Provides abstraction of large array of records
*/
type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	fileName    string
	currentSlot int
}

func NewTableScan(tx *tx.Transaction, tableName string, layout *Layout) *TableScan {
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		fileName: tableName + ".tbl",
	}

	if tx.Size(ts.fileName) == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}

	return ts
}

// methods that implement scan

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() bool {
	ts.currentSlot = ts.rp.NextAfter(ts.currentSlot)
	for ts.currentSlot < 0 {
		if ts.atLastBlock() {
			return false
		}
		ts.moveToBlock(ts.rp.Block().BlockNumber() + 1)
		ts.currentSlot = ts.rp.NextAfter(ts.currentSlot)
	}
	return true
}

func (ts *TableScan) GetInt(fieldName string) int {
	return ts.rp.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) string {
	return ts.rp.GetString(ts.currentSlot, fieldName)
}

// TODO: Fix this
func (ts *TableScan) GetVal(fieldName string) int {
	if ts.layout.Schema().FieldType(fieldName) == INTEGER {
		return ts.GetInt(fieldName)
	} else {
		return ts.GetString(fieldName)
	}
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.UnPin(ts.rp.Block())
	}
}

// Methods that implement UpdateScan

func (ts *TableScan) SetInt(fieldName string, val int) {
	ts.rp.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) {
	ts.rp.SetString(ts.currentSlot, fieldName, val)
}

// TODO: Fix this
func (ts *TableScan) SetVal(fieldName string, val int) {
	if ts.layout.Schema().FieldType(fieldName) == INTEGER {
		ts.SetInt(fieldName, val)
	} else {
		ts.SetString(fieldName, string(val))
	}
}

func (ts *TableScan) Insert() {
	ts.currentSlot = ts.rp.InsertAfter(ts.currentSlot)
	for ts.currentSlot < 0 {
		if ts.atLastBlock() {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.Block().BlockNumber() + 1)
		}
		ts.currentSlot = ts.rp.InsertAfter(ts.currentSlot)
	}
}

func (ts *TableScan) Delete() {
	ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRid(rid RID) {
	ts.Close()
	blockId := file.NewBlockID(ts.fileName, rid.BlockNum())
	ts.rp = NewRecordPage(ts.tx, blockId, ts.layout)
	ts.currentSlot = rid.Slot()
}

func (ts *TableScan) GetRID() RID {
	return NewRID(ts.rp.Block().BlockNumber(), ts.currentSlot)
}

func (ts *TableScan) moveToBlock(blockNum int) {
	ts.Close()
	blockId := file.NewBlockID(ts.fileName, blockNum)
	ts.rp = NewRecordPage(ts.tx, blockId, ts.layout)
	ts.currentSlot = -1
}

func (ts *TableScan) moveToNewBlock() {
	ts.Close()
	blockId := ts.tx.Append(ts.fileName)
	ts.rp = NewRecordPage(ts.tx, blockId, ts.layout)
	ts.rp.Format()
	ts.currentSlot = -1
}

func (ts *TableScan) atLastBlock() bool {
	return ts.rp.Block().BlockNumber() == ts.tx.Size(ts.fileName)-1
}
