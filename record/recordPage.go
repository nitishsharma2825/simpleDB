package record

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Store a record at a given location in a block
*/

const (
	EMPTY = 0
	USED  = 1
)

type RecordPage struct {
	blockId file.BlockID
	layout  *Layout
	tx      *tx.Transaction
}

func NewRecordPage(tx *tx.Transaction, blockId file.BlockID, layout *Layout) *RecordPage {
	recPage := &RecordPage{
		blockId: blockId,
		layout:  layout,
		tx:      tx,
	}
	tx.Pin(blockId)
	return recPage
}

/*
Return the integer stored for the specified field of a specified slot
*/
func (rp *RecordPage) GetInt(slot int, fieldName string) int {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	return rp.tx.GetInt(rp.blockId, fieldPos)
}

/*
Return the string value stored for the specified field of a specified slot
*/
func (rp *RecordPage) GetString(slot int, fieldName string) string {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	return rp.tx.GetString(rp.blockId, fieldPos)
}

/*
Store an integer at the specified field of the specified slot
*/
func (rp *RecordPage) SetInt(slot int, fieldName string, val int) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	rp.tx.SetInt(rp.blockId, fieldPos, val, true)
}

/*
Store an string at the specified field of the specified slot
*/
func (rp *RecordPage) SetString(slot int, fieldName string, val string) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	rp.tx.SetString(rp.blockId, fieldPos, val, true)
}

func (rp *RecordPage) Delete(slot int) {
	rp.SetFlag(slot, EMPTY)
}

/*
Set the record's empty/inuse flag
*/
func (rp *RecordPage) SetFlag(slot int, flag int) {
	rp.tx.SetInt(rp.blockId, rp.offset(slot), flag, true)
}

func (rp *RecordPage) offset(slot int) int {
	return rp.layout.SlotSize() * slot
}
