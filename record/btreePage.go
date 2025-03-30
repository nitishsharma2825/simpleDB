package record

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
A B-tree directory[non-leaf] and leaf pages have common functionalities
Their records are stored in sorted order, pages split when full
A BTree Node object contains this common functionality
*/

type BTPage struct {
	tx           *tx.Transaction
	currentBlock *file.BlockID
	layout       *Layout
}

/*
Open a node for the specified B-tree block
- tx: the calling transaction
- currentBlock: reference to the B-Tree block
- layout: metadata about the particular B-Tree file
*/
func NewBTPage(tx *tx.Transaction, currentBlock *file.BlockID, layout *Layout) *BTPage {
	btPage := &BTPage{
		tx:           tx,
		currentBlock: currentBlock,
		layout:       layout,
	}
	tx.Pin(*currentBlock)
	return btPage
}

/*
Calculate position where the 1st record having the specified search key should be,
then return the position before it
*/
func (btpage *BTPage) FindSlotBefore(searchKey *Constant) int {
	slot := 0
	for slot < btpage.GetNumRecs() && btpage.GetDataVal(slot).CompareTo(*searchKey) < 0 {
		slot++
	}
	return slot - 1
}

/*
Close the page by unpinning its buffer
*/
func (btpage *BTPage) Close() {
	if btpage.currentBlock != nil {
		btpage.tx.UnPin(*btpage.currentBlock)
	}
	btpage.currentBlock = nil
}

/*
Return true if block is full
*/
func (btpage *BTPage) IsFull() bool {
	return btpage.slotPos(btpage.GetNumRecs()+1) >= btpage.tx.BlockSize()
}

/*
Split the page at the specified position
A new page is created, and records of the page starting at split pos are transferred to the new page
*/
func (btpage *BTPage) Split(splitPos int, flag int) *file.BlockID {
	newBlock := btpage.AppendNew(flag)
	newPage := NewBTPage(btpage.tx, newBlock, btpage.layout)
	btpage.transferRecords(splitPos, newPage)
	newPage.SetFlag(flag)
	newPage.Close()
	return newBlock
}

/*
Return the dataval of the record at the specified slot
*/
func (btpage *BTPage) GetDataVal(slot int) Constant {
	val := btpage.getVal(slot, "dataval")
	return *val
}

// set the flag value for the block
func (btpage *BTPage) SetFlag(val int) {
	btpage.tx.SetInt(*btpage.currentBlock, 0, val, true)
}

// get the flag value for the block
func (btpage *BTPage) GetFlag() int {
	return btpage.tx.GetInt(*btpage.currentBlock, 0)
}

/*
Append a new block to the end of the specified B-tree file,
having the specified flag value
*/
func (btpage *BTPage) AppendNew(flag int) *file.BlockID {
	block := btpage.tx.Append(btpage.currentBlock.FileName())
	btpage.tx.Pin(block)
	btpage.Format(&block, flag)
	return &block
}

func (btpage *BTPage) Format(block *file.BlockID, flag int) {
	btpage.tx.SetInt(*block, 0, flag, false)
	btpage.tx.SetInt(*block, file.IntBytes, 0, false) // #records = 0
	recordSize := btpage.layout.SlotSize()
	for pos := 2 * file.IntBytes; pos+recordSize <= btpage.tx.BlockSize(); pos += recordSize {
		btpage.makeDefaultRecord(block, pos)
	}
}

// Methods called only by BTreeDir

/*
Return the block number stored in the index record at the specified slot
*/
func (btpage *BTPage) GetChildNum(slot int) int {
	return btpage.getInt(slot, "block")
}

/*
Insert a directory entry at the specified slot
*/
func (btpage *BTPage) InsertDir(slot int, val *Constant, blkNum int) {
	btpage.insert(slot)
	btpage.setVal(slot, "dataval", val)
	btpage.setInt(slot, "block", blkNum)
}

// Methods called only by BTreeLeaf

/*
Return the dataRID value stored in the specified leaf index record
*/
func (btpage *BTPage) GetDataRID(slot int) *RID {
	blockNum := btpage.getInt(slot, "block")
	id := btpage.getInt(slot, "id")
	rid := NewRID(blockNum, id)
	return &rid
}

/*
Insert a lead index record at the specified slot
*/
func (btpage *BTPage) InsertLeaf(slot int, val *Constant, rid *RID) {
	btpage.insert(slot)
	btpage.setVal(slot, "dataval", val)
	btpage.setInt(slot, "block", rid.BlockNum())
	btpage.setInt(slot, "id", rid.Slot())
}

/*
Delete the index record at the specified slot
*/
func (btpage *BTPage) Delete(slot int) {
	for i := slot + 1; i < btpage.GetNumRecs(); i++ {
		btpage.copyRecord(i, i-1)
	}
	btpage.setNumRecs(btpage.GetNumRecs() - 1)
}

/*
Return the numnber of index records in this page
The 4th-7th bit contains the number of records
*/
func (btpage *BTPage) GetNumRecs() int {
	return btpage.tx.GetInt(*btpage.currentBlock, file.IntBytes)
}

// private methods

func (btpage *BTPage) getVal(slot int, fieldname string) *Constant {
	fieldType := btpage.layout.schema.FieldType(fieldname)
	if fieldType == INTEGER {
		val := NewIntConstant(btpage.getInt(slot, fieldname))
		return &val
	} else {
		val := NewStringConstant(btpage.getString(slot, fieldname))
		return &val
	}
}

func (btpage *BTPage) getInt(slot int, fieldname string) int {
	pos := btpage.fieldPos(slot, fieldname)
	return btpage.tx.GetInt(*btpage.currentBlock, pos)
}

func (btpage *BTPage) getString(slot int, fieldname string) string {
	pos := btpage.fieldPos(slot, fieldname)
	return btpage.tx.GetString(*btpage.currentBlock, pos)
}

func (btpage *BTPage) setVal(slot int, fldname string, val *Constant) {
	fieldType := btpage.layout.Schema().FieldType(fldname)
	if fieldType == INTEGER {
		btpage.setInt(slot, fldname, val.AsInt())
	} else {
		btpage.setString(slot, fldname, val.AsString())
	}
}

func (btpage *BTPage) setInt(slot int, fieldname string, val int) {
	pos := btpage.fieldPos(slot, fieldname)
	btpage.tx.SetInt(*btpage.currentBlock, pos, val, true)
}

func (btpage *BTPage) setString(slot int, fieldname string, val string) {
	pos := btpage.fieldPos(slot, fieldname)
	btpage.tx.SetString(*btpage.currentBlock, pos, val, true)
}

func (btpage *BTPage) setNumRecs(n int) {
	btpage.tx.SetInt(*btpage.currentBlock, file.IntBytes, n, true)
}

// move any existing records to the right to make space for a new record
func (btpage *BTPage) insert(slot int) {
	for i := btpage.GetNumRecs(); i > slot; i-- {
		btpage.copyRecord(i-1, i)
	}
	btpage.setNumRecs(btpage.GetNumRecs() + 1)
}

// copy record from one slot to another in a block
func (btpage *BTPage) copyRecord(from, to int) {
	schema := btpage.layout.Schema()
	for _, fieldname := range schema.Fields() {
		btpage.setVal(to, fieldname, btpage.getVal(from, fieldname))
	}
}

func (btpage *BTPage) transferRecords(slot int, destPage *BTPage) {
	destSlot := 0
	for slot < btpage.GetNumRecs() {
		destPage.insert(destSlot)
		schema := btpage.layout.Schema()
		for _, fldName := range schema.Fields() {
			destPage.setVal(destSlot, fldName, btpage.getVal(slot, fldName))
		}
		btpage.Delete(slot)
		destSlot++
	}
}

func (btpage *BTPage) makeDefaultRecord(block *file.BlockID, pos int) {
	for _, fldname := range btpage.layout.Schema().Fields() {
		offset := btpage.layout.Offset(fldname)
		if btpage.layout.Schema().FieldType(fldname) == INTEGER {
			btpage.tx.SetInt(*block, pos+offset, 0, false)
		} else {
			btpage.tx.SetString(*block, pos+offset, "", false)
		}
	}
}

func (btpage *BTPage) fieldPos(slot int, fieldname string) int {
	offset := btpage.layout.Offset(fieldname)
	return btpage.slotPos(slot) + offset
}

func (btpage *BTPage) slotPos(slot int) int {
	slotSize := btpage.layout.SlotSize()
	// Q. why 2 extra int bytes at front?
	// Ans: 1st 4 bits - flag for inuse/empty, next 4 bits for number of records in this block
	return file.IntBytes + file.IntBytes + (slot * slotSize)
}
