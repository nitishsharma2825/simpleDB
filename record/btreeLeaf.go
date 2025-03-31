package record

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
An object that holds the contents of a B-tree leaf block
*/

type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *Layout
	searchKey   *Constant
	contents    *BTPage
	currentSlot int
	filename    string
}

/*
Opens a buffer to hold the specified leaf block
The buffer is positioned immediately before the 1st record having the specified search key
*/
func NewBTreeLeaf(tx *tx.Transaction, blockId *file.BlockID, layout *Layout, searchKey *Constant) *BTreeLeaf {
	btreeLeaf := &BTreeLeaf{
		tx:        tx,
		layout:    layout,
		searchKey: searchKey,
		contents:  NewBTPage(tx, blockId, layout),
		filename:  blockId.FileName(),
	}
	btreeLeaf.currentSlot = btreeLeaf.contents.FindSlotBefore(searchKey)
	return btreeLeaf
}

func (bleaf *BTreeLeaf) Close() {
	bleaf.contents.Close()
}

/*
Move to the next leaf record having the previously specified search key
Returns false if there is no more such records
*/
func (bleaf *BTreeLeaf) Next() bool {
	bleaf.currentSlot++
	if bleaf.currentSlot >= bleaf.contents.GetNumRecs() {
		return bleaf.tryOverFlow()
	} else if bleaf.contents.GetDataVal(bleaf.currentSlot).Equals(*bleaf.searchKey) {
		return true
	} else {
		return bleaf.tryOverFlow()
	}
}

// Return the dataRID value of the current leaf record
func (bleaf *BTreeLeaf) GetDataRID() RID {
	return *bleaf.contents.GetDataRID(bleaf.currentSlot)
}

// Delete the leaf record having the specified dataRID
func (bleaf *BTreeLeaf) Delete(datarid RID) {
	for bleaf.Next() {
		if bleaf.GetDataRID().Equals(datarid) {
			bleaf.contents.Delete(bleaf.currentSlot)
			return
		}
	}
}

/*
Insert a new leaf record having the specified dataRID and previously specified search key
If the record does not fit in the page, then the page splits and method returns directory entry for the new page
otherwise method returns null
If all of the records in the page have the same dataval,
then block does not splot, instead all but one of the records are placed into an overflow block
*/
func (bleaf *BTreeLeaf) Insert(dataRID RID) *DirEntry {
	// creation of a block on left side if insertion value is < 1st value of current block
	if bleaf.contents.GetFlag() >= 0 && bleaf.contents.GetDataVal(0).CompareTo(*bleaf.searchKey) > 0 {
		firstVal := bleaf.contents.GetDataVal(0)
		newBlock := bleaf.contents.Split(0, bleaf.contents.GetFlag())
		bleaf.currentSlot = 0
		bleaf.contents.SetFlag(-1)
		bleaf.contents.InsertLeaf(bleaf.currentSlot, bleaf.searchKey, &dataRID)
		return NewDirEntry(&firstVal, newBlock.BlockNumber())
	}

	bleaf.currentSlot++
	bleaf.contents.InsertLeaf(bleaf.currentSlot, bleaf.searchKey, &dataRID)
	if !bleaf.contents.IsFull() {
		return nil
	}
	// else page is full
	firstKey := bleaf.contents.GetDataVal(0)
	lastKey := bleaf.contents.GetDataVal(bleaf.contents.GetNumRecs() - 1)
	if lastKey.Equals(firstKey) {
		// create an overflow block to hold all but the first record
		newBlock := bleaf.contents.Split(1, bleaf.contents.GetFlag())
		bleaf.contents.SetFlag(newBlock.BlockNumber())
		return nil
	} else {
		splitPos := bleaf.contents.GetNumRecs() / 2
		splitKey := bleaf.contents.GetDataVal(splitPos)
		if splitKey.Equals(firstKey) {
			// move right, looking for the next key
			for bleaf.contents.GetDataVal(splitPos).Equals(splitKey) {
				splitPos++
			}
			splitKey = bleaf.contents.GetDataVal(splitPos)
		} else {
			// move left, looking for the 1st entry having that key
			for bleaf.contents.GetDataVal(splitPos - 1).Equals(splitKey) {
				splitPos--
			}
		}
		newBlock := bleaf.contents.Split(splitPos, -1)
		return NewDirEntry(&splitKey, newBlock.BlockNumber())
	}
}

// check if the leaf node is overflowed, the flag field will contain the block number of overflowed block
func (bleaf *BTreeLeaf) tryOverFlow() bool {
	firstKey := bleaf.contents.GetDataVal(0)
	flag := bleaf.contents.GetFlag()
	if flag < 0 || !bleaf.searchKey.Equals(firstKey) {
		return false
	}
	bleaf.contents.Close()
	nextBlock := file.NewBlockID(bleaf.filename, flag)
	bleaf.contents = NewBTPage(bleaf.tx, &nextBlock, bleaf.layout)
	bleaf.currentSlot = 0
	return true
}
