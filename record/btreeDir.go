package record

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
A B-tree directory block
*/

type BTreeDir struct {
	tx       *tx.Transaction
	layout   *Layout
	contents *BTPage
	filename string
}

func NewBTreeDir(tx *tx.Transaction, blockId *file.BlockID, layout *Layout) *BTreeDir {
	return &BTreeDir{
		tx:       tx,
		layout:   layout,
		contents: NewBTPage(tx, blockId, layout),
		filename: blockId.FileName(),
	}
}

// closes the directory page
func (bdir *BTreeDir) Close() {
	bdir.contents.Close()
}

// Returns the block number of the b-tree leaf block that contains the search key
func (bdir *BTreeDir) Search(searchKey *Constant) int {
	childBlock := bdir.findChildBlock(searchKey)
	// recursively traverse the directory blocks to level-0 directory block
	for bdir.contents.GetFlag() > 0 {
		bdir.contents.Close()
		bdir.contents = NewBTPage(bdir.tx, childBlock, bdir.layout)
		childBlock = bdir.findChildBlock(searchKey)
	}
	return childBlock.BlockNumber()
}

/*
Create a new root block for the B-tree
The new root will have 2 children: the old root, and the specified block
Since the root must always be in block 0 of the file,
contents of old root will get transferred to a new block
*/
func (bdir *BTreeDir) MakeNewRoot(e *DirEntry) {
	firstVal := bdir.contents.GetDataVal(0)
	level := bdir.contents.GetFlag()
	newBlock := bdir.contents.Split(0, level) // i.e transfer all records
	oldroot := NewDirEntry(&firstVal, newBlock.BlockNumber())
	bdir.insertEntry(oldroot)
	bdir.insertEntry(e)
	bdir.contents.SetFlag(level + 1)
}

/*
Inserts a new directory entry into the B-tree block
If the block is at level 0, entry is inserted there
Otherwise, entry is inserted into the appropriate child node and return value is examined
A non-null return indicates that child node split, and so the returned entry is inserted into this block
If this block split, then the method similarily returns the entry information of the new block to its caller
otherwise method returns nil
*/
func (bdir *BTreeDir) Insert(e *DirEntry) *DirEntry {
	// level-0 directory
	if bdir.contents.GetFlag() == 0 {
		return bdir.insertEntry(e)
	}
	childBlock := bdir.findChildBlock(e.Dataval)
	child := NewBTreeDir(bdir.tx, childBlock, bdir.layout)
	myEntry := child.Insert(e)
	child.Close()

	if myEntry != nil {
		return bdir.insertEntry(myEntry)
	} else {
		return nil
	}
}

func (bdir *BTreeDir) insertEntry(e *DirEntry) *DirEntry {
	newSlot := 1 + bdir.contents.FindSlotBefore(e.Dataval)
	bdir.contents.InsertDir(newSlot, e.Dataval, e.Blocknum)
	if !bdir.contents.IsFull() {
		return nil
	}
	// else page is full, so split it
	level := bdir.contents.GetFlag()
	splitPos := bdir.contents.GetNumRecs() / 2
	splitVal := bdir.contents.GetDataVal(splitPos)
	newBlock := bdir.contents.Split(splitPos, level)
	return NewDirEntry(&splitVal, newBlock.BlockNumber())
}

func (bdir *BTreeDir) findChildBlock(searchKey *Constant) *file.BlockID {
	slot := bdir.contents.FindSlotBefore(searchKey)
	if bdir.contents.GetDataVal(slot + 1).Equals(*searchKey) {
		slot++
	}
	blockNum := bdir.contents.GetChildNum(slot)
	blockId := file.NewBlockID(bdir.filename, blockNum)
	return &blockId
}
