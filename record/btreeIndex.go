package record

import (
	"fmt"
	"math"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
A B-tree implementation of the index interface
*/

type BTreeIndex struct {
	tx                    *tx.Transaction
	dirLayout, leafLayout *Layout
	leafTable             string
	leaf                  *BTreeLeaf
	rootBlock             *file.BlockID
}

/*
Opens a B-Tree index for the specified Index
The method determines the appropriate files
for the leaf and directory records
creating them if they did not exist
*/
func NewBTreeIndex(tx *tx.Transaction, idxname string, leafLayout *Layout) *BTreeIndex {
	index := &BTreeIndex{
		tx:         tx,
		leafLayout: leafLayout,
		leaf:       nil,
	}

	// deal with the leaves
	index.leafTable = fmt.Sprintf("%q%q", idxname, "leaf")
	if tx.Size(index.leafTable) == 0 {
		// Add a block to the leaf index file
		block := tx.Append(index.leafTable)
		node := NewBTPage(tx, &block, index.leafLayout)
		node.Format(&block, -1)
	}

	// deal with the directory
	dirSchema := NewSchema()
	dirSchema.Add("block", leafLayout.Schema())
	dirSchema.Add("dataval", leafLayout.Schema())
	dirTable := fmt.Sprintf("%q%q", idxname, "dir")
	index.dirLayout = NewLayout(dirSchema)
	rootBlock := file.NewBlockID(dirTable, 0)
	index.rootBlock = &rootBlock
	if tx.Size(dirTable) == 0 {
		// create new root block
		tx.Append(dirTable)
		node := NewBTPage(tx, &rootBlock, index.dirLayout)
		node.Format(&rootBlock, 0)
		// insert initial directory entry
		fieldType := dirSchema.FieldType("dataval")
		minVal := NewStringConstant("")
		if fieldType == INTEGER {
			minVal = NewIntConstant(math.MinInt)
		}
		node.InsertDir(0, &minVal, 0)
		node.Close()
	}

	return index
}

/*
Traverse the directory to find the leaf block corresponding to the search key
The method then opens a page for that leaf block and positions the page before the 1st record (if any) having that search key
The leaf page is kept open for use by methods next and getDataRid
*/
func (bindex *BTreeIndex) BeforeFirst(searchKey *Constant) {
	bindex.Close()
	root := NewBTreeDir(bindex.tx, bindex.rootBlock, bindex.dirLayout)
	blockNum := root.Search(searchKey)
	root.Close()
	leafBlock := file.NewBlockID(bindex.leafTable, blockNum)
	bindex.leaf = NewBTreeLeaf(bindex.tx, &leafBlock, bindex.leafLayout, searchKey)
}

// Move to the next leaf record having the previously specified search key
func (bindex *BTreeIndex) Next() bool {
	return bindex.leaf.Next()
}

// Return the dataRID value from the current leaf record
func (bindex *BTreeIndex) GetDataRID() RID {
	return bindex.leaf.GetDataRID()
}

/*
Insert the specified record into the index
The method first traverses the directory to find the appropriate leaf page,
then it inserts the record into the leaf
If the insertion causes the leaf to split, then method calls insert on the root
passing it the directory entry of the new leaf page,
If the root node splits, then makeNewRoot is called
*/
func (bindex *BTreeIndex) Insert(dataval *Constant, dataRid RID) {
	bindex.BeforeFirst(dataval)
	e := bindex.leaf.Insert(dataRid)
	bindex.leaf.Close()
	if e == nil {
		return
	}
	root := NewBTreeDir(bindex.tx, bindex.rootBlock, bindex.dirLayout)
	e2 := root.Insert(e)
	if e2 != nil {
		root.MakeNewRoot(e2)
	}
	root.Close()
}

/*
Delete the specified index record
The method first traverses the directory to find the leaf page containing that record
then it deletes the record from the page
*/
func (bindex *BTreeIndex) Delete(dataval *Constant, datarid RID) {
	bindex.BeforeFirst(dataval)
	bindex.leaf.Delete(datarid)
	bindex.leaf.Close()
}

// closes the index by closing its open leaf page
func (bindex *BTreeIndex) Close() {
	bindex.leaf.Close()
}

/*
Estimate the number of block accesses required to find all index records having a particular search key
*/
func BTreeSearchCost(numBlocks int, recPerBlock int) int {
	return 1 + int(math.Log(float64(numBlocks))/math.Log(float64(recPerBlock)))
}
