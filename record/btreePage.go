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
	currentBlock file.BlockID
	layout       *Layout
}

/*
Open a node for the specified B-tree block
*/
func NewBTPage(tx *tx.Transaction, currentBlock file.BlockID, layout *Layout) *BTPage {
	btPage := &BTPage{
		tx:           tx,
		currentBlock: currentBlock,
		layout:       layout,
	}
	tx.Pin(currentBlock)
	return btPage
}

/*
Calculate position where the 1st record having the specified search key should be,
then return the position before it
*/
func (btree *BTPage) FindSlotBefore(searchKey *Constant) int {
	return 0
}

func (btree *BTPage) GetNumRecs() int {
	return btree.tx.GetInt(btree.currentBlock, file.IntBytes)
}
