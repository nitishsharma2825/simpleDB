package log

import "github.com/nitishsharma2825/simpleDB/file"

// moves through the log file in reverse order

type Iterator struct {
	fm         *file.Manager
	blockId    file.BlockID
	page       *file.Page
	currentPos int
	boundary   int
}

func NewIterator(fm *file.Manager, blockId file.BlockID) *Iterator {
	iterator := &Iterator{
		fm:      fm,
		blockId: blockId,
		page:    file.NewPageWithSlice(make([]byte, fm.BlockSize())),
	}

	iterator.moveToBlock(blockId)

	return iterator
}

// Determines if the current log record
// is the earliest record in the log file
// returns true if there is an earlier record
func (it *Iterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.blockId.BlockNumber() > 0
}

// Moves to the next log record in the block
// If there are no more log records in the block,
// then move to the previous block and return log from there
func (it *Iterator) Next() []byte {
	if it.currentPos == it.fm.BlockSize() {
		// we are the end of the block
		it.blockId = file.NewBlockID(it.blockId.FileName(), it.blockId.BlockNumber()-1)
		it.moveToBlock(it.blockId)
	}
	record := it.page.GetBytes(it.currentPos)

	// move the iterator forward by
	it.currentPos += 4 + len(record)
	return record
}

// Moves to the specified log block
// and positions it at the first record in that block
func (it *Iterator) moveToBlock(blockId file.BlockID) {
	it.fm.Read(blockId, it.page)
	it.boundary = it.page.GetInt(0)
	it.currentPos = it.boundary
}
