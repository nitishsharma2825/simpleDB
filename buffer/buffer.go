package buffer

import (
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

// An individual buffer. It wraps a page
// and stores information about its status,
// such as associated disk block, the number of
// times the buffer has been pinned,
// whether its contents has been modified
// and if so the id and lsn of the modifying transaction

type Buffer struct {
	fm       *file.Manager
	lm       *log.Manager
	contents *file.Page
	blockId  file.BlockID
	pins     int
	txnum    int
	lsn      int
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPageWithSize(fm.BlockSize()),
		blockId:  file.NewBlockID("", -1),
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() file.BlockID {
	return b.blockId
}

func (b *Buffer) SetModified(txnum int, lsn int) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTxn() int {
	return b.txnum
}

func (b *Buffer) AssignToBlock(blockId file.BlockID) {
	// flush current contents
	b.flush()
	// Read the new block into the page
	b.blockId = blockId
	b.fm.Read(b.blockId, b.contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txnum >= 0 {
		// Flush the log page with this lsn
		b.lm.Flush(b.lsn)
		// Write the buffer to its disk block if it is dirty
		b.fm.Write(b.blockId, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) UnPin() {
	b.pins--
}
