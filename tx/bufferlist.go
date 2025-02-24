package tx

import (
	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
)

/*
Manages the transaction's currently pinned buffers
*/

type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	pins    map[file.BlockID]bool
	bm      *buffer.Manager
}

func NewBufferList(bm *buffer.Manager) *BufferList {
	return &BufferList{
		bm:      bm,
		pins:    make(map[file.BlockID]bool),
		buffers: make(map[file.BlockID]*buffer.Buffer),
	}
}

/*
Return the buffer pinned to the specified block
Returns null if transaction has not pinned the block
*/
func (bl *BufferList) GetBuffer(blockId file.BlockID) *buffer.Buffer {
	if buff, ok := bl.buffers[blockId]; ok {
		return buff
	}
	return nil
}

/*
Pin the block and keep track of the buffer internally
*/
func (bl *BufferList) Pin(blockId file.BlockID) {
	buff, err := bl.bm.Pin(blockId)
	if err != nil {
		panic(err)
	}

	bl.buffers[blockId] = buff
	bl.pins[blockId] = true
}

/*
Unpin the specified block
*/
func (bl *BufferList) UnPin(blockId file.BlockID) {
	buff := bl.buffers[blockId]
	bl.bm.UnPin(buff)
	delete(bl.pins, blockId)
	if !bl.pins[blockId] {
		delete(bl.buffers, blockId)
	}
}

/*
Unpin any buffers still pinned by this transaction
*/
func (bl *BufferList) UnPinAll() {
	for blockId := range bl.pins {
		buff := bl.buffers[blockId]
		bl.bm.UnPin(buff)
	}

	for bi := range bl.buffers {
		delete(bl.buffers, bi)
	}

	for bi := range bl.pins {
		delete(bl.pins, bi)
	}
}
