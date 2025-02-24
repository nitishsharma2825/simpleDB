package buffer

import (
	"sync"
	"time"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

// Manages the pinning and unpinning of buffers to blocks

const MAX_TIME = 10 * time.Second // 10s

type Manager struct {
	bufferPool   []*Buffer
	numAvailable int
	mu           sync.Mutex
}

func NewBufferManager(fm *file.Manager, lm *log.Manager, numBuffs int) *Manager {
	bp := make([]*Buffer, 0)
	for i := 0; i < numBuffs; i++ {
		bp = append(bp, NewBuffer(fm, lm))
	}

	return &Manager{
		bufferPool:   bp,
		numAvailable: numBuffs,
		mu:           sync.Mutex{},
	}
}

func (bm *Manager) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	return bm.numAvailable
}

// flushes the dirty buffers modified by the specified txns
func (bm *Manager) FlushAll(txnum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buf := range bm.bufferPool {
		if buf.ModifyingTxn() == txnum {
			buf.flush()
		}
	}
}

func (bm *Manager) UnPin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buff.UnPin()
	if !buff.IsPinned() {
		bm.numAvailable++
	}
}

// tries to pin a buffer to the given block
// if no buffer is available, clients will be put on wait until timeout
// if timeout is over, an ErrAbortException is returned to client
func (bm *Manager) Pin(blockId file.BlockID) (*Buffer, error) {
	// Try immediately first before waiting
	bm.mu.Lock()
	buff := bm.TryToPin(blockId)
	if buff != nil {
		bm.mu.Unlock()
		return buff, nil
	}
	bm.mu.Unlock()

	timeoutCh := time.After(MAX_TIME)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		// waits for the time to elapse, then send current time on this channel
		case <-timeoutCh:
			return nil, ErrBufferAbort
		case <-ticker.C:
			bm.mu.Lock()
			buff := bm.TryToPin(blockId)
			bm.mu.Unlock()

			if buff != nil {
				return buff, nil
			}
		}
	}
}

func (bm *Manager) TryToPin(blockId file.BlockID) *Buffer {
	buff := bm.FindExistingBuffer(blockId)
	if buff == nil {
		buff = bm.ChooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blockId)
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff
}

// tries to find if a buffer exists which is already assigned this block, else nil
func (bm *Manager) FindExistingBuffer(blockId file.BlockID) *Buffer {
	for _, buf := range bm.bufferPool {
		if buf.Block().Equals(blockId) {
			return buf
		}
	}
	return nil
}

// chooses for the 1st unpinned buffer in the buffer pool, returns nil if no buffer is available
// Improve with Algo's like LRU-K
func (bm *Manager) ChooseUnpinnedBuffer() *Buffer {
	for _, buf := range bm.bufferPool {
		if !buf.IsPinned() {
			return buf
		}
	}
	return nil
}
