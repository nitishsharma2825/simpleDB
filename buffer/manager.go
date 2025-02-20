package buffer

import (
	"errors"
	"sync"
	"time"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

// Manages the pinning and unpinning of buffers to blocks

const MAX_TIME_MS = 10000 // 10s

type Manager struct {
	bufferPool   []*Buffer
	numAvailable int
	mu           sync.Mutex
	applyCond    *sync.Cond
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

	for idx := range bm.bufferPool {
		if bm.bufferPool[idx].ModifyingTxn() == txnum {
			bm.bufferPool[idx].flush()
		}
	}
}

func (bm *Manager) UnPin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buff.UnPin()
	if !buff.IsPinned() {
		bm.numAvailable++
		// notify all waiting txns that a buffer is free
		bm.applyCond.Broadcast()
	}
}

func (bm *Manager) Pin(blockId file.BlockID) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	curTime := time.Now()
	buff := bm.TryToPin(blockId)
	for {
		if buff == nil && !bm.WaitingTooLong(curTime) {
			bm.applyCond.Wait()
		} else {
			break
		}
		buff = bm.TryToPin(blockId)
	}
	if buff == nil {
		return nil, errors.New("BufferAbortException")
	}

	return buff, nil
}

func (bm *Manager) WaitingTooLong(startTime time.Time) bool {
	return time.Since(startTime).Milliseconds() > MAX_TIME_MS
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

func (bm *Manager) FindExistingBuffer(blockId file.BlockID) *Buffer {
	for idx := range bm.bufferPool {
		if bm.bufferPool[idx].Block().Equals(blockId) {
			return bm.bufferPool[idx]
		}
	}
	return nil
}

func (bm *Manager) ChooseUnpinnedBuffer() *Buffer {
	for idx := range bm.bufferPool {
		if !bm.bufferPool[idx].IsPinned() {
			return bm.bufferPool[idx]
		}
	}
	return nil
}
