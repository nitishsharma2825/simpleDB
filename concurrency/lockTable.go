package concurrency

import (
	"sync"
	"time"

	"github.com/nitishsharma2825/simpleDB/file"
)

/*
Lock Table which provides methods to lock/unlock blocks
If a txn requests a lock that causes a conflict with an existing lock, then it is placed on a wait list
There is only 1 wait list for all blocks
When the last lock on a block is unlocked, then all txns are removed from the wait list and scheduled
If one of those txns discovers that block is still locked, it will place itself back on the wait list
*/

const MAX_TIME = 10 * time.Second // 10s
type LockTable struct {
	locks map[file.BlockID]int
	mu    sync.Mutex
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks: make(map[file.BlockID]int),
		mu:    sync.Mutex{},
	}
}

/*
Grant an SLock on the block
Check if an XLock exist on the block
*/
func (lt *LockTable) Slock(blockId file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now()
	for {
		if !lt.HasXlock(blockId) {
			val := lt.GetLockVal(blockId)
			lt.locks[blockId] = val + 1
			break
		}

		if lt.WaitingTooLong(startTime) {
			return ErrLockAbort
		}

		time.Sleep(time.Duration(MAX_TIME))
	}

	return nil
}

/*
Grant XLock on the block
Check if any SLock exist on the block
*/
func (lt *LockTable) Xlock(blockId file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now()
	for {
		if !lt.HasOtherSlocks(blockId) {
			lt.locks[blockId] = -1
			break
		}

		if lt.WaitingTooLong(startTime) {
			return ErrLockAbort
		}

		time.Sleep(time.Duration(MAX_TIME))
	}

	return nil
}

/*
Release a lock on the specified block
*/
func (lt *LockTable) Unlock(blockId file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.GetLockVal(blockId)
	if val > 1 {
		lt.locks[blockId] = val - 1
	} else {
		delete(lt.locks, blockId)
	}
}

// -1 indicates XLock for this block
func (lt *LockTable) HasXlock(blockId file.BlockID) bool {
	return lt.GetLockVal(blockId) < 0
}

// Positive int indicates no of SLocks held for this block
func (lt *LockTable) HasOtherSlocks(blockId file.BlockID) bool {
	return lt.GetLockVal(blockId) > 0
}

func (lt *LockTable) GetLockVal(blockId file.BlockID) int {
	val, ok := lt.locks[blockId]
	if !ok {
		return 0
	}

	return val
}

func (lt *LockTable) WaitingTooLong(startTime time.Time) bool {
	return time.Since(startTime).Seconds() > MAX_TIME.Seconds()
}
