package tx

import "github.com/nitishsharma2825/simpleDB/file"

/*
The concurrency manager for the transaction
Each transaction has its own concurrency manager
It keeps track of which locks the transaction currently has
Interacts with the global lock table as needed
*/
type ConcurrencyManager struct {
	lt    *LockTable
	locks map[file.BlockID]string
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		lt:    NewLockTable(),
		locks: make(map[file.BlockID]string),
	}
}

/*
Obtain an SLock on the block
Ask the lock table for an SLock if the txn currently has no locks on that block
*/
func (cm *ConcurrencyManager) Slock(blockId file.BlockID) {
	if _, ok := cm.locks[blockId]; !ok {
		cm.Slock(blockId)
		cm.locks[blockId] = "S"
	}
}

/*
Obtain an XLock on the block
First, get an SLock and then upgrade to XLock
*/
func (cm *ConcurrencyManager) Xlock(blockId file.BlockID) {
	if !cm.HasXlock(blockId) {
		cm.Slock(blockId)
		cm.lt.Xlock(blockId)
		cm.locks[blockId] = "X"
	}
}

/*
Release all locks by asking the lock table
*/
func (cm *ConcurrencyManager) Release() {
	for blockId := range cm.locks {
		cm.lt.Unlock(blockId)
	}
	for bi := range cm.locks {
		delete(cm.locks, bi)
	}
}

func (cm *ConcurrencyManager) HasXlock(blockId file.BlockID) bool {
	val, ok := cm.locks[blockId]
	if ok && val == "X" {
		return true
	}
	return false
}
