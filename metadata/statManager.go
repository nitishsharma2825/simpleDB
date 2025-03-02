package metadata

import (
	"sync"

	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Keeps statistical information about each table
It does not store this information in the database as catalog table
Instead, it calculates the information on system startup,
and periodically refreshes it
*/
type StatManager struct {
	tableManager *TableManager
	tableStats   map[string]StatInfo
	numCalls     int
	mu           sync.Mutex
}

func NewStatManager(tm *TableManager, tx *tx.Transaction) *StatManager {
	sm := &StatManager{
		tableManager: tm,
		tableStats:   make(map[string]StatInfo),
		mu:           sync.Mutex{},
	}
	sm.mu.Lock()
	sm.refreshStatistics(tx)
	sm.mu.Unlock()
	return sm
}

func (sm *StatManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > 100 {
		sm.refreshStatistics(tx)
	}
	si, ok := sm.tableStats[tableName]
	if !ok {
		si = sm.calculateTableStats(tableName, layout, tx)
		sm.tableStats[tableName] = si
	}
	return si
}

func (sm *StatManager) refreshStatistics(tx *tx.Transaction) {
	sm.tableStats = make(map[string]StatInfo)
	sm.numCalls = 0
	tcatLayout := sm.tableManager.GetLayout("tblcat", tx)
	tcat := record.NewTableScan(tx, "tblcat", tcatLayout)
	for tcat.Next() {
		tableName := tcat.GetString("tblname")
		layout := sm.tableManager.GetLayout(tableName, tx)
		si := sm.calculateTableStats(tableName, layout, tx)
		sm.tableStats[tableName] = si
	}
	tcat.Close()
}

func (sm *StatManager) calculateTableStats(tableName string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	numRecs := 0
	numBlocks := 0
	ts := record.NewTableScan(tx, tableName, layout)
	for ts.Next() {
		numRecs++
		numBlocks = ts.GetRID().BlockNum() + 1
	}
	ts.Close()
	return StatInfo{numRecs, numBlocks}
}
