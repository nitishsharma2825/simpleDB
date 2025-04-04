package record

import (
	"fmt"
	"sync"

	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
A class that creates temporary tables
A temp table is not registered in the catalog
*/
type TempTable struct {
	mu sync.Mutex

	tableName    string
	layout       *Layout
	tx           *tx.Transaction
	nextTableNum int
}

func NewTempTable(tx *tx.Transaction, schema *Schema) *TempTable {
	table := &TempTable{
		tx:           tx,
		layout:       NewLayout(schema),
		nextTableNum: 0,
	}
	table.tableName = table.nextTableName()
	return table
}

func (tt *TempTable) Open() UpdateScan {
	return NewTableScan(tt.tx, tt.tableName, tt.layout)
}

func (tt *TempTable) TableName() string {
	return tt.tableName
}

// return the table's metadata
func (tt *TempTable) GetLayout() *Layout {
	return tt.layout
}

func (tt *TempTable) nextTableName() string {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.nextTableNum++
	return fmt.Sprintf("test%d", tt.nextTableNum)
}
