package record

import (
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
	The Plan corresponding to a table
*/

type TablePlan struct {
	tableName string
	tx        *tx.Transaction
	layout    *Layout
	si        StatInfo
}

/*
Creates a leaf node in the query tree corresponding to the specified table
*/
func NewTablePlan(tx *tx.Transaction, tableName string, md *MetadataManager) *TablePlan {
	layout := md.GetLayout(tableName, tx)
	return &TablePlan{
		tableName: tableName,
		tx:        tx,
		layout:    layout,
		si:        md.GetStatInfo(tableName, layout, tx),
	}
}

/*
Creates a table scan for this query
*/
func (tp *TablePlan) Open() Scan {
	return NewTableScan(tp.tx, tp.tableName, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(fieldName string) int {
	return tp.si.DistinctValues(fieldName)
}

func (tp *TablePlan) Schema() *Schema {
	return tp.layout.Schema()
}
