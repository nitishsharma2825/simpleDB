package record

import (
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Index manager has similar functionality to the table manager
*/
type IndexManager struct {
	layout       *Layout
	tableManager *TableManager
	statManager  *StatManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, tx *tx.Transaction) *IndexManager {
	if isNew {
		sch := NewSchema()
		sch.AddStringField("indexname", MAX_NAME)
		sch.AddStringField("tablename", MAX_NAME)
		sch.AddStringField("fieldname", MAX_NAME)
		tableManager.CreateTable("idxcat", sch, tx)
	}

	return &IndexManager{
		tableManager: tableManager,
		statManager:  statManager,
		layout:       tableManager.GetLayout("idxcat", tx),
	}
}

/*
Create an index of the specified type for the specified field.
A unique ID is assigned to this index and its information is stored in "idxcat" table
*/
func (ii *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) {
	ts := NewTableScan(tx, "idxcat", ii.layout)
	ts.Insert()
	ts.SetString("indexname", indexName)
	ts.SetString("tablename", tableName)
	ts.SetString("fieldname", fieldName)
	ts.Close()
}

/*
Return a map containing the index info for all indexes on the specified table
Map[indexedfield]IndexInfo
*/
func (ii *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]*IndexInfo {
	result := make(map[string]*IndexInfo)
	ts := NewTableScan(tx, "idxcat", ii.layout)
	for ts.Next() {
		if ts.GetString("tablename") == tableName {
			indexName := ts.GetString("indexname")
			fieldName := ts.GetString("fieldname")
			tableLayout := ii.tableManager.GetLayout(tableName, tx)
			tableStatInfo := ii.statManager.GetStatInfo(tableName, tableLayout, tx)
			indexInfo := NewIndexInfo(indexName, fieldName, tableLayout.Schema(), tx, tableStatInfo)
			result[fieldName] = indexInfo
		}
	}
	ts.Close()
	return result
}
