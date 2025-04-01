package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
A modification of the basic update planner
It dispatches each update statement to the corresponding index planner
*/
type IndexUpdatePlanner struct {
	mdm *MetadataManager
}

func NewIndexUpdatePlanner(mdm *MetadataManager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{
		mdm: mdm,
	}
}

func (iup *IndexUpdatePlanner) ExecuteInsert(data *InsertData, tx *tx.Transaction) int {
	return 1
}

func (iup *IndexUpdatePlanner) ExecuteDelete(data *DeleteData, tx *tx.Transaction) int {
	return 1
}

func (iup *IndexUpdatePlanner) ExecuteModify(data *ModifyData, tx *tx.Transaction) int {
	return 1
}

func (iup *IndexUpdatePlanner) ExecuteCreateTable(data *CreateTableData, tx *tx.Transaction) int {
	return 1
}

func (iup *IndexUpdatePlanner) ExecuteCreateIndex(data *CreateIndexData, tx *tx.Transaction) int {
	return 1
}

func (iup *IndexUpdatePlanner) ExecuteCreateView(data *CreateViewData, tx *tx.Transaction) int {
	return 1
}
