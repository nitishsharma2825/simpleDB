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
	tableName := data.TblName
	tablePlan := NewTablePlan(tx, tableName, iup.mdm)

	// first insert the record
	updateScan := tablePlan.Open().(*TableScan)
	updateScan.Insert()
	rid := updateScan.GetRID()

	// then modify each field, inserting an index record if appropriate
	indexes := iup.mdm.GetIndexInfo(tableName, tx)
	for i, fieldName := range data.Fields {
		updateScan.SetVal(fieldName, *data.Vals[i])

		ii := indexes[fieldName]
		if ii != nil {
			index := ii.Open()
			index.Insert(data.Vals[i], rid)
			index.Close()
		}
	}
	updateScan.Close()
	return 1
}

func (iup *IndexUpdatePlanner) ExecuteDelete(data *DeleteData, tx *tx.Transaction) int {
	tableName := data.TblName
	tablePlan := NewTablePlan(tx, tableName, iup.mdm)
	selectPlan := NewSelectPlan(tablePlan, data.Pred)
	indexes := iup.mdm.GetIndexInfo(tableName, tx)

	updateScan := selectPlan.Open().(*SelectScan)
	count := 0
	for updateScan.Next() {
		// 1st delete the record's RID from every index
		rid := updateScan.GetRID()
		for fieldName := range indexes {
			val := updateScan.GetVal(fieldName)
			index := indexes[fieldName].Open()
			index.Delete(&val, rid)
			index.Close()
		}
		// then delete the record
		updateScan.Delete()
		count++
	}
	updateScan.Close()
	return count
}

func (iup *IndexUpdatePlanner) ExecuteModify(data *ModifyData, tx *tx.Transaction) int {
	tableName := data.TblName
	fieldName := data.FldName
	tablePlan := NewTablePlan(tx, tableName, iup.mdm)
	selectPlan := NewSelectPlan(tablePlan, data.Pred)

	ii := iup.mdm.GetIndexInfo(tableName, tx)[fieldName]
	var index Index
	if ii != nil {
		index = ii.Open()
	} else {
		index = nil
	}

	updateScan := selectPlan.Open().(*SelectScan)
	count := 0
	for updateScan.Next() {
		// 1st update the record
		newVal := data.NewVal.Evaluate(updateScan)
		oldVal := updateScan.GetVal(fieldName)
		updateScan.SetVal(fieldName, newVal)

		// then update the appropriate index, if it exists
		if index != nil {
			rid := updateScan.GetRID()
			index.Delete(&oldVal, rid)
			index.Insert(&newVal, rid)
		}
		count++
	}
	if index != nil {
		index.Close()
	}
	updateScan.Close()
	return count
}

func (iup *IndexUpdatePlanner) ExecuteCreateTable(data *CreateTableData, tx *tx.Transaction) int {
	iup.mdm.CreateTable(data.TblName, data.Schema, tx)
	return 0
}

func (iup *IndexUpdatePlanner) ExecuteCreateIndex(data *CreateIndexData, tx *tx.Transaction) int {
	iup.mdm.CreateIndex(data.IdxName, data.TblName, data.FldName, tx)
	return 0
}

func (iup *IndexUpdatePlanner) ExecuteCreateView(data *CreateViewData, tx *tx.Transaction) int {
	iup.mdm.CreateView(data.ViewName, data.ViewDef(), tx)
	return 0
}
