package record

import "github.com/nitishsharma2825/simpleDB/tx"

type BasicUpdatePlanner struct {
	mdm *MetadataManager
}

func NewBasicUpdatePlanner(mdm *MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		mdm: mdm,
	}
}

func (bup *BasicUpdatePlanner) ExecuteDelete(data *DeleteData, tx *tx.Transaction) int {
	tablePlan := NewTablePlan(tx, data.TblName, bup.mdm)
	selectPlan := NewSelectPlan(tablePlan, data.Pred)
	updateScan := selectPlan.Open().(*SelectScan)
	count := 0
	for updateScan.Next() {
		updateScan.Delete()
		count++
	}
	updateScan.Close()
	return count
}

func (bup *BasicUpdatePlanner) ExecuteModify(data *ModifyData, tx *tx.Transaction) int {
	tablePlan := NewTablePlan(tx, data.TblName, bup.mdm)
	selectPlan := NewSelectPlan(tablePlan, data.Pred)
	updateScan := selectPlan.Open().(*SelectScan)
	count := 0
	for updateScan.Next() {
		val := data.NewVal.Evaluate(updateScan)
		updateScan.SetVal(data.FldName, val)
		count++
	}
	updateScan.Close()
	return count
}

func (bup *BasicUpdatePlanner) ExecuteInsert(data *InsertData, tx *tx.Transaction) int {
	tablePlan := NewTablePlan(tx, data.TblName, bup.mdm)
	updateScan := tablePlan.Open().(*TableScan)
	updateScan.Insert()
	for i := 0; i < len(data.Fields); i++ {
		updateScan.SetVal(data.Fields[i], *data.Vals[i])
	}
	updateScan.Close()
	return 1
}

func (bup *BasicUpdatePlanner) ExecuteCreateTable(data *CreateTableData, tx *tx.Transaction) int {
	bup.mdm.CreateTable(data.TblName, data.Schema, tx)
	return 0
}

func (bup *BasicUpdatePlanner) ExecuteCreateIndex(data *CreateIndexData, tx *tx.Transaction) int {
	bup.mdm.CreateIndex(data.IdxName, data.TblName, data.FldName, tx)
	return 0
}

func (bup *BasicUpdatePlanner) ExecuteCreateView(data *CreateViewData, tx *tx.Transaction) int {
	bup.mdm.CreateView(data.ViewName, data.ViewDef(), tx)
	return 0
}
