package metadata

import (
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

const MAX_VIEWDEF = 100

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *tx.Transaction) *ViewManager {
	vm := &ViewManager{tableManager}
	if isNew {
		sch := record.NewSchema()
		sch.AddStringField("viewname", MAX_NAME)
		sch.AddStringField("viewdef", MAX_VIEWDEF)
		tableManager.CreateTable("viewcat", sch, tx)
	}
	return vm
}

func (vm *ViewManager) CreateView(vname string, vdef string, tx *tx.Transaction) {
	layout := vm.tableManager.GetLayout("viewcat", tx)
	ts := record.NewTableScan(tx, "viewcat", layout)
	ts.Insert()
	ts.SetString("viewname", vname)
	ts.SetString("viewdef", vdef)
	ts.Close()
}

func (vm *ViewManager) GetViewDef(vname string, tx *tx.Transaction) string {
	result := ""
	layout := vm.tableManager.GetLayout("viewcat", tx)
	ts := record.NewTableScan(tx, "viewcat", layout)
	for ts.Next() {
		if ts.GetString("viewname") == vname {
			result = ts.GetString("viewdef")
			break
		}
	}
	ts.Close()
	return result
}
