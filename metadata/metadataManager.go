package metadata

import (
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type MetadataManager struct {
	tableManager *TableManager
	viewManager  *ViewManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) *MetadataManager {
	tm := NewTableManager(isNew, tx)
	vm := NewViewManager(isNew, tm, tx)

	return &MetadataManager{
		tableManager: tm,
		viewManager:  vm,
	}
}

func (mm *MetadataManager) CreateTable(tblname string, schema *record.Schema, tx *tx.Transaction) {
	mm.tableManager.CreateTable(tblname, schema, tx)
}

func (mm *MetadataManager) GetLayout(tblname string, tx *tx.Transaction) *record.Layout {
	return mm.tableManager.GetLayout(tblname, tx)
}

func (mm *MetadataManager) CreateView(viewname string, viewdef string, tx *tx.Transaction) {
	mm.viewManager.CreateView(viewname, viewdef, tx)
}

func (mm *MetadataManager) GetViewDef(viewname string, tx *tx.Transaction) string {
	return mm.viewManager.GetViewDef(viewname, tx)
}
