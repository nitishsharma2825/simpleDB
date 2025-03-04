package metadata

import (
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type MetadataManager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) *MetadataManager {
	tm := NewTableManager(isNew, tx)
	vm := NewViewManager(isNew, tm, tx)
	sm := NewStatManager(tm, tx)
	im := NewIndexManager(isNew, tm, sm, tx)

	return &MetadataManager{
		tableManager: tm,
		viewManager:  vm,
		statManager:  sm,
		indexManager: im,
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

func (mm *MetadataManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) {
	mm.indexManager.CreateIndex(indexName, tableName, fieldName, tx)
}

func (mm *MetadataManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]*IndexInfo {
	return mm.indexManager.GetIndexInfo(tableName, tx)
}

func (mm *MetadataManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	return mm.statManager.GetStatInfo(tableName, layout, tx)
}
