package metadata

import (
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type MetadataManager struct {
	tableManager *TableManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) *MetadataManager {
	return &MetadataManager{
		tableManager: NewTableManager(isNew, tx),
	}
}

func (mm *MetadataManager) CreateTable(tblname string, schema *record.Schema, tx *tx.Transaction) {
	mm.tableManager.CreateTable(tblname, schema, tx)
}

func (mm *MetadataManager) GetLayout(tblname string, tx *tx.Transaction) *record.Layout {
	return mm.tableManager.GetLayout(tblname, tx)
}
