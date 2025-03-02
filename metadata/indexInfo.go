package metadata

import (
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Information about an index
This is used by the query planner
to estimate the costs of using the index,
and to obtain the layout of the index records
Its methods are essentially the same as those of Plan
*/
type IndexInfo struct {
	indexName   string
	fieldName   string
	tx          *tx.Transaction
	tableSchema *record.Schema // table schema
	indexLayout *record.Layout // index layout
	statInfo    StatInfo
}

func NewIndexInfo(idxName string, fldName string, tblSchema *record.Schema, tx *tx.Transaction, si StatInfo) *IndexInfo {
	indexInfo := &IndexInfo{
		indexName:   idxName,
		fieldName:   fldName,
		tx:          tx,
		tableSchema: tblSchema,
		statInfo:    si,
	}

	indexInfo.indexLayout = indexInfo.CreateIndexLayout()
	return indexInfo
}

/*
Open the index described by this object
*/
// TODO: Need to implement index
func (ii *IndexInfo) Open() Index {
	return NewHashIndex(ii.tx, ii.indexName, ii.indexLayout)
	// return NewBTreeIndex()
}

/*
Estimate the number of block accesses required to find all index records
having a particular search key.
The method uses the table's metadata to estimate the size of the index file and number of
indexed records per blocl
It then passes this information to the traversalCost method of the appropriate index type
which provides the estimate
*/
func (ii *IndexInfo) BlocksAccessed() int {
}

/*
Return the estimated number of records having a search key.
This value is the same as doing a select query
it is the number of records in the table / number of distinct value of the indexed field
*/
func (ii *IndexInfo) RecordsOutput() int {
	return ii.statInfo.RecordsOutput() / ii.statInfo.DistinctValues(ii.fieldName)
}

/*
Return the distinct values for a specified field in the underlying table,
or 1 for the indexed field
*/
func (ii *IndexInfo) DistinctValues(fname string) int {
	if ii.fieldName == fname {
		return 1
	}
	return ii.statInfo.DistinctValues(ii.fieldName)
}

/*
Return the layout of the index records
Scheme consists of dataRID (represented as 2 integers - blockNum, recordId)
and dataval (which is the indexed field)
Schema information about the indexed field is obtained via the table's schema
*/
func (ii *IndexInfo) CreateIndexLayout() *record.Layout {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")
	if ii.tableSchema.FieldType(ii.fieldName) == record.INTEGER {
		sch.AddIntField("dataval")
	} else {
		fldLen := ii.tableSchema.Length(ii.fieldName)
		sch.AddStringField("dataval", fldLen)
	}
	return record.NewLayout(sch)
}
