package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
Plan class for the multi buffer version of the product operator
*/
type MultiBufferProductPlan struct {
	tx       *tx.Transaction
	lhs, rhs Plan
	schema   *Schema
}

func NewMultiBufferProductPlan(tx *tx.Transaction, lhs, rhs Plan) *MultiBufferProductPlan {
	p := &MultiBufferProductPlan{
		tx:     tx,
		lhs:    NewMaterializePlan(tx, lhs),
		rhs:    rhs,
		schema: NewSchema(),
	}
	p.schema.Addall(p.lhs.Schema())
	p.schema.Addall(p.rhs.Schema())
	return p
}

/*
A scan for this query is created and returned
First, the method materializes its LHS, RHS queries
It then determines the optimal chunk size,
based on the materialized RHS file and number of available buffers
It creates a chunk plan for each chunk, saving them in a list.
Finally, it creates a multiscan for this list of plans, and returns that scan
*/
func (pp *MultiBufferProductPlan) Open() Scan {
	leftScan := pp.lhs.Open()
	tempTable := pp.copyRecordsFrom(pp.rhs)
	return NewMultiBufferProductScan(tx, leftScan, tempTable.tableName())
}

func (pp *MultiBufferProductPlan) copyRecordsFrom(plan Plan) *TempTable {
	sourceScan := plan.Open()
	schema := plan.Schema()
	tempTable := NewTempTable(pp.tx, schema)
	destScan := tempTable.Open().(UpdateScan)
	for sourceScan.Next() {
		destScan.Insert()
		for _, fldName := range schema.Fields() {
			destScan.SetVal(fldName, sourceScan.GetVal(fldName))
		}
	}
	sourceScan.Close()
	destScan.Close()
	return tempTable
}
