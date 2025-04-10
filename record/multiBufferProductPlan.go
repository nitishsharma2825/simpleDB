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

/*
Create the scan class for the product of LHS and a table
*/
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
	return NewMultiBufferProductScan(pp.tx, leftScan, tempTable.TableName(), tempTable.layout)
}

/*
B(product(p1, p2)) = B(p2) + B(p1) * C(p2)
where C(p2) = No of chunks of p2
The method uses current number of available buffers to
calculate C(p2), and so value may differ when
query scan is opened.
*/
func (pp *MultiBufferProductPlan) BlocksAccessed() int {
	// this guess number of chunks
	availableBuffers := pp.tx.AvailableBuffs()
	size := NewMaterializePlan(pp.tx, pp.rhs).BlocksAccessed()
	numChunks := size / availableBuffers
	return pp.rhs.BlocksAccessed() + (pp.lhs.BlocksAccessed() * numChunks)
}

/*
Estimates the number of output records in the product
*/
func (pp *MultiBufferProductPlan) RecordsOutput() int {
	return pp.lhs.RecordsOutput() * pp.rhs.RecordsOutput()
}

/*
Estimates the distinct number of field values in the product
*/
func (pp *MultiBufferProductPlan) DistinctValues(fldName string) int {
	if pp.lhs.Schema().HasField(fldName) {
		return pp.lhs.DistinctValues(fldName)
	} else {
		return pp.rhs.DistinctValues(fldName)
	}
}

func (pp *MultiBufferProductPlan) Schema() *Schema {
	return pp.schema
}

func (pp *MultiBufferProductPlan) copyRecordsFrom(plan Plan) *TempTable {
	sourceScan := plan.Open()
	schema := plan.Schema()
	tempTable := NewTempTable(pp.tx, schema)
	destScan := tempTable.Open()
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
