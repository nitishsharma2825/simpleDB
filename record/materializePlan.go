package record

import (
	"math"

	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Class for the materialize operator
*/
type MaterializePlan struct {
	sourcePlan Plan
	tx         *tx.Transaction
}

func NewMaterializePlan(tx *tx.Transaction, sourcePlan Plan) *MaterializePlan {
	return &MaterializePlan{
		tx:         tx,
		sourcePlan: sourcePlan,
	}
}

/*
This method loops through the underlying query,
copying its output records into a temp table
It then returns a table scan for that table not the actual table
*/
func (mp *MaterializePlan) Open() Scan {
	schema := mp.sourcePlan.Schema()
	tempTable := NewTempTable(mp.tx, schema)
	sourceScan := mp.sourcePlan.Open()
	destScan := tempTable.Open().(*TableScan)
	for sourceScan.Next() {
		destScan.Insert()
		for _, fieldName := range schema.Fields() {
			destScan.SetVal(fieldName, sourceScan.GetVal(fieldName))
		}
	}
	sourceScan.Close()
	destScan.BeforeFirst()
	return destScan
}

// Return the estimated number of blocks in the materialized table
func (mp *MaterializePlan) BlocksAccessed() int {
	// create a dummy layout object to calculate a record length
	layout := NewLayout(mp.sourcePlan.Schema())
	recordsPerBlock := float64(mp.tx.BlockSize() / layout.SlotSize())
	return int(math.Ceil(float64(float64(mp.sourcePlan.RecordsOutput()) / recordsPerBlock)))
}

func (mp *MaterializePlan) RecordsOutput() int {
	return mp.sourcePlan.RecordsOutput()
}

func (mp *MaterializePlan) DistinctValues(fieldName string) int {
	return mp.sourcePlan.DistinctValues(fieldName)
}

func (mp *MaterializePlan) Schema() *Schema {
	return mp.sourcePlan.Schema()
}
