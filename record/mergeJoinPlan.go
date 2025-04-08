package record

import (
	"math"

	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Plan class for merge join operator
*/
type MergeJoinPlan struct {
	plan1, plan2           Plan
	fieldName1, fieldName2 string
	schema                 *Schema
}

func NewMergeJoinPlan(tx *tx.Transaction, p1, p2 Plan, fldName1, fldName2 string) *MergeJoinPlan {
	p := &MergeJoinPlan{
		fieldName1: fldName1,
		fieldName2: fldName2,
		schema:     NewSchema(),
		plan1:      NewSortPlan(tx, p1, []string{fldName1}),
		plan2:      NewSortPlan(tx, p2, []string{fldName2}),
	}
	p.schema.Addall(p.plan1.Schema())
	p.schema.Addall(p.plan2.Schema())
	return p
}

func (mjp *MergeJoinPlan) Open() Scan {
	scan1 := mjp.plan1.Open()
	scan2 := mjp.plan2.Open().(*SortScan)
	return NewMergeJoinScan(scan1, *scan2, mjp.fieldName1, mjp.fieldName2)
}

func (mjp *MergeJoinPlan) BlocksAccessed() int {
	return mjp.plan1.BlocksAccessed() + mjp.plan2.BlocksAccessed()
}

func (mjp *MergeJoinPlan) RecordsOutput() int {
	maxVals := math.Max(float64(mjp.plan1.DistinctValues(mjp.fieldName1)), float64(mjp.plan2.DistinctValues(mjp.fieldName2)))
	return (mjp.plan1.RecordsOutput() * mjp.plan2.RecordsOutput()) / int(maxVals)
}

func (mjp *MergeJoinPlan) DistinctValues(fldName string) int {
	if mjp.plan1.Schema().HasField(fldName) {
		return mjp.plan1.DistinctValues(fldName)
	} else {
		return mjp.plan2.DistinctValues(fldName)
	}
}

func (mjp *MergeJoinPlan) Schema() *Schema {
	return mjp.schema
}
