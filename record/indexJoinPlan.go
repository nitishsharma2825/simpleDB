package record

/*
Plan corresponding to the index join relational operator
*/
type IndexJoinPlan struct {
	p1, p2    Plan
	ii        *IndexInfo
	joinField string
	schema    *Schema
}

func NewIndexJoinPlan(p1, p2 Plan, ii *IndexInfo, joinField string) *IndexJoinPlan {
	plan := &IndexJoinPlan{
		p1:        p1,
		p2:        p2,
		ii:        ii,
		joinField: joinField,
		schema:    NewSchema(),
	}
	plan.schema.Addall(p1.Schema())
	plan.schema.Addall(p2.Schema())
	return plan
}

/*
Opens an indexjoin scan for this query
*/
func (ijp *IndexJoinPlan) Open() Scan {
	scan := ijp.p1.Open()
	// p2 has to be a table plan
	tableScan := ijp.p2.Open().(*TableScan)
	index := ijp.ii.Open()
	return NewIndexJoinScan(scan, index, ijp.joinField, tableScan)
}

/*
Estimates the number of block accesses to compute the join
Formula is: B(indexjoin(p1, p2, idx)) = B(p1) + R(p1)*B(idx) + R(indexjoin(p1, p2, idx))
*/
func (ijp *IndexJoinPlan) BlocksAccessed() int {
	return ijp.p1.BlocksAccessed() + (ijp.p1.RecordsOutput() * ijp.ii.BlocksAccessed()) + ijp.RecordsOutput()
}

/*
Estimates the number of output records in the join
Formula is: R(indexjoin(p1, p2, idx)) = R(p1) * R(idx)
*/
func (ijp *IndexJoinPlan) RecordsOutput() int {
	return ijp.p1.RecordsOutput() * ijp.ii.RecordsOutput()
}

/*
Estimates the number of distinct values for the specified field
*/
func (ijp *IndexJoinPlan) DistinctValues(fldName string) int {
	if ijp.p1.Schema().HasField(fldName) {
		return ijp.p1.DistinctValues(fldName)
	} else {
		return ijp.p2.DistinctValues(fldName)
	}
}

/*
Returns the schema of the index join
*/
func (ijp *IndexJoinPlan) Schema() *Schema {
	return ijp.schema
}
