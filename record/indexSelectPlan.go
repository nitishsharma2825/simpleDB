package record

/*
Plan class corresponding to the Index Select relational operator
*/

type IndexSelectPlan struct {
	plan Plan
	ii   *IndexInfo
	val  *Constant
}

func NewIndexSelectPlan(plan Plan, ii *IndexInfo, val *Constant) *IndexSelectPlan {
	return &IndexSelectPlan{
		plan: plan,
		ii:   ii,
		val:  val,
	}
}

func (isp *IndexSelectPlan) Open() Scan {
	// error if p is not a tableplan
	tableScan := isp.plan.Open().(*TableScan)
	idx := isp.ii.Open()
	return NewIndexSelectScan(tableScan, idx, isp.val)
}

/*
Estimates the number of block accesses to compute the index selection,
which is the same as traversal cost + number of matching data records
*/
func (isp *IndexSelectPlan) BlocksAccessed() int {
	return isp.ii.BlocksAccessed() + isp.RecordsOutput()
}

/*
Estimates the number of output records in the index selection
which is the same as number of search key values
*/
func (isp *IndexSelectPlan) RecordsOutput() int {
	return isp.ii.RecordsOutput()
}

/*
Returns the distinct values as defined by the index
*/
func (isp *IndexSelectPlan) DistinctValues(fldName string) int {
	return isp.ii.DistinctValues(fldName)
}

/*
Returns the schema of the data table
*/
func (isp *IndexSelectPlan) Schema() *Schema {
	return isp.plan.Schema()
}
