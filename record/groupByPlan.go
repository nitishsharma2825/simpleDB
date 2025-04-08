package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
Plan class for the groupby operator
Group by is used with aggregate functions
*/
type GroupByPlan struct {
	plan        Plan
	groupFields []string
	aggfns      []AggregateFn
	schema      *Schema
}

/*
Create a groupby plan for the underlying query.
The grouping is determined by the specified collection of group fields
and aggregate is computed by the specified collection of aggregate functions.
*/
func NewGroupByPlan(tx *tx.Transaction, plan Plan, groupFields []string, aggfns []AggregateFn) *GroupByPlan {
	p := &GroupByPlan{
		plan:        NewSortPlan(tx, plan, groupFields),
		groupFields: groupFields,
		aggfns:      aggfns,
		schema:      NewSchema(),
	}

	// schema contains the group fields and fields on which aggregate functions are created
	for _, fieldName := range groupFields {
		p.schema.Add(fieldName, p.plan.Schema())
	}
	for _, fn := range aggfns {
		p.schema.AddIntField(fn.FieldName())
	}
	return p
}

/*
Opens a sort plan for the specified plan.
this sort plan ensures that the underlying records
will be appropriately grouped
*/
func (gp *GroupByPlan) Open() Scan {
	scan := gp.plan.Open()
	return NewGroupByScan(scan, gp.groupFields, gp.aggfns)
}

func (gp *GroupByPlan) BlocksAccessed() int {
	return gp.plan.BlocksAccessed()
}

func (gp *GroupByPlan) RecordsOutput() int {
	numGroups := 1
	for _, fldName := range gp.groupFields {
		numGroups = numGroups * gp.plan.DistinctValues(fldName)
	}
	return numGroups
}

func (gp *GroupByPlan) DistinctValues(fieldName string) int {
	if gp.plan.Schema().HasField(fieldName) {
		return gp.plan.DistinctValues(fieldName)
	} else {
		return gp.RecordsOutput()
	}
}

func (gp *GroupByPlan) Schema() *Schema {
	return gp.schema
}
