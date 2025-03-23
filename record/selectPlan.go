package record

/*
	The plan corresponding to the Select operator
*/

type SelectPlan struct {
	plan Plan
	pred *Predicate
}

/*
Creates a new select node in the query tree,
having the specified subquery and predicate
*/
func NewSelectPlan(plan Plan, pred *Predicate) *SelectPlan {
	return &SelectPlan{
		plan: plan,
		pred: pred,
	}
}

/*
Creates a select scan for this query
*/
func (sp *SelectPlan) Open() Scan {
	scan := sp.plan.Open()
	return NewSelectScan(scan, sp.pred)
}

func (sp *SelectPlan) BlocksAccessed() int {
	return sp.plan.BlocksAccessed()
}

func (sp *SelectPlan) RecordsOutput() int {
	return sp.plan.RecordsOutput() / sp.pred.ReductionFactor(sp.plan)
}

/*
Estimates the number of distinct field values in the projection
If the predicate contains a term equating the specified field to a constant, value will be 1
else, it will be number of distinct values in the underlying query(but not more than size of output table)
*/
func (sp *SelectPlan) DistinctValues(fieldName string) int {
	if val := sp.pred.EquatesWithConstant(fieldName); val != nil {
		return 1
	} else {
		fieldName2 := sp.pred.EquatesWithField(fieldName)
		if fieldName2 != "" {
			return min(sp.plan.DistinctValues(fieldName), sp.plan.DistinctValues(fieldName2))
		} else {
			return sp.plan.DistinctValues(fieldName)
		}
	}
}

func (sp *SelectPlan) Schema() *Schema {
	return sp.plan.Schema()
}
