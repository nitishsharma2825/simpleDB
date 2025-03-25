package record

/*
A plan class corresponding to Product operator that determines
the most efficient ordering of its inputs
*/

type OptimizedProductPlan struct {
	bestPlan Plan
}

func NewOptimizedProductPlan(plan1, plan2 Plan) *OptimizedProductPlan {
	plan := &OptimizedProductPlan{}
	prod1 := NewProductPlan(plan1, plan2)
	prod2 := NewProductPlan(plan2, plan1)
	if prod1.BlocksAccessed() <= prod2.BlocksAccessed() {
		plan.bestPlan = prod1
	} else {
		plan.bestPlan = prod2
	}
	return plan
}

func (opp *OptimizedProductPlan) Open() Scan {
	return opp.bestPlan.Open()
}

func (opp *OptimizedProductPlan) BlocksAccessed() int {
	return opp.bestPlan.BlocksAccessed()
}

func (opp *OptimizedProductPlan) RecordsOutput() int {
	return opp.bestPlan.RecordsOutput()
}

func (opp *OptimizedProductPlan) DistinctValues(fieldName string) int {
	return opp.bestPlan.DistinctValues(fieldName)
}

func (opp *OptimizedProductPlan) Schema() *Schema {
	return opp.bestPlan.Schema()
}
