package record

/*
Plan for Product operator
*/
// TODO: Implement this
type ProductPlan struct {
	plan1, plan2 Plan
	schema       *Schema
}

func NewProductPlan(p1, p2 Plan) *ProductPlan {
	plan := ProductPlan{
		plan1:  p1,
		plan2:  p2,
		schema: NewSchema(),
	}
	plan.schema.Addall(p1.Schema())
	plan.schema.Addall(p2.Schema())
	return &plan
}

func (pp *ProductPlan) Open() Scan {
	scan1 := pp.plan1.Open()
	scan2 := pp.plan2.Open()
	return NewProductScan(scan1, scan2)
}

func (pp *ProductPlan) BlocksAccessed() int {
	return pp.plan1.BlocksAccessed() + (pp.plan1.RecordsOutput() * pp.plan2.BlocksAccessed())
}

func (pp *ProductPlan) RecordsOutput() int {
	return pp.plan1.RecordsOutput() * pp.plan2.RecordsOutput()
}

func (pp *ProductPlan) DistinctValues(fieldName string) int {
	if pp.plan1.Schema().HasField(fieldName) {
		return pp.plan1.DistinctValues(fieldName)
	} else {
		return pp.plan2.DistinctValues(fieldName)
	}
}

func (pp *ProductPlan) Schema() *Schema {
	return pp.schema
}
