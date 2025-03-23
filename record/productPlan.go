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
