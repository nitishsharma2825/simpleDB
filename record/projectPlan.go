package record

/*
Plan class for Project operator
*/

// TODO: Implement this
type ProjectPlan struct {
	plan   Plan
	schema *Schema
}

func NewProjectPlan(plan Plan, fieldList []string) *ProjectPlan {
	projectPlan := ProjectPlan{
		plan:   plan,
		schema: NewSchema(),
	}
	for _, fieldName := range fieldList {
		projectPlan.schema.Add(fieldName, plan.Schema())
	}

	return &projectPlan
}

func (pp *ProjectPlan) Open() Scan {
	scan := pp.plan.Open()
	return NewProjectScan(scan, pp.schema.Fields())
}

func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.plan.BlocksAccessed()
}

func (pp *ProjectPlan) RecordsOutput() int {
	return pp.plan.RecordsOutput()
}

func (pp *ProjectPlan) DistinctValues(fieldName string) int {
	return pp.plan.DistinctValues(fieldName)
}

func (pp *ProjectPlan) Schema() *Schema {
	return pp.schema
}
