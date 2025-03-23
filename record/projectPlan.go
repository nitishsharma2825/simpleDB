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
