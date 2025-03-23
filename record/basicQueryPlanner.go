package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
Simplest naive query planner
*/
type BasicQueryPlanner struct {
	mdm *MetadataManager
}

func NewBasicQueryPlanner(mdm *MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{
		mdm: mdm,
	}
}

func (bqp *BasicQueryPlanner) CreatePlan(data *QueryData, tx *tx.Transaction) Plan {
	// 1. Create a plan for each table/view
	plans := make([]Plan, 0)
	for _, tableName := range data.Tables() {
		viewDef := bqp.mdm.GetViewDef(tableName, tx)
		if viewDef != "" { // recursively plan the view
			parser := NewParser(viewDef)
			viewData, err := parser.Query()
			if err != nil {
				panic(err)
			}
			plans = append(plans, bqp.CreatePlan(viewData, tx))
		} else {
			plans = append(plans, NewTablePlan(tx, tableName, bqp.mdm))
		}
	}

	// 2. Create the product of all table plans
	p := plans[0]
	plans = plans[1:]
	for _, nextPlan := range plans {
		p = NewProductPlan(p, nextPlan)
	}

	// 3. Add a selection plan for the predicate
	p = NewSelectPlan(p, &data.pred)

	// 4. Project on the field names
	p = NewProjectPlan(p, data.Fields())

	return p
}
