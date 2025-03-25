package record

import "github.com/nitishsharma2825/simpleDB/tx"

type BetterQueryPlanner struct {
	mdm *MetadataManager
}

func NewBetterQueryPlanner(mdm *MetadataManager) *BetterQueryPlanner {
	return &BetterQueryPlanner{
		mdm: mdm,
	}
}

func (bqp *BetterQueryPlanner) CreatePlan(data *QueryData, tx *tx.Transaction) Plan {
	// 1. Create a plan for each table
	plans := make([]Plan, 0)
	for _, tableName := range data.tables {
		viewDef := bqp.mdm.GetViewDef(tableName, tx)
		if viewDef != "" {
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
		// try both ordering and choose one with lowest cost
		choice1 := NewProductPlan(nextPlan, p)
		choice2 := NewProductPlan(p, nextPlan)
		if choice1.BlocksAccessed() < choice2.BlocksAccessed() {
			p = choice1
		} else {
			p = choice2
		}
	}

	// 3. Add a selection plan for the predicate
	p = NewSelectPlan(p, &data.pred)

	// 4. Project on the field names
	p = NewProjectPlan(p, data.Fields())

	return p
}
