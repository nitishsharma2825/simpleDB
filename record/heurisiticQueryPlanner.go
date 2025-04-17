package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
A query planner that optimizes using a heuristic based algorithm
*/
type HeuristicQueryPlanner struct {
	tablePlanners []*TablePlanner
	mdm           *MetadataManager
}

func NewHeuristicQueryPlanner(mdm *MetadataManager) *HeuristicQueryPlanner {
	return &HeuristicQueryPlanner{
		tablePlanners: make([]*TablePlanner, 0),
		mdm:           mdm,
	}
}

/*
Creates an optimized left deep query plan using the following heuristics
1. Choose the smallest table to be first in join order (considering selection predicates)
2. Add the table to the join order which results in smallest output
*/
func (qp *HeuristicQueryPlanner) CreatePlan(data *QueryData, tx *tx.Transaction) Plan {
	// Step 1: create TablePlanner object for each mentioned table
	for _, tblName := range data.tables {
		tp := NewTablePlanner(tblName, &data.pred, tx, qp.mdm)
		qp.tablePlanners = append(qp.tablePlanners, tp)
	}

	// Step 2: Choose the lowest size plan to begin the join order
	currentPlan := qp.getLowestSelectPlan()

	// Step 3: Repeatedly add a plan to the join order
	for len(qp.tablePlanners) > 0 {
		p := qp.getLowestJoinPlan(currentPlan)
		if p != nil {
			currentPlan = p
		} else {
			currentPlan = qp.getLowestProductPlan(currentPlan)
		}
	}

	// Step 4: Project on field names and return
	return NewProjectPlan(currentPlan, data.fields)
}

func (qp *HeuristicQueryPlanner) getLowestSelectPlan() Plan {
	var bestTablePlanner *TablePlanner
	var bestPlan Plan
	for _, tp := range qp.tablePlanners {
		plan := tp.MakeSelectPlan()
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTablePlanner = tp
			bestPlan = plan
		}
	}
	if bestPlan != nil {
		for i, tp := range qp.tablePlanners {
			if tp == bestTablePlanner {
				qp.tablePlanners = append(qp.tablePlanners[:i], qp.tablePlanners[i+1:]...)
				break
			}
		}
	}
	return bestPlan
}

func (qp *HeuristicQueryPlanner) getLowestJoinPlan(current Plan) Plan {
	var bestTablePlanner *TablePlanner
	var bestPlan Plan
	for _, tp := range qp.tablePlanners {
		plan := tp.MakeJoinPlan(current)
		if plan != nil && (bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput()) {
			bestTablePlanner = tp
			bestPlan = plan
		}
	}
	if bestPlan != nil {
		for i, tp := range qp.tablePlanners {
			if tp == bestTablePlanner {
				qp.tablePlanners = append(qp.tablePlanners[:i], qp.tablePlanners[i+1:]...)
				break
			}
		}
	}
	return bestPlan
}

func (qp *HeuristicQueryPlanner) getLowestProductPlan(current Plan) Plan {
	var bestTablePlanner *TablePlanner
	var bestPlan Plan
	for _, tp := range qp.tablePlanners {
		plan := tp.MakeProductPlan(current)
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTablePlanner = tp
			bestPlan = plan
		}
	}
	if bestPlan != nil {
		for i, tp := range qp.tablePlanners {
			if tp == bestTablePlanner {
				qp.tablePlanners = append(qp.tablePlanners[:i], qp.tablePlanners[i+1:]...)
				break
			}
		}
	}
	return bestPlan
}

func (qp *HeuristicQueryPlanner) SetPlanner(planner Planner) {
	// for use in planning views
}
