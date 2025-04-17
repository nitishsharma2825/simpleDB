package record

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Class contains methods for planning a single table
*/
type TablePlanner struct {
	myPlan   *TablePlan
	mypred   *Predicate
	myschema *Schema
	indexes  map[string]*IndexInfo
	tx       *tx.Transaction
}

func NewTablePlanner(tblName string, mypred *Predicate, tx *tx.Transaction, mdm *MetadataManager) *TablePlanner {
	tp := &TablePlanner{
		mypred:  mypred,
		tx:      tx,
		myPlan:  NewTablePlan(tx, tblName, mdm),
		indexes: mdm.GetIndexInfo(tblName, tx),
	}
	tp.myschema = tp.myPlan.Schema()
	return tp
}

/*
Constructs a select plan for the table
The plan will use an indexselect if possible
*/
func (tp *TablePlanner) MakeSelectPlan() Plan {
	plan := tp.makeIndexSelect()
	if plan == nil {
		plan = tp.myPlan
	}
	return tp.addSelectPred(plan)
}

/*
Constructs a join plan of the specified plan and the table
The plan will use an indexjoin if possible
returns null if no join is possible
*/
func (tp *TablePlanner) MakeJoinPlan(current Plan) Plan {
	currSchema := current.Schema()
	// check if join predicate field is present in the 2 plans
	joinPred := tp.mypred.JoinSubPred(tp.myschema, currSchema)
	if joinPred == nil {
		return nil
	}

	plan := tp.makeIndexJoin(current, currSchema)
	if plan == nil {
		plan = tp.makeProductJoin(current, currSchema)
	}
	return plan
}

/*
Constructs a product plan of the specified plan and this table
*/
func (tp *TablePlanner) MakeProductPlan(current Plan) Plan {
	plan := tp.addSelectPred(tp.myPlan)
	return NewMultiBufferProductPlan(tp.tx, current, plan)
}

func (tp *TablePlanner) makeIndexSelect() Plan {
	for fldname, ii := range tp.indexes {
		val := tp.mypred.EquatesWithConstant(fldname)
		if val != nil {
			fmt.Println("index on " + fldname + " used\n")
			return NewIndexSelectPlan(tp.myPlan, ii, val)
		}
	}
	return nil
}

func (tp *TablePlanner) makeIndexJoin(current Plan, currsch *Schema) Plan {
	for fldname, ii := range tp.indexes {
		outerField := tp.mypred.EquatesWithField(fldname)
		var plan Plan
		if outerField != "" && currsch.HasField(outerField) {
			plan = NewIndexJoinPlan(current, tp.myPlan, ii, outerField)
			plan = tp.addSelectPred(plan)
			return tp.addJoinPred(plan, currsch)
		}
	}
	return nil
}

func (tp *TablePlanner) makeProductJoin(current Plan, currsch *Schema) Plan {
	plan := tp.MakeProductPlan(current)
	return tp.addJoinPred(plan, currsch)
}

func (tp *TablePlanner) addSelectPred(plan Plan) Plan {
	selectPred := tp.mypred.SelectSubPred(tp.myschema)
	if selectPred != nil {
		return NewSelectPlan(plan, selectPred)
	}
	return plan
}

func (tp *TablePlanner) addJoinPred(plan Plan, currsch *Schema) Plan {
	joinPred := tp.mypred.JoinSubPred(currsch, tp.myschema)
	if joinPred != nil {
		return NewSelectPlan(plan, joinPred)
	}
	return plan
}
