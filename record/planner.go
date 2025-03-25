package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
The object that executes SQL statements
*/

type Planner struct {
	qplanner QueryPlanner
	uplanner UpdatePlanner
}

func NewPlanner(qplanner QueryPlanner, uplanner UpdatePlanner) *Planner {
	return &Planner{
		qplanner: qplanner,
		uplanner: uplanner,
	}
}

/*
Create plan for SQL select statement using the supplied planner
*/
func (p *Planner) CreateQueryPlan(query string, tx *tx.Transaction) Plan {
	parser := NewParser(query)
	data, err := parser.Query()
	if err != nil {
		panic(err)
	}
	p.verifyQuery(data)
	return p.qplanner.CreatePlan(data, tx)
}

/*
Execute SQL insert, delete, modify, update statement
*/
func (p *Planner) ExecuteUpdate(cmd string, tx *tx.Transaction) int {
	parser := NewParser(cmd)
	data, err := parser.UpdateCmd()
	if err != nil {
		panic(err)
	}
	p.verifyUpdate(data)
	switch d := data.(type) {
	case InsertData:
		return p.uplanner.ExecuteInsert(&d, tx)
	case DeleteData:
		return p.uplanner.ExecuteDelete(&d, tx)
	case ModifyData:
		return p.uplanner.ExecuteModify(&d, tx)
	case CreateTableData:
		return p.uplanner.ExecuteCreateTable(&d, tx)
	case CreateIndexData:
		return p.uplanner.ExecuteCreateIndex(&d, tx)
	case CreateViewData:
		return p.uplanner.ExecuteCreateView(&d, tx)
	}
	return 0
}

// Should verify query using metadata
func (p *Planner) verifyQuery(data *QueryData) {
}

// Should verify update query using metadata
func (p *Planner) verifyUpdate(data interface{}) {
}
