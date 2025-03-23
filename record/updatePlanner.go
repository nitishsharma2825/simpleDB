package record

import (
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
The interface implemented by planners for SQL insert, delete and modify statements
*/

type UpdatePlanner interface {
	/*
		Execute the specified insert statement,
		and returns number of affected records
	*/
	ExecuteInsert(*InsertData, *tx.Transaction) int

	/*
		Execute the specified delete statement,
		and return the number of affected records
	*/
	ExecuteDelete(*DeleteData, *tx.Transaction) int

	/*
		Execute the specified modify statement,
		and return the number of affected records
	*/
	ExecuteModify(*ModifyData, *tx.Transaction) int

	/*
		Execute the specified create table statement,
		and return the number of affected records
	*/
	ExecuteCreateTable(*CreateTableData, *tx.Transaction) int

	/*
		Execute the specified create view statement,
		and return the number of affected records
	*/
	ExecuteCreateView(*CreateViewData, *tx.Transaction) int

	/*
		Execute the specified create index statement,
		and return the number of affected records
	*/
	ExecuteCreateIndex(*CreateIndexData, *tx.Transaction) int
}
