package sql

import "github.com/nitishsharma2825/simpleDB/record"

/*
Data for the create table statement
*/

type CreateTableData struct {
	TblName string
	Schema  *record.Schema
}

func NewCreateTableData(tblName string, schema *record.Schema) *CreateTableData {
	return &CreateTableData{
		TblName: tblName,
		Schema:  schema,
	}
}
