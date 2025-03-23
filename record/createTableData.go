package record

/*
Data for the create table statement
*/

type CreateTableData struct {
	TblName string
	Schema  *Schema
}

func NewCreateTableData(tblName string, schema *Schema) *CreateTableData {
	return &CreateTableData{
		TblName: tblName,
		Schema:  schema,
	}
}
