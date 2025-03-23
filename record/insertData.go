package record

/*
Data for insert statement
*/

type InsertData struct {
	TblName string
	Fields  []string
	Vals    []*Constant
}

func NewInsertData(tblName string, fields []string, vals []*Constant) *InsertData {
	return &InsertData{
		TblName: tblName,
		Fields:  fields,
		Vals:    vals,
	}
}
