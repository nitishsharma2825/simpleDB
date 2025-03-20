package sql

import "github.com/nitishsharma2825/simpleDB/record"

/*
Data for insert statement
*/

type InsertData struct {
	TblName string
	Fields  []string
	Vals    []*record.Constant
}

func NewInsertData(tblName string, fields []string, vals []*record.Constant) *InsertData {
	return &InsertData{
		TblName: tblName,
		Fields:  fields,
		Vals:    vals,
	}
}
