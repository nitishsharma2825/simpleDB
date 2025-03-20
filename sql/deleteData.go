package sql

import "github.com/nitishsharma2825/simpleDB/record"

/*
Data for delete statement
*/

type DeleteData struct {
	TblName string
	Pred    *record.Predicate
}

func NewDeleteData(tblName string, pred *record.Predicate) *DeleteData {
	return &DeleteData{
		TblName: tblName,
		Pred:    pred,
	}
}
