package sql

import "github.com/nitishsharma2825/simpleDB/record"

type ModifyData struct {
	TblName string
	FldName string
	NewVal  *record.Expression
	Pred    *record.Predicate
}

func NewModifyData(tblName, fldName string, newVal *record.Expression, pred *record.Predicate) *ModifyData {
	return &ModifyData{
		TblName: tblName,
		FldName: fldName,
		NewVal:  newVal,
		Pred:    pred,
	}
}
