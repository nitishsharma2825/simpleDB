package record

type ModifyData struct {
	TblName string
	FldName string
	NewVal  *Expression
	Pred    *Predicate
}

func NewModifyData(tblName, fldName string, newVal *Expression, pred *Predicate) *ModifyData {
	return &ModifyData{
		TblName: tblName,
		FldName: fldName,
		NewVal:  newVal,
		Pred:    pred,
	}
}
