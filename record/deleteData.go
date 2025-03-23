package record

/*
Data for delete statement
*/

type DeleteData struct {
	TblName string
	Pred    *Predicate
}

func NewDeleteData(tblName string, pred *Predicate) *DeleteData {
	return &DeleteData{
		TblName: tblName,
		Pred:    pred,
	}
}
