package record

/*
The parser for create index statement
*/

type CreateIndexData struct {
	IdxName string
	TblName string
	FldName string
}

func NewCreateIndexData(idxName, tblName, fldName string) *CreateIndexData {
	return &CreateIndexData{
		IdxName: idxName,
		TblName: tblName,
		FldName: fldName,
	}
}
