package record

/*
This interface is implemented by all updateable scans
*/
type UpdateScan interface {
	/*
		Modify the field value of the current record
	*/
	SetVal(string, Constant)
	/*
		Modify the field value of the current record
	*/
	SetInt(string, int)
	/*
		Modify the field value of the current record
	*/
	SetString(string, string)
	/*
		Insert a new record somewhere in the scan
	*/
	Insert()
	/*
		Delete the current record from the scan
	*/
	Delete()
	/*
		Return the id of the current record
	*/
	GetRID() RID
	/*
		Position the scan so that the current record has the specified id
	*/
	MoveToRID(RID)
}
