package query

import "github.com/nitishsharma2825/simpleDB/record"

/*
Interface implemented by all updateable scans
*/

type UpdateScan interface {
	/*
		Modify the field value of the current record
	*/
	SetVal(fieldName string, val Constant)

	/*
		Modify the integer field value of the current record
	*/
	SetInt(fieldName string, val int)

	/*
		Modify the string field value of the current record
	*/
	SetString(fieldName string, val string)

	/*
		Insert a new record somewhere in the scan
	*/
	Insert()

	/*
		Delete the current record from the scan
	*/
	Delete()

	/*
		Return the ID of the current record
	*/
	GetRID() record.RID

	/*
		Position the scan so that current record has the specified ID
	*/
	MoveToRid(rid record.RID)
}
