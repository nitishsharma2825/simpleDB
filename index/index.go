package index

import (
	"github.com/nitishsharma2825/simpleDB/query"
	"github.com/nitishsharma2825/simpleDB/record"
)

/*
Interface contains methods to traverse an index
*/
type Index interface {
	/*
		Positions the index before the 1st record
		having the specified search key
	*/
	BeforeFirst(query.Constant)

	/*
		Moves the index to the next record having the search key
		specified in the beforeFirst method
		Returns false if there are no such records
	*/
	Next() bool

	/*
		Return the dataRID value stored in the current index record
	*/
	GetDataRID() record.RID

	/*
		Inserts an index record having the specified dataval and dataRID values
	*/
	Insert(query.Constant, record.RID)

	/*
		Deletes the index record having the specified dataval and dataRID values
	*/
	Delete(query.Constant, record.RID)

	/*
		Closes the index
	*/
	Close()
}
