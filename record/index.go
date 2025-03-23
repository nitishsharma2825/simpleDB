package record

/*
Interface contains methods to traverse an index
*/
type Index interface {
	/*
		Positions the index before the 1st record
		having the specified search key
	*/
	BeforeFirst(Constant)

	/*
		Moves the index to the next record having the search key
		specified in the beforeFirst method
		Returns false if there are no such records
	*/
	Next() bool

	/*
		Return the dataRID value stored in the current index record
	*/
	GetDataRID() RID

	/*
		Inserts an index record having the specified dataval and dataRID values
	*/
	Insert(Constant, RID)

	/*
		Deletes the index record having the specified dataval and dataRID values
	*/
	Delete(Constant, RID)

	/*
		Closes the index
	*/
	Close()
}
