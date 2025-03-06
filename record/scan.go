package record

/*
This interface will be implemented by each query scan
There is a scan class for each relational algebra operator
*/
type Scan interface {
	/*
		Positions the scan before its first record
		A subsequent call to next() will return the first record
	*/
	BeforeFirst()
	/*
		Move the scan to the next record
	*/
	Next() bool
	/*
		Return the value of the specified integer field in the current record
	*/
	GetInt(string) int
	/*
		Return the value of the specified string field in the current record
	*/
	GetString(string) string
	/*
		Return the value of specified field in the current record
		The value is expressed as Constant
	*/
	GetVal(string) Constant
	/*
		Return true if the scan has the specified field
	*/
	HasField(string) bool
	/*
		Close the scan and its subscans if any
	*/
	Close()
}
