package record

/*
Interface implemented by each query plan
There is a plan class for each relational operator
*/

type Plan interface {
	/*
		Opens a scan corresponding to this plan
		The scan will be positioned before its first record
	*/
	Open() Scan

	/*
		Returns an estimate of the number of block accesses
		that will occur when the scan is read to completion
	*/
	BlocksAccessed() int

	/*
		Returns an estimate of the number of records in query's output table
	*/
	RecordsOutput() int

	/*
		Returns an estimate of the number of distinct values
		for the specified field in the query's output table
	*/
	DistinctValues(string) int

	/*
		Returns the schema of the query
	*/
	Schema() *Schema
}
