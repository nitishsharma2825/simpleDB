package record

/*
Holds 3 pieces of information about a table:
- number of blocks
- number of records
- number of distinct value for each fields
*/
type StatInfo struct {
	numBlocks int
	numRecs   int
}

func NewStatInfo(numBlocks, numRecs int) StatInfo {
	return StatInfo{
		numBlocks: numBlocks,
		numRecs:   numRecs,
	}
}

// return the estimated number of blocks in the table
func (si StatInfo) BlocksAccessed() int {
	return si.numBlocks
}

// return the estimated number of records in the table
func (si StatInfo) RecordsOutput() int {
	return si.numRecs
}

// return the estimated number of distinct values for the specified field
// this estimate is complete guess
func (si StatInfo) DistinctValues(fieldName string) int {
	return 1 + (si.numRecs / 3)
}
