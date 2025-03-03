package index

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/query"
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
A static hash implementation of the index interface
A fixed number of buckets is allocated (100)
and each bucket is implemented as a file of index records
Each bucket is a separate table with (indexName+bucketNo)
*/

const NUM_BUCKETS = 100

type HashIndex struct {
	tx        *tx.Transaction
	indexName string
	layout    *record.Layout
	searchKey query.Constant
	tableScan *record.TableScan
}

/*
Opens a hash index for the specified index
*/
func NewHashIndex(tx *tx.Transaction, idxName string, layout *record.Layout) *HashIndex {
	return &HashIndex{
		tx:        tx,
		indexName: idxName,
		layout:    layout,
		tableScan: nil,
	}
}

/*
Positions the index before the first index record
having the specified search key
The method hashes the search key to determine the bucket,
and then opens a table scan on the file
corresponding to the bucket.
The table scan for the previous bucket is closed
*/
func (hi *HashIndex) BeforeFirst(searchKey query.Constant) {
	hi.Close()
	hi.searchKey = searchKey
	bucket := searchKey.HashCode() % NUM_BUCKETS
	indexTableName := fmt.Sprintf("%q%d", hi.indexName, bucket)
	hi.tableScan = record.NewTableScan(hi.tx, indexTableName, hi.layout)
}

/*
Moves to the next record having the search key
It loops through the table scan for the bucket
looking for a matching record, and returning false if there are
no such records
*/
func (hi *HashIndex) Next() bool {
	for hi.tableScan.Next() {
		if hi.tableScan.GetVal("dataval") == hi.searchKey {
			return true
		}
	}
	return false
}

/*
Retrieves the dataRID from the current record
in the table scan for the bucket
*/
func (hi *HashIndex) GetDataRID() record.RID {
	blockNum := hi.tableScan.GetInt("block")
	id := hi.tableScan.GetInt("id")
	return record.NewRID(blockNum, id)
}

/*
Inserts a new record into the table scan for the bucket
*/
func (hi *HashIndex) Insert(value query.Constant, rid record.RID) {
	hi.BeforeFirst(value)
	hi.tableScan.Insert()
	hi.tableScan.SetInt("block", rid.BlockNum())
	hi.tableScan.SetInt("id", rid.Slot())
	hi.tableScan.SetVal("dataval", value)
}

/*
Deletes the specified record from the table scan for the bucket
*/
func (hi *HashIndex) Delete(value query.Constant, rid record.RID) {
	hi.BeforeFirst(value)
	for hi.Next() {
		if hi.GetDataRID() == rid {
			hi.tableScan.Delete()
			return
		}
	}
}

/*
Closes the index by closing the current table scan
*/
func (hi *HashIndex) Close() {
	if hi.tableScan != nil {
		hi.tableScan.Close()
	}
}

/*
Returns the cost of searching an index file having the specified number of blocks
The method assumes that all buckets are about the same size
so the cost is simply the size of the bucket
*/
func (hi *HashIndex) SearchCost(numBlocks int, recPerBlock int) int {
	return numBlocks / NUM_BUCKETS
}
