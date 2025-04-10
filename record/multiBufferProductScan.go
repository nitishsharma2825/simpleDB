package record

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/tx"
)

/*
Scan class for the multi buffer version of the product operator
*/
type MultiBufferProductScan struct {
	tx                                *tx.Transaction
	lhsScan, rhsScan, prodScan        Scan
	fileName                          string
	layout                            *Layout
	chunkSize, nextBlockNum, fileSize int
}

func NewMultiBufferProductScan(tx *tx.Transaction, lhsScan Scan, tableName string, layout *Layout) *MultiBufferProductScan {
	s := &MultiBufferProductScan{
		tx:       tx,
		lhsScan:  lhsScan,
		fileName: fmt.Sprintf("%q.tbl", tableName),
		layout:   layout,
		rhsScan:  nil,
	}

	// this file is of temp table from rhs table
	s.fileSize = tx.Size(s.fileName)
	available := tx.AvailableBuffs()
	s.chunkSize = BestFactor(available, s.fileSize)
	s.BeforeFirst()
	return s
}

/*
Positions the scan before the 1st record.
LHS scan is positioned at its 1st record,
RHS scan is positioned before the 1st record of the 1st chunk
*/
func (ps *MultiBufferProductScan) BeforeFirst() {
	ps.nextBlockNum = 0 // of the rhs table
	ps.useNextChunk()
}

/*
Moves to the next record in the current scan
If there are no more records in the current chunk,
them move to the LHS record and beginning of that chunk
If there are no more LHS records, then move to the next chunk and begin again
*/
func (ps *MultiBufferProductScan) Next() bool {
	for !ps.prodScan.Next() {
		if !ps.useNextChunk() {
			return false
		}
	}
	return true
}

func (ps *MultiBufferProductScan) Close() {
	ps.prodScan.Close()
}

func (ps *MultiBufferProductScan) GetVal(fldname string) Constant {
	return ps.prodScan.GetVal(fldname)
}

func (ps *MultiBufferProductScan) GetInt(fldname string) int {
	return ps.prodScan.GetInt(fldname)
}

func (ps *MultiBufferProductScan) GetString(fldname string) string {
	return ps.prodScan.GetString(fldname)
}

func (ps *MultiBufferProductScan) HasField(fldname string) bool {
	return ps.prodScan.HasField(fldname)
}

func (ps *MultiBufferProductScan) useNextChunk() bool {
	if ps.nextBlockNum >= ps.fileSize {
		return false
	}

	// rhsScan represents the current chunk
	if ps.rhsScan != nil {
		ps.rhsScan.Close()
	}

	end := ps.nextBlockNum + ps.chunkSize - 1
	if end >= ps.fileSize {
		end = ps.fileSize - 1
	}
	ps.rhsScan = NewChunkScan(ps.tx, ps.fileName, ps.layout, ps.nextBlockNum, end)
	ps.lhsScan.BeforeFirst()
	ps.prodScan = NewProductScan(ps.lhsScan, ps.rhsScan)
	ps.nextBlockNum = end + 1
	return true
}
