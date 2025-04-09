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

	s.fileSize = tx.Size(s.fileName)
	available := tx.AvailableBuffs()
	s.chunkSize = BestFactor(available, s.fileSize)
	s.BeforeFirst()
	return s
}





