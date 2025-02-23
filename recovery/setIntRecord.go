package recovery

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type SetIntRecord struct {
	txnum   int
	blockId file.BlockID
	offset  int
	val     int
}

func NewSetIntRecord(p *file.Page) *SetIntRecord {
	tpos := p.GetInt(file.IntBytes)

	fpos := tpos + file.IntBytes
	fileName := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(fileName))
	blockNum := p.GetInt(bpos)

	opos := bpos + file.IntBytes
	vpos := opos + file.IntBytes

	return &SetIntRecord{
		txnum:   p.GetInt(tpos),
		blockId: file.NewBlockID(fileName, blockNum),
		offset:  p.GetInt(opos),
		val:     p.GetInt(vpos),
	}
}

func (sir *SetIntRecord) Op() int {
	return SETINT
}

func (sir *SetIntRecord) TxNumber() int {
	return sir.txnum
}

func (sir *SetIntRecord) Undo(txn *tx.Transaction) {
	txn.Pin(sir.blockId)
	txn.SetInt(sir.blockId, sir.offset, sir.val, false) // don't log the undo
	txn.Unpin(sir.blockId)
}

func (sir *SetIntRecord) ToString() string {
	return fmt.Sprintf("<SETINT %d %v %d %d>", sir.txnum, sir.blockId.String(), sir.offset, sir.val)
}

func WriteSetIntRecordToLog(lm *log.Manager, txnum int, blockId file.BlockID, offset int, val int) int {
	tpos := file.IntBytes
	fpos := tpos + file.IntBytes
	bpos := fpos + file.MaxLength(len(blockId.FileName()))
	opos := bpos + file.IntBytes
	vpos := opos + file.IntBytes

	record := make([]byte, vpos+file.IntBytes)
	page := file.NewPageWithSlice(record)

	page.SetInt(0, SETINT)
	page.SetInt(tpos, txnum)
	page.SetString(fpos, blockId.FileName())
	page.SetInt(bpos, blockId.BlockNumber())
	page.SetInt(opos, offset)
	page.SetInt(vpos, val)

	return lm.Append(record)
}
