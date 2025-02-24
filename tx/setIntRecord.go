package tx

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

type SetIntRecord struct {
	txnum   int
	blockId file.BlockID
	offset  int
	val     int
}

func NewSetIntRecord(p *file.Page) *SetIntRecord {
	tpos := file.IntBytes
	txnum := p.GetInt(tpos)

	fpos := tpos + file.IntBytes
	filename := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(filename))
	blockNum := p.GetInt(bpos)
	blockId := file.NewBlockID(filename, blockNum)

	opos := bpos + file.IntBytes
	offset := p.GetInt(opos)

	vpos := opos + file.IntBytes
	val := p.GetInt(vpos)

	return &SetIntRecord{
		txnum:   txnum,
		blockId: blockId,
		offset:  offset,
		val:     val,
	}
}

func (sir *SetIntRecord) Op() int {
	return SETINT
}

func (sir *SetIntRecord) TxNumber() int {
	return sir.txnum
}

func (sir *SetIntRecord) Undo(txn *Transaction) {
	txn.Pin(sir.blockId)
	txn.SetInt(sir.blockId, sir.offset, sir.val, false) // don't log the undo
	txn.UnPin(sir.blockId)
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
