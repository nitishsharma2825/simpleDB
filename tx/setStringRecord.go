package tx

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

type SetStringRecord struct {
	txnum   int
	blockId file.BlockID
	offset  int
	val     string
}

func NewSetStringRecord(p *file.Page) *SetStringRecord {
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
	val := p.GetString(vpos)

	return &SetStringRecord{
		txnum:   txnum,
		blockId: blockId,
		offset:  offset,
		val:     val,
	}
}

func (ssr *SetStringRecord) Op() int {
	return SETSTRING
}

func (ssr *SetStringRecord) TxNumber() int {
	return ssr.txnum
}

func (ssr *SetStringRecord) Undo(txn *Transaction) {
	txn.Pin(ssr.blockId)
	txn.SetString(ssr.blockId, ssr.offset, ssr.val, false) // don't log the undo
	txn.UnPin(ssr.blockId)
}

func (ssr *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %q>", ssr.txnum, ssr.blockId.String(), ssr.offset, ssr.val)
}

func WriteSetStringRecordToLog(lm *log.Manager, txnum int, blockId file.BlockID, offset int, val string) int {
	tpos := file.IntBytes
	fpos := tpos + file.IntBytes
	bpos := fpos + file.MaxLength(len(blockId.FileName()))
	opos := bpos + file.IntBytes
	vpos := opos + file.IntBytes

	record := make([]byte, vpos+file.MaxLength(len(val)))
	page := file.NewPageWithSlice(record)

	page.SetInt(0, SETSTRING)
	page.SetInt(tpos, txnum)
	page.SetString(fpos, blockId.FileName())
	page.SetInt(bpos, blockId.BlockNumber())
	page.SetInt(opos, offset)
	page.SetString(vpos, val)

	return lm.Append(record)
}
