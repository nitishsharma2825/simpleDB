package recovery

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

type SetStringRecord struct {
	txnum   int
	blockId file.BlockID
	offset  int
	val     string
}

func NewSetStringRecord(p *file.Page) *SetStringRecord {
	tpos := p.GetInt(file.IntBytes)

	fpos := tpos + file.IntBytes
	fileName := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(fileName))
	blockNum := p.GetInt(bpos)

	opos := bpos + file.IntBytes
	vpos := opos + file.IntBytes

	return &SetStringRecord{
		txnum:   p.GetInt(tpos),
		blockId: file.NewBlockID(fileName, blockNum),
		offset:  p.GetInt(opos),
		val:     p.GetString(vpos),
	}
}

func (ssr *SetStringRecord) Op() int {
	return SETSTRING
}

func (ssr *SetStringRecord) TxNumber() int {
	return ssr.txnum
}

func (ssr *SetStringRecord) Undo(txn *tx.Transaction) {
	txn.Pin(ssr.blockId)
	txn.SetString(ssr.blockId, ssr.offset, ssr.val, false) // don't log the undo
	txn.Unpin(ssr.blockId)
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

	record := make([]byte, vpos+file.IntBytes)
	page := file.NewPageWithSlice(record)

	page.SetInt(0, SETSTRING)
	page.SetInt(tpos, txnum)
	page.SetString(fpos, blockId.FileName())
	page.SetInt(bpos, blockId.BlockNumber())
	page.SetInt(opos, offset)
	page.SetString(vpos, val)

	return lm.Append(record)
}
