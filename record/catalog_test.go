package record

import (
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

func TestCatalog(t *testing.T) {
	const dbFolder = "../test_data"
	const blockFile = "testfile"
	const logFile = "logfile"
	const blockSize = 400
	const bufferPoolSize = 8

	t.Cleanup(func() {
		p1 := path.Join(dbFolder, blockFile)
		os.RemoveAll(path.Dir(p1))
	})

	fm := file.NewFileManager(dbFolder, blockSize)
	lm := log.NewLogManager(fm, logFile)
	bm := buffer.NewBufferManager(fm, lm, bufferPoolSize)

	tx := tx.NewTransaction(fm, lm, bm)
	tableManager := NewTableManager(true, tx) // keep it false if data files exist
	tcatLayout := tableManager.GetLayout("tblcat", tx)

	t.Logf("Here are all the tables and their lengths.\n")
	ts := NewTableScan(tx, "tblcat", tcatLayout)
	for ts.Next() {
		tname := ts.GetString("tblname")
		slotSize := ts.GetInt("slotsize")
		t.Logf("%q %d\n", tname, slotSize)
	}
	ts.Close()

	t.Logf("Here are the fields for each table and their offset")
	fcatLayout := tableManager.GetLayout("fldcat", tx)
	ts = NewTableScan(tx, "fldcat", fcatLayout)
	for ts.Next() {
		tname := ts.GetString("tblname")
		fname := ts.GetString("fldname")
		offset := ts.GetInt("offset")
		t.Logf("%q %q %d", tname, fname, offset)
	}
	ts.Close()
}
