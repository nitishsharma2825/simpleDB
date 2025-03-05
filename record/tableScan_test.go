package record

import (
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

func TestTableScan(t *testing.T) {
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

	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayout(sch)

	for _, fieldName := range layout.Schema().fields {
		offset := layout.Offset(fieldName)
		t.Logf("%q has offset %d\n", fieldName, offset)
	}

	t.Logf("Filling the page with 50 random records.\n")
	ts := NewTableScan(tx, "testfile", layout)
	for range 50 {
		ts.Insert()
		n := rand.Intn(50)
		ts.SetInt("A", n)
		ts.SetString("B", "rec"+strconv.Itoa(n))
		t.Logf("Inserting into slot %v: {%d, rec%d}\n", ts.GetRID(), n, n)
	}

	t.Logf("Deleting these records where A < 25\n")
	count := 0
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		if a < 25 {
			count++
			t.Logf("Deleting slot %v: {%d, %s}\n", ts.GetRID(), a, b)
			ts.Delete()
		}
	}
	t.Logf("Deleted %d records\n", count)

	t.Logf("Remaining records:\n")
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		t.Logf("slot %v: {%d, %s}\n", ts.GetRID(), a, b)
	}
	ts.Close()
	tx.Commit()
}
