package record

import (
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

func TestProductScan(t *testing.T) {
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

	sch1 := NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1 := NewLayout(sch1)
	ts1 := NewTableScan(tx, "T1", layout1)

	sch2 := NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := NewLayout(sch2)
	ts2 := NewTableScan(tx, "T2", layout2)

	ts1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d records into T1\n", n)
	for i := 0; i < n; i++ {
		ts1.Insert()
		ts1.SetInt("A", i)
		ts1.SetString("B", "aaa"+strconv.Itoa(i))
	}
	ts1.Close()

	ts2.BeforeFirst()
	t.Logf("Inserting %d records into T2\n", n)
	for i := 0; i < n; i++ {
		ts2.Insert()
		ts2.SetInt("C", n-i-1)
		ts2.SetString("D", "bbb"+strconv.Itoa(n-i-1))
	}
	ts2.Close()

	s1 := NewTableScan(tx, "T1", layout1)
	s2 := NewTableScan(tx, "T2", layout2)
	s3 := NewProductScan(s1, s2)
	cnt := 0
	for s3.Next() {
		cnt++
		t.Logf("%q\n", s3.GetString("B"))
	}
	t.Logf("Total no of records = %d\n", cnt)
	if cnt != n*n {
		t.Fatalf("Product failed expected=%d, got=%d", n*n, cnt)
	}
	s3.Close()
	tx.Commit()
}
