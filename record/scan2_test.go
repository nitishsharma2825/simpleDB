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

func TestScan2(t *testing.T) {
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
	us1 := NewTableScan(tx, "T1", layout1)

	us1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d records into T1\n", n)
	for i := 0; i < n; i++ {
		us1.Insert()
		us1.SetInt("A", i)
		us1.SetString("B", "bbb"+strconv.Itoa(i))
	}
	us1.Close()

	sch2 := NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := NewLayout(sch2)
	us2 := NewTableScan(tx, "T2", layout2)

	us2.BeforeFirst()
	t.Logf("Inserting %d records into T2\n", n)
	for i := 0; i < n; i++ {
		us2.Insert()
		us2.SetInt("C", n-i-1)
		us2.SetString("D", "ddd"+strconv.Itoa(n-i-1))
	}
	us2.Close()

	s1 := NewTableScan(tx, "T1", layout1)
	s2 := NewTableScan(tx, "T2", layout2)
	s3 := NewProductScan(s1, s2)
	// selecting all records where A=C
	term1 := NewTerm(NewExpressionWithField("A"), NewExpressionWithField("C"))
	pred := NewPredicateWithTerm(term1)
	t.Logf("The predicate is %q\n", pred.ToString())
	s4 := NewSelectScan(s3, pred)

	// projecting on [B, D]
	fields := []string{"B", "D"}
	s5 := NewProjectScan(s4, fields)
	for s5.Next() {
		lhs := s5.GetString("B")
		if lhs == "" {
			panic("field not found")
		}
		rhs := s5.GetString("D")
		if rhs == "" {
			panic("field not found")
		}
		t.Logf("%q %q\n", lhs, rhs)
	}
	s5.Close()
	tx.Commit()
}
