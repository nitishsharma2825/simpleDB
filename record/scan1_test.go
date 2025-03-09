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

func TestScan1(t *testing.T) {
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

	layout := NewLayout(sch1)
	s1 := NewTableScan(tx, "T", layout)

	s1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d random records\n", n)
	for range n {
		s1.Insert()
		k := rand.Intn(50)
		s1.SetInt("A", k)
		s1.SetString("B", "rec"+strconv.Itoa(k))
		t.Logf("Inserting into slot %v: {%d, rec%d}\n", s1.GetRID(), k, k)
	}
	s1.Close()

	s2 := NewTableScan(tx, "T", layout)
	// selecting all records where A=10
	constant1 := NewIntConstant(10)
	term1 := NewTerm(NewExpressionWithField("A"), NewExpressionWithConstant(constant1))
	pred := NewPredicateWithTerm(term1)

	t.Logf("The predicate is %q\n", pred.ToString())

	s3 := NewSelectScan(s2, pred)
	fields := []string{"B"}
	s4 := NewProjectScan(s3, fields)
	for s4.Next() {
		val, err := s4.GetString("B")
		if err != nil {
			panic(err)
		}
		t.Logf("%q\n", val)
	}
	s4.Close()
	tx.Commit()
}
