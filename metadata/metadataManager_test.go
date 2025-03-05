package metadata

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/record"
	"github.com/nitishsharma2825/simpleDB/tx"
)

func TestMetadataManager(t *testing.T) {
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
	mdm := NewMetadataManager(true, tx)

	schema1 := record.NewSchema()
	schema1.AddIntField("A")
	schema1.AddStringField("B", 9)

	// Part 1: Table metadata
	mdm.CreateTable("MyTable", schema1, tx)
	layout := mdm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	schema2 := layout.Schema()

	t.Logf("MyTable has slot size: %d\n", size)
	t.Logf("Its fields are: \n")
	for _, fieldName := range schema2.Fields() {
		var fldType string
		if schema2.FieldType(fieldName) == record.INTEGER {
			fldType = "int"
		} else {
			strlen := schema2.Length(fieldName)
			fldType = fmt.Sprintf("varchar(%d)", strlen)
		}
		t.Logf("%q: %q\n", fieldName, fldType)
	}

	// Part 2: Statistics Metadata
	ts := record.NewTableScan(tx, "MyTable", layout)
	for range 50 {
		ts.Insert()
		n := rand.Intn(50)
		ts.SetInt("A", n)
		ts.SetString("B", "rec"+strconv.Itoa(n))
		t.Logf("Inserting into slot %v: {%d, rec%d}\n", ts.GetRID(), n, n)
	}
	statInfo := mdm.GetStatInfo("MyTable", layout, tx)
	t.Logf("B(MyTable) = %d\n", statInfo.BlocksAccessed())
	t.Logf("R(MyTable) = %d\n", statInfo.RecordsOutput())
	t.Logf("V(MyTable, A) = %d\n", statInfo.DistinctValues("A"))
	t.Logf("V(MyTable, B) = %d\n", statInfo.DistinctValues("B"))

	// Part 3: View Metadata
	viewDef := "select B from MyTable where A = 1"
	mdm.CreateView("viewA", viewDef, tx)
	v := mdm.GetViewDef("viewA", tx)
	t.Logf("View def = %q\n", v)

	// Part 4: Index Metadata
	mdm.CreateIndex("indexA", "MyTable", "A", tx)
	mdm.CreateIndex("indexB", "MyTable", "B", tx)
	idxMap := mdm.GetIndexInfo("MyTable", tx)

	indexInfo := idxMap["A"]
	t.Logf("B(indexA) = %d\n", indexInfo.BlocksAccessed())
	t.Logf("R(indexA) = %d\n", indexInfo.RecordsOutput())
	t.Logf("V(indexA, A) = %d\n", indexInfo.DistinctValues("A"))
	t.Logf("V(indexA, B) = %d\n", indexInfo.DistinctValues("B"))

	indexInfo = idxMap["B"]
	t.Logf("B(indexB) = %d\n", indexInfo.BlocksAccessed())
	t.Logf("R(indexB) = %d\n", indexInfo.RecordsOutput())
	t.Logf("V(indexB, A) = %d\n", indexInfo.DistinctValues("A"))
	t.Logf("V(indexB, B) = %d\n", indexInfo.DistinctValues("B"))
	tx.Commit()
}
