package record

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
	"github.com/nitishsharma2825/simpleDB/tx"
)

func TestTableManager(t *testing.T) {
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

	tableMgr := NewTableManager(true, tx)
	schema := NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	tableMgr.CreateTable("MyTable", schema, tx)

	layout := tableMgr.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	schema2 := layout.Schema()

	t.Logf("MyTable has slot size %d\n", size)
	t.Logf("Its fields are \n")

	for _, fieldName := range schema2.Fields() {
		var fldType string
		if schema2.FieldType(fieldName) == INTEGER {
			fldType = "int"
		} else {
			strlen := schema2.Length(fieldName)
			fldType = fmt.Sprintf("varchar(%d)", strlen)
		}
		t.Logf("%q: %q\n", fieldName, fldType)
	}
	tx.Commit()
}
