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

func TestRecord(t *testing.T) {
	const dbFolder = "../test_data"
	const blockFile = "testfile"
	const logFile = "logfile"
	const blockSize = 400
	const bufferPoolSize = 8

	t.Cleanup(func() {
		p1 := path.Join(dbFolder, blockFile)
		p2 := path.Join(dbFolder, logFile)
		os.Remove(p1)
		os.Remove(p2)
		os.Remove(path.Dir(p1))
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

	blockId := tx.Append("testfile")
	tx.Pin(blockId)
	rp := NewRecordPage(tx, blockId, layout)
	rp.Format()

	t.Logf("Filling the page with random records.\n")

	slot := rp.InsertAfter(-1)
	for slot >= 0 {
		n := rand.Intn(50)
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", "rec"+strconv.Itoa(n))
		t.Logf("Inserting into slot %d: {%d, rec%d}\n", slot, n, n)
		slot = rp.InsertAfter(slot)
	}

	t.Logf("Deleting these records, whose A-values are less than 25.\n")

	count := 0
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		if a < 25 {
			count++
			t.Logf("slot %d: {%d, %s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}
	t.Logf("%d values under 25 were deleted\n", count)

	t.Logf("The remaining records are:\n")
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		t.Logf("slot %d: {%d, %s}\n", slot, a, b)
		slot = rp.NextAfter(slot)
	}
	tx.UnPin(blockId)
	tx.Commit()
}
