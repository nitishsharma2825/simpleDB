package tx

import (
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

func TestTransaction(t *testing.T) {
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

	tx1 := NewTransaction(fm, lm, bm)
	blockId := file.NewBlockID(blockFile, 1)
	tx1.Pin(blockId)
	// The block initially contains unknown bytes,
	// so don't log those values here.
	tx1.SetInt(blockId, 80, 1, false)
	tx1.SetString(blockId, 40, "one", false)
	tx1.Commit()

	tx2 := NewTransaction(fm, lm, bm)
	tx2.Pin(blockId)
	ival := tx2.GetInt(blockId, 80)
	sval := tx2.GetString(blockId, 40)
	t.Logf("Initial value at location 80 = %d\n", ival)
	t.Logf("Initial value at location 40 = %q\n", sval)
	newival := ival + 1
	newsval := sval + "!"
	tx2.SetInt(blockId, 80, newival, true)
	tx2.SetString(blockId, 40, newsval, true)
	tx2.Commit()

	tx3 := NewTransaction(fm, lm, bm)
	tx3.Pin(blockId)
	t.Logf("new value at location 80 = %d", tx3.GetInt(blockId, 80))
	t.Logf("new value at location 40 = %q", tx3.GetString(blockId, 40))
	tx3.SetInt(blockId, 80, 9999, true)
	t.Logf("pre-rollback value at location 80 = %d\n", tx3.GetInt(blockId, 80))
	tx3.Rollback()

	tx4 := NewTransaction(fm, lm, bm)
	tx4.Pin(blockId)
	t.Logf("post-rollback at location 80 = %d\n", tx4.GetInt(blockId, 80))
	tx4.Commit()
}
