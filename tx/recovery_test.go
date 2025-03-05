package tx

import (
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

const dbFolder = "../test_data"
const blockFile = "testfile"
const logFile = "logfile"
const blockSize = 400
const bufferPoolSize = 8

var fm *file.Manager
var lm *log.Manager
var bm *buffer.Manager

var blockId0 file.BlockID
var blockId1 file.BlockID

func TestRecovery(t *testing.T) {

	// Uncomment this while testing recovery
	t.Cleanup(func() {
		p1 := path.Join(dbFolder, blockFile)
		os.RemoveAll(path.Dir(p1))
	})

	fm = file.NewFileManager(dbFolder, blockSize)
	lm = log.NewLogManager(fm, logFile)
	bm = buffer.NewBufferManager(fm, lm, bufferPoolSize)

	blockId0 = file.NewBlockID(blockFile, 0)
	blockId1 = file.NewBlockID(blockFile, 1)

	if fm.Length(blockFile) == 0 {
		initialize(t)
		modify(t)
	} else {
		recover(t)
	}
}

func initialize(t *testing.T) {
	tx1 := NewTransaction(fm, lm, bm)
	tx2 := NewTransaction(fm, lm, bm)

	tx1.Pin(blockId0)
	tx2.Pin(blockId1)
	pos := 0
	for i := 0; i < 6; i++ {
		tx1.SetInt(blockId0, pos, pos, false)
		tx2.SetInt(blockId1, pos, pos, false)
		pos += file.IntBytes
	}
	tx1.SetString(blockId0, 30, "abc", false)
	tx2.SetString(blockId1, 30, "def", false)
	tx1.Commit()
	tx2.Commit()
	printValues(t, "After Initialization:")

}

func modify(t *testing.T) {
	tx3 := NewTransaction(fm, lm, bm)
	tx4 := NewTransaction(fm, lm, bm)

	tx3.Pin(blockId0)
	tx4.Pin(blockId1)
	pos := 0
	for i := 0; i < 6; i++ {
		tx3.SetInt(blockId0, pos, pos+100, true)
		tx4.SetInt(blockId1, pos, pos+100, true)
		pos += file.IntBytes
	}
	tx3.SetString(blockId0, 30, "uvw", true)
	tx4.SetString(blockId1, 30, "xyz", true)
	bm.FlushAll(tx3.txnum)
	bm.FlushAll(tx4.txnum)
	printValues(t, "After Modification:")

	tx3.Rollback()
	printValues(t, "After rollback:")
	// tx4 stops here without committing or rolling back,
	// so all its changes should be undone during recovery
}

func recover(t *testing.T) {
	tx := NewTransaction(fm, lm, bm)
	tx.Recover()
	printValues(t, "After recovery:")
}

func printValues(t *testing.T, msg string) {
	t.Log(msg)
	page0 := file.NewPageWithSize(fm.BlockSize())
	page1 := file.NewPageWithSize(fm.BlockSize())
	fm.Read(blockId0, page0)
	fm.Read(blockId1, page1)
	pos := 0
	for i := 0; i < 6; i++ {
		t.Logf("%d ", page0.GetInt(pos))
		t.Logf("%d ", page1.GetInt(pos))
		pos += file.IntBytes
	}
	t.Logf("%q ", page0.GetString(30))
	t.Logf("%q ", page1.GetString(30))
	t.Log("\n")
}
