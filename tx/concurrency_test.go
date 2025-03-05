package tx

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/nitishsharma2825/simpleDB/buffer"
	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

func TestConcurrency(t *testing.T) {
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

	go txA(t, fm, lm, bm, blockFile)
	go txB(t, fm, lm, bm, blockFile)
	go txC(t, fm, lm, bm, blockFile)
}

func txA(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager, blockFile string) {
	tx := NewTransaction(fm, lm, bm)
	blockId1 := file.NewBlockID(blockFile, 1)
	blockId2 := file.NewBlockID(blockFile, 2)
	var err error
	err = tx.Pin(blockId1)
	if err != nil {
		panic(err)
	}
	err = tx.Pin(blockId2)
	if err != nil {
		panic(err)
	}

	t.Log("Tx A: request slock 1")
	tx.GetInt(blockId1, 0)
	t.Log("Tx A: receive slock 1")
	time.Sleep(time.Second)
	t.Log("Tx A: request slock 2")
	tx.GetInt(blockId2, 0)
	t.Log("Tx A: receive slock 2")
	tx.Commit()
	t.Log("Tx A: commit")
}

func txB(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager, blockFile string) {
	tx := NewTransaction(fm, lm, bm)
	blockId1 := file.NewBlockID(blockFile, 1)
	blockId2 := file.NewBlockID(blockFile, 2)
	var err error
	err = tx.Pin(blockId1)
	if err != nil {
		panic(err)
	}
	err = tx.Pin(blockId2)
	if err != nil {
		panic(err)
	}

	t.Log("Tx B: request xlock 2")
	tx.SetInt(blockId2, 0, 0, false)
	t.Log("Tx B: receive xlock 2")
	time.Sleep(time.Second)
	t.Log("Tx B: request slock 1")
	tx.GetInt(blockId1, 0)
	t.Log("Tx B: receive slock 1")
	tx.Commit()
	t.Log("Tx B: commit")
}

func txC(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager, blockFile string) {
	tx := NewTransaction(fm, lm, bm)
	blockId1 := file.NewBlockID(blockFile, 1)
	blockId2 := file.NewBlockID(blockFile, 2)
	var err error
	err = tx.Pin(blockId1)
	if err != nil {
		panic(err)
	}
	err = tx.Pin(blockId2)
	if err != nil {
		panic(err)
	}

	time.Sleep(500 * time.Millisecond)
	t.Log("Tx C: request xlock 1")
	tx.SetInt(blockId1, 0, 0, false)
	t.Log("Tx C: receive xlock 1")
	time.Sleep(time.Second)
	t.Log("Tx C: request slock 2")
	tx.GetInt(blockId2, 0)
	t.Log("Tx C: receive slock 2")
	tx.Commit()
	t.Log("Tx C: commit")
}
