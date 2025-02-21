package buffer

import (
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

func TestBufferPoolManager(t *testing.T) {
	const dbFolder = "../test_data"
	const blockFile = "testfile"
	const logFile = "logfile"
	const blockSize = 400
	const bufferPoolSize = 3

	t.Cleanup(func() {
		p1 := path.Join(dbFolder, blockFile)
		p2 := path.Join(dbFolder, logFile)
		os.Remove(p1)
		os.Remove(p2)
		os.Remove(path.Dir(p1))
	})

	fm := file.NewFileManager(dbFolder, blockSize)
	lm := log.NewLogManager(fm, logFile)
	bm := NewBufferManager(fm, lm, bufferPoolSize)

	buff := make([]*Buffer, 6)
	var err error
	buff[0], err = bm.Pin(file.NewBlockID(blockFile, 0))
	if err != nil {
		t.Logf("failure during trying to pin block 0: %q", err.Error())
	}
	buff[1], err = bm.Pin(file.NewBlockID(blockFile, 1))
	if err != nil {
		t.Logf("failure during trying to pin block 1: %q", err.Error())
	}
	buff[2], err = bm.Pin(file.NewBlockID(blockFile, 2))
	if err != nil {
		t.Logf("failure during trying to pin block 2: %q", err.Error())
	}

	bm.UnPin(buff[1])
	buff[1] = nil

	buff[3], err = bm.Pin(file.NewBlockID(blockFile, 0)) // block 0 pinned twice
	if err != nil {
		t.Logf("failure during trying to pin block 0: %q", err.Error())
	}

	buff[4], err = bm.Pin(file.NewBlockID(blockFile, 1)) // block 1 repinned
	if err != nil {
		t.Logf("failure during trying to pin block 1: %q", err.Error())
	}

	t.Logf("Available Buffers: %d", bm.Available())
	t.Logf("Attempting to pin block 3")
	buff[5], err = bm.Pin(file.NewBlockID(blockFile, 3))
	if err != nil {
		t.Logf("failure during trying to pin block 3: %q", err.Error())
	}

	bm.UnPin(buff[2])
	buff[2] = nil

	buff[5], err = bm.Pin(file.NewBlockID(blockFile, 3)) // now this works
	if err != nil {
		t.Logf("failure during trying to pin block 3: %q", err.Error())
	}

	t.Logf("Final Buffer Allocation")
	for i := 0; i < len(buff); i++ {
		b := buff[i]
		if b != nil {
			t.Logf("buff[%d] pinned to block %q", i, b.Block().String())
		}
	}
}
