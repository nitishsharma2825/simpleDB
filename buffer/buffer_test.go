package buffer

import (
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

func TestBuffer(t *testing.T) {
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

	buff1, err := bm.Pin(file.NewBlockID(blockFile, 1))
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}

	p := buff1.Contents()

	pos := 80

	n := p.GetInt(pos)
	p.SetInt(pos, n+1)
	buff1.SetModified(1, 0) // placeholder values
	t.Logf("The new value is %d", (n + 1))
	bm.UnPin(buff1)

	// One of these pins will flush buff1 to disk
	buff2, err := bm.Pin(file.NewBlockID(blockFile, 2))
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}
	_, err = bm.Pin(file.NewBlockID(blockFile, 4))
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}
	_, err = bm.Pin(file.NewBlockID(blockFile, 4))
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}

	bm.UnPin(buff2)
	buff2, err = bm.Pin(file.NewBlockID(blockFile, 1))
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}
	p2 := buff2.Contents()

	expectedVal := p2.GetInt(pos)
	if expectedVal != (n + 1) {
		t.Fatalf("expected %d at offset %d, got %d", (n + 1), pos, expectedVal)
	}

	// this modification won't get written
	p2.SetInt(pos, 9999)
	buff2.SetModified(1, 0)

	t.Logf("offset %d contains %d", pos, p2.GetInt(pos))
}
