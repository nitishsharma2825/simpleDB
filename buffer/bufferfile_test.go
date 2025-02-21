package buffer

import (
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

func TestBufferFile(t *testing.T) {
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
	bm := NewBufferManager(fm, lm, bufferPoolSize)

	blockId := file.NewBlockID(blockFile, 2)

	pos1 := 88

	b1, err := bm.Pin(blockId)
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}

	p1 := b1.Contents()
	val1 := "abcdefghijklm"
	p1.SetString(pos1, val1)

	size := file.MaxLength(len(val1))

	pos2 := pos1 + size
	val2 := 345
	p1.SetInt(pos2, val2)

	b1.SetModified(1, 0)
	bm.UnPin(b1)

	b2, err := bm.Pin(blockId)
	if err != nil {
		t.Fatalf("Client timedout waiting for a buffer to be assigned")
	}

	p2 := b2.Contents()

	expectedVal2 := p2.GetInt(pos2)
	if expectedVal2 != val2 {
		t.Fatalf("expected %d at offset %d, got %d", val2, pos2, expectedVal2)
	}

	expectedVal1 := p2.GetString(pos1)
	if expectedVal1 != val1 {
		t.Fatalf("expected %q at offset %d, got %q", val1, pos1, expectedVal1)
	}

	bm.UnPin(b2)

	t.Logf("offset %d contains %s", pos1, expectedVal1)
	t.Logf("offset %d contains %d", pos2, expectedVal2)
}
