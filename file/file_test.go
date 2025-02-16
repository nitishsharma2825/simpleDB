package file

import (
	"os"
	"path"
	"testing"
)

func TestFile(t *testing.T) {
	const dbFolder = "../test_data"
	const blockFile = "testfile"
	const blockSize = 400

	t.Cleanup(func() {
		p := path.Join(dbFolder, blockFile)
		os.Remove(p)
	})

	fileManager := NewFileManager(dbFolder, blockSize)
	blockID := NewBlockID(blockFile, 2)
	page1 := NewPageWithSize(fileManager.blockSize)
	page2 := NewPageWithSize(fileManager.blockSize)

	pos := 88

	const val = "abcdefghijklm"
	const intv = 352

	page1.SetString(pos, val)

	pos2 := pos + MaxLength(len(val))

	page1.SetInt(pos2, intv)

	fileManager.Write(blockID, page1)
	fileManager.Read(blockID, page2)

	if got := page2.GetInt(pos2); got != intv {
		t.Fatalf("expected %d at offset %d, got %d", intv, pos2, got)
	}

	if got := page2.GetString(pos); got != val {
		t.Fatalf("expected %q at offset %d, got %q", val, pos, got)
	}

	t.Logf("offset %d contains %s", pos, page2.GetString(pos))
	t.Logf("offset %d contains %d", pos2, page2.GetInt(pos2))
}
