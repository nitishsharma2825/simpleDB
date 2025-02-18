package log

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/nitishsharma2825/simpleDB/file"
)

func TestLog(t *testing.T) {
	const testDataFolder = "../test_data"
	const logFile = "testlog"
	const blockSize = 400

	t.Cleanup(func() {
		path := path.Join(testDataFolder, logFile)
		os.Remove(path)
	})

	fileManager := file.NewFileManager(testDataFolder, blockSize)

	logManager := NewLogManager(fileManager, logFile)

	populateLogManager(t, logManager, 1, 35)
	testLogIteration(t, logManager, 35)
	populateLogManager(t, logManager, 36, 70)

	logManager.Flush(65)
	testLogIteration(t, logManager, 70)
}

func makeLogKey(idx int) string {
	return fmt.Sprintf("record_%d", idx)
}

func makeLogVal(idx int) int {
	return idx + 100
}

// verifies that logs are returned in a LIFO manner
func testLogIteration(t *testing.T, lm *Manager, from int) {
	t.Log("The log file has now these records:")
	iter := lm.Iterator()
	f := from
	for {
		if !iter.HasNext() {
			break
		}

		sexp := makeLogKey(f)
		vexp := makeLogVal(f)
		f--

		record := iter.Next()
		page := file.NewPageWithSlice(record)

		s := page.GetString(0)
		if s != sexp {
			t.Fatalf("expected key %q, got %q", sexp, s)
		}

		keyLen := file.MaxLength(len(s))
		v := page.GetInt(keyLen)
		if v != vexp {
			t.Fatalf("expected val %d, got %d", vexp, v)
		}

		t.Logf("[key: %s, val: %d]", s, v)
	}
	t.Log("\n")
}

// populateLogManager appends logs of format K -> V to the logfile
func populateLogManager(t *testing.T, lm *Manager, start int, end int) {
	t.Log("Creating log records:")
	for i := start; i <= end; i++ {
		record := createLogRecord(makeLogKey(i), makeLogVal(i))
		lsn := lm.Append(record)
		t.Logf("%d", lsn)
	}
	t.Log("Records created.\n")
}

func createLogRecord(key string, val int) []byte {
	keyLen := file.MaxLength(len(key))
	recordBuf := make([]byte, keyLen+4)
	page := file.NewPageWithSlice(recordBuf)
	page.SetString(0, key)
	page.SetInt(keyLen, val)
	return recordBuf
}
