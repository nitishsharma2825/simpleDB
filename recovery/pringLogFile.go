package recovery

import (
	"fmt"

	"github.com/nitishsharma2825/simpleDB/file"
	"github.com/nitishsharma2825/simpleDB/log"
)

func PrintLogFile() {
	const dbFolder = "../test_data"
	const logFile = "logfile"
	const blockSize = 400

	fm := file.NewFileManager(dbFolder, blockSize)
	lm := log.NewLogManager(fm, logFile)

	lastBlock := fm.Length(logFile) - 1
	blockId := file.NewBlockID(logFile, lastBlock)
	page := file.NewPageWithSize(fm.BlockSize())
	fm.Read(blockId, page)

	iter := lm.Iterator()
	for iter.HasNext() {
		buf := iter.Next()
		record := CreateLogRecord(buf)
		fmt.Println(record.ToString())
	}
}
