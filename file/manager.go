package file

import (
	"os"
	"path"
	"strings"
	"sync"
)

// Only one manager in simpleDB
// methods reads, writes, append need to be synchronized
// Part of database engine which talks to OS to read/write pages to disks
// Always reads/writes block sized number of bytes from a file, always at a block boundary
// ensures that each call to read/write/append will incur exactly one disk access

type Manager struct {
	mu sync.Mutex

	directory string
	blockSize int
	isNew     bool

	openFiles map[string]*os.File // filename -> open file, files opend in RWS mode [Direct I/O]
}

func NewFileManager(dirPath string, blockSize int) *Manager {
	_, err := os.Stat(dirPath)
	isNew := os.IsNotExist(err)

	if isNew {
		// give all permissions and not allow non-owner to delete
		os.MkdirAll(dirPath, os.ModeSticky|os.ModePerm)
	}

	if !isNew && err != nil {
		panic(err)
	}

	// clear all tmp files in the folder
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	for _, v := range entries {
		if strings.HasPrefix(v.Name(), "tmp") {
			os.Remove(v.Name())
		}
	}

	return &Manager{
		directory: dirPath,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: make(map[string]*os.File),
	}
}

func (manager *Manager) Read(blockID BlockID, page *Page) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	file := manager.getFile(blockID.FileName())
	if _, err := file.ReadAt(page.Contents(), int64(blockID.BlockNumber())*int64(manager.blockSize)); err != nil {
		panic(err)
	}
}

func (manager *Manager) Write(blockID BlockID, page *Page) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	file := manager.getFile(blockID.FileName())
	file.WriteAt(page.Contents(), int64(blockID.BlockNumber())*int64(manager.blockSize))
}

// Seeks to the end of the file and writes an empty array of bytes to the file
// is this needed in go?
func (manager *Manager) Append(filename string) BlockID {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	newBlockNum := manager.length(filename)
	blockID := NewBlockID(filename, newBlockNum)
	buf := make([]byte, manager.blockSize)

	file := manager.getFile(filename)
	file.WriteAt(buf, int64(blockID.BlockNumber())*int64(manager.blockSize))
	return blockID
}

// Getters and Setters

func (manager *Manager) BlockSize() int {
	return manager.blockSize
}

func (manager *Manager) IsNew() bool {
	return manager.isNew
}

// Helper functions

func (manager *Manager) getFile(filename string) *os.File {
	file, ok := manager.openFiles[filename]
	if !ok {
		filePath := path.Join(manager.directory, filename)
		newFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0755)
		if err != nil {
			panic(err)
		}
		manager.openFiles[filename] = newFile
		return newFile
	}

	return file
}

// returns the total blocks in file
func (manager *Manager) length(filename string) int {
	file := manager.getFile(filename)

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}

	return int(fileInfo.Size() / int64(manager.blockSize))
}
