package record

/*
A directory entry has 2 components:
- number of the child block
- dataval of 1st record in that child block
*/

type DirEntry struct {
	Dataval  *Constant
	Blocknum int
}

func NewDirEntry(dataval *Constant, blocknum int) *DirEntry {
	return &DirEntry{
		Dataval:  dataval,
		Blocknum: blocknum,
	}
}
