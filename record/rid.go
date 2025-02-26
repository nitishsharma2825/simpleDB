package record

import "fmt"

/*
Every record can be uniquely identified by the block number in file
and the slot number in block
*/
type RID struct {
	blockNum int
	slot     int
}

func NewRID(blockNum, slot int) RID {
	return RID{blockNum: blockNum, slot: slot}
}

func (rid RID) BlockNum() int {
	return rid.blockNum
}

func (rid RID) Slot() int {
	return rid.slot
}

func (rid RID) Equals(other RID) bool {
	return rid.blockNum == other.blockNum && rid.slot == other.slot
}

func (rid RID) ToString() string {
	return fmt.Sprintf("[%d, %d]", rid.blockNum, rid.slot)
}
