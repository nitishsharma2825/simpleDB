package record

/*
Scan class for the merge join operator
*/
type MergeJoinScan struct {
	scan1              Scan
	scan2              SortScan
	fldname1, fldname2 string
	joinval            Constant
}

func NewMergeJoinScan(s1 Scan, s2 SortScan, fldname1, fldname2 string) *MergeJoinScan {
	scan := &MergeJoinScan{
		scan1:    s1,
		scan2:    s2,
		fldname1: fldname1,
		fldname2: fldname2,
		joinval:  NewNilConstant(),
	}
	scan.BeforeFirst()
	return scan
}

func (mjs *MergeJoinScan) Close() {
	mjs.scan1.Close()
	mjs.scan2.Close()
}

func (mjs *MergeJoinScan) BeforeFirst() {
	mjs.scan1.BeforeFirst()
	mjs.scan2.BeforeFirst()
}

/*
Move to the next record
If the next RHS record has the same join value, then move to it
Otherwise, if the next LHS record has the same join value, then reposition the scan back to 1st record having that join value
Otherwise, repeatedly move the scan having the smallest value until a common join value is found
When one of records run out of records, return false
*/
func (mjs *MergeJoinScan) Next() bool {
	hasmore2 := mjs.scan2.Next()
	if hasmore2 && mjs.scan2.GetVal(mjs.fldname2).Equals(mjs.joinval) {
		return true
	}

	hasmore1 := mjs.scan1.Next()
	if hasmore1 && mjs.scan1.GetVal(mjs.fldname1).Equals(mjs.joinval) {
		mjs.scan2.RestorePosition()
		return true
	}

	for hasmore1 && hasmore2 {
		v1 := mjs.scan1.GetVal(mjs.fldname1)
		v2 := mjs.scan2.GetVal(mjs.fldname2)
		if v1.CompareTo(v2) < 0 {
			hasmore1 = mjs.scan1.Next()
		} else if v1.CompareTo(v2) > 0 {
			hasmore2 = mjs.scan2.Next()
		} else {
			mjs.scan2.SavePosition()
			mjs.joinval = mjs.scan2.GetVal(mjs.fldname2)
			return true
		}
	}
	return false
}

func (mjs *MergeJoinScan) GetVal(fieldName string) Constant {
	if mjs.scan1.HasField(fieldName) {
		return mjs.scan1.GetVal(fieldName)
	} else {
		return mjs.scan2.GetVal(fieldName)
	}
}

func (mjs *MergeJoinScan) GetInt(fieldName string) int {
	if mjs.scan1.HasField(fieldName) {
		return mjs.scan1.GetInt(fieldName)
	} else {
		return mjs.scan2.GetInt(fieldName)
	}
}

func (mjs *MergeJoinScan) GetString(fieldName string) string {
	if mjs.scan1.HasField(fieldName) {
		return mjs.scan1.GetString(fieldName)
	} else {
		return mjs.scan2.GetString(fieldName)
	}
}

func (mjs *MergeJoinScan) HasField(fldName string) bool {
	return mjs.scan1.HasField(fldName) || mjs.scan2.HasField(fldName)
}
