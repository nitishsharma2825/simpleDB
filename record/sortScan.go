package record

/*
Scan class for the Sort operator
*/
type SortScan struct {
	s1, s2, currentScan UpdateScan
	comp                *RecordComparator
	hasmore1, hasmore2  bool
	savedPosition       []*RID
}

/*
Create a sort scan, given a list of 1 or 2 runs
If there is only 1 run, s2 will be nil and hasmore2 will be false
*/
func NewSortScan(runs []*TempTable, comp *RecordComparator) *SortScan {
	scan := &SortScan{
		comp:          comp,
		s1:            nil,
		s2:            nil,
		currentScan:   nil,
		hasmore1:      false,
		hasmore2:      false,
		savedPosition: make([]*RID, 0),
	}

	scan.s1 = runs[0].Open()
	scan.hasmore1 = scan.s1.Next()
	if len(runs) > 1 {
		scan.s2 = runs[1].Open()
		scan.hasmore2 = scan.s2.Next()
	}
	return scan
}

// Positions the scan before the 1st record in sorted order
func (ss *SortScan) BeforeFirst() {
	ss.currentScan = nil
	ss.s1.BeforeFirst()
	ss.hasmore1 = ss.s1.Next()
	if ss.s2 != nil {
		ss.s2.BeforeFirst()
		ss.hasmore2 = ss.s2.Next()
	}
}

/*
Moves to the next record in sorted order
1st, current scan is moved to the next record
Then the lowest record of the 2 scans is found, and that scan is chosen to be the new current scan
*/
func (ss *SortScan) Next() bool {
	if ss.currentScan != nil {
		if ss.currentScan == ss.s1 {
			ss.hasmore1 = ss.s1.Next()
		} else if ss.currentScan == ss.s2 {
			ss.hasmore2 = ss.s2.Next()
		}
	}

	if !ss.hasmore1 && !ss.hasmore2 {
		return false
	} else if ss.hasmore1 && ss.hasmore2 {
		if ss.comp.compare(ss.s1, ss.s2) < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasmore1 {
		ss.currentScan = ss.s1
	} else if ss.hasmore2 {
		ss.currentScan = ss.s2
	}
	return true
}

func (ss *SortScan) Close() {
	ss.s1.Close()
	if ss.s2 != nil {
		ss.s2.Close()
	}
}

func (ss *SortScan) GetVal(fieldName string) Constant {
	return ss.currentScan.GetVal(fieldName)
}

func (ss *SortScan) GetInt(fieldname string) int {
	return ss.currentScan.GetInt(fieldname)
}

func (ss *SortScan) GetString(fieldName string) string {
	return ss.currentScan.GetString(fieldName)
}

func (ss *SortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

// save the position of the current record so that it can be restored at a later time
func (ss *SortScan) SavePosition() {
	rid1 := ss.s1.GetRID()
	var rid2 *RID
	if ss.s2 != nil {
		curRid := ss.s2.GetRID()
		rid2 = &curRid
	} else {
		rid2 = nil
	}
	ss.savedPosition = append(ss.savedPosition, &rid1, rid2)
}

/*
Move the scan to its previously saved position
*/
func (ss *SortScan) RestorePosition() {
	rid1 := ss.savedPosition[0]
	rid2 := ss.savedPosition[1]
	ss.s1.MoveToRID(*rid1)
	if rid2 != nil {
		ss.s2.MoveToRID(*rid2)
	}
}
