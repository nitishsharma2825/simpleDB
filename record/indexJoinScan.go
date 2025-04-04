package record

/*
Scan class corresponding to Index Join relational operator
*/
type IndexJoinScan struct {
	lhs       Scan
	index     Index
	joinField string
	rhs       *TableScan
}

/*
Creates an index join scan for the specified LHS scan and RHS index
*/
func NewIndexJoinScan(lhs Scan, index Index, joinField string, rhs *TableScan) *IndexJoinScan {
	scan := &IndexJoinScan{
		lhs:       lhs,
		index:     index,
		joinField: joinField,
		rhs:       rhs,
	}
	scan.BeforeFirst()
	return scan
}

/*
Positions the scan before the 1st record
The LHS scan will be positioned at the 1st record
and index will be positioned before the 1st record
for the join value
*/
func (ijs *IndexJoinScan) BeforeFirst() {
	ijs.lhs.BeforeFirst()
	ijs.lhs.Next()
	ijs.resetIndex()
}

/*
Moves the scan to the next record
The method moves to the next index record, if possible
Otherwise, it moves to the next LHS record and first index record
If there are no more LHS records, the method returns false
*/
func (ijs *IndexJoinScan) Next() bool {
	for {
		if ijs.index.Next() {
			// move the table scan for right table to this record ID
			ijs.rhs.MoveToRID(ijs.index.GetDataRID())
			return true
		}
		if !ijs.lhs.Next() {
			return false
		}
		ijs.resetIndex()
	}
}

/*
Returns the value of the field of the current data record
*/
func (ijs *IndexJoinScan) GetInt(fieldName string) int {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetInt(fieldName)
	} else {
		return ijs.lhs.GetInt(fieldName)
	}
}

/*
Returns the value of the field of the current data record
*/
func (ijs *IndexJoinScan) GetString(fieldName string) string {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetString(fieldName)
	} else {
		return ijs.lhs.GetString(fieldName)
	}
}

/*
Returns the value of the field of the current data record
*/
func (ijs *IndexJoinScan) GetVal(fieldName string) Constant {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetVal(fieldName)
	} else {
		return ijs.lhs.GetVal(fieldName)
	}
}

/*
Returns true if field is in the schema
*/
func (ijs *IndexJoinScan) HasField(fieldName string) bool {
	return ijs.rhs.HasField(fieldName) || ijs.lhs.HasField(fieldName)
}

/*
Closes the scan by closing the LHS scan and its RHS index
*/
func (ijs *IndexJoinScan) Close() {
	ijs.lhs.Close()
	ijs.index.Close()
	ijs.rhs.Close()
}

func (ijs *IndexJoinScan) resetIndex() {
	searchKey := ijs.lhs.GetVal(ijs.joinField)
	ijs.index.BeforeFirst(&searchKey)
}
