package record

/*
Scan class corresponding to the select relational operator
*/

type IndexSelectScan struct {
	ts  *TableScan
	idx Index
	val *Constant
}

func NewIndexSelectScan(ts *TableScan, idx Index, val *Constant) *IndexSelectScan {
	scan := &IndexSelectScan{
		ts:  ts,
		idx: idx,
		val: val,
	}
	scan.BeforeFirst()
	return scan
}

/*
Positions the scan before the 1st record,
which in this case means positioning the index
before the 1st instance of the selection constant
*/
func (iss *IndexSelectScan) BeforeFirst() {
	iss.idx.BeforeFirst(iss.val)
}

/*
Moves to the next record, which in this case means
moving the index to the next record satisfying the selection constant
and returning false if there are no more such index records
If there is a next record, the method moves the tablescan
to the corresponding data record
*/
func (iss *IndexSelectScan) Next() bool {
	ok := iss.idx.Next()
	if ok {
		rid := iss.idx.GetDataRID()
		iss.ts.MoveToRid(rid)
	}
	return ok
}

/*
Returns the value of the field of the current data record
*/
func (iss *IndexSelectScan) GetInt(fieldName string) int {
	return iss.ts.GetInt(fieldName)
}

/*
Returns the value of the field of the current data record
*/
func (iss *IndexSelectScan) GetString(fieldName string) string {
	return iss.ts.GetString(fieldName)
}

/*
Returns the value of the field of the current data record
*/
func (iss *IndexSelectScan) GetVal(fieldName string) Constant {
	return iss.ts.GetVal(fieldName)
}

/*
Returns true if field is in the schema
*/
func (iss *IndexSelectScan) HasField(fieldName string) bool {
	return iss.ts.HasField(fieldName)
}

/*
Closes the scan by closing the index and the tablescan
*/
func (iss *IndexSelectScan) Close() {
	iss.idx.Close()
	iss.ts.Close()
}
