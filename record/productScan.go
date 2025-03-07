package record

/*
Scan class corresponding ti the product operator
*/
type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(s1, s2 Scan) *ProductScan {
	ps := &ProductScan{
		scan1: s1,
		scan2: s2,
	}
	ps.BeforeFirst()
	return ps
}

/*
Position the scan before its first record
LHS scan is positioned at 1st record
RHS scan is positioned before its first record
*/
func (prs *ProductScan) BeforeFirst() {
	prs.scan1.BeforeFirst()
	prs.scan1.Next()
	prs.scan2.BeforeFirst()
}

/*
Move the scan to the next record
Move to the next RHS record, if possible
Else, move to the next LHS record and first RHS record
If there are no LHS records method returns false
*/
func (prs *ProductScan) Next() bool {
	if prs.scan2.Next() {
		return true
	}
	prs.scan2.BeforeFirst()
	return prs.scan2.Next() && prs.scan1.Next()
}

/*
Value is returned from whichever scan contains the field
*/
func (prs *ProductScan) GetInt(fieldName string) int {
	if prs.scan1.HasField(fieldName) {
		return prs.scan1.GetInt(fieldName)
	}
	return prs.scan2.GetInt(fieldName)
}

/*
Value is returned from whichever scan contains the field
*/
func (prs *ProductScan) GetString(fieldName string) string {
	if prs.scan1.HasField(fieldName) {
		return prs.scan1.GetString(fieldName)
	}
	return prs.scan2.GetString(fieldName)
}

/*
Value is returned from whichever scan contains the field
*/
func (prs *ProductScan) GetVal(fieldName string) Constant {
	if prs.scan1.HasField(fieldName) {
		return prs.scan1.GetVal(fieldName)
	}
	return prs.scan2.GetVal(fieldName)
}

/*
Returns true if the specified field is in either of the scans
*/
func (prs *ProductScan) HasField(fieldName string) bool {
	return prs.scan1.HasField(fieldName) || prs.scan2.HasField(fieldName)
}

func (prs *ProductScan) Close() {
	prs.scan1.Close()
	prs.scan2.Close()
}
