package record

/*
The scan class corresponding to the select relational algebra operator
*/
type SelectScan struct {
	scan Scan
	pred *Predicate
}

func NewSelectScan(s Scan, pred *Predicate) *SelectScan {
	return &SelectScan{
		scan: s,
		pred: pred,
	}
}

// Scan methods

func (ss *SelectScan) BeforeFirst() {
	ss.scan.BeforeFirst()
}

func (ss *SelectScan) Next() bool {
	for ss.scan.Next() {
		if ss.pred.IsSatisfied(ss.scan) {
			return true
		}
	}
	return false
}

func (ss *SelectScan) GetInt(fieldName string) int {
	return ss.scan.GetInt(fieldName)
}

func (ss *SelectScan) GetString(fieldName string) string {
	return ss.scan.GetString(fieldName)
}

func (ss *SelectScan) GetVal(fieldName string) Constant {
	return ss.scan.GetVal(fieldName)
}

func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.scan.HasField(fieldName)
}

func (ss *SelectScan) Close() {
	ss.scan.Close()
}

// UpdateScan methods

func (ss *SelectScan) SetInt(fieldName string, value int) {
	updateScan := ss.scan.(UpdateScan)
	updateScan.SetInt(fieldName, value)
}

func (ss *SelectScan) SetString(fieldName string, value string) {
	updateScan := ss.scan.(UpdateScan)
	updateScan.SetString(fieldName, value)
}

func (ss *SelectScan) SetVal(fieldName string, value Constant) {
	updateScan := ss.scan.(UpdateScan)
	updateScan.SetVal(fieldName, value)
}

func (ss *SelectScan) Delete() {
	updateScan := ss.scan.(UpdateScan)
	updateScan.Delete()
}

func (ss *SelectScan) Insert() {
	updateScan := ss.scan.(UpdateScan)
	updateScan.Insert()
}

func (ss *SelectScan) GetRID() RID {
	updateScan := ss.scan.(UpdateScan)
	return updateScan.GetRID()
}

func (ss *SelectScan) MoveToRid(rid RID) {
	updateScan := ss.scan.(UpdateScan)
	updateScan.MoveToRID(rid)
}
