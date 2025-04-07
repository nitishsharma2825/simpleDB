package record

/*
An object that holds the values of the grouping fields for the current record of a scan
*/
type GroupValue struct {
	vals map[string]Constant
}

func NewGroupValue(scan Scan, fields []string) *GroupValue {
	gv := &GroupValue{
		vals: make(map[string]Constant),
	}
	for _, fieldName := range fields {
		gv.vals[fieldName] = scan.GetVal(fieldName)
	}
	return gv
}

/*
Return the constant value of the specified field in the group
*/
func (gv *GroupValue) GetVal(fieldName string) Constant {
	return gv.vals[fieldName]
}

/*
Two GroupValue objects are equal if they have the same values for their grouping fields
*/
func (gv *GroupValue) Equals(other *GroupValue) bool {
	for fieldName, value := range gv.vals {
		otherValue := other.GetVal(fieldName)
		if !value.Equals(otherValue) {
			return false
		}
	}
	return true
}

/*
hashcode of the groupvalue object is the sum of hashcodes of its field values
*/
func (gv *GroupValue) HashCode() int {
	hashVal := 0
	for _, value := range gv.vals {
		hashVal += value.HashCode()
	}
	return hashVal
}
