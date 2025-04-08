package record

import "fmt"

/*The max aggregate function*/
type MaxFn struct {
	fieldName string
	val       Constant
}

/*
Create a max aggregation function for the specified field
*/
func NewMaxFn(fieldName string) *MaxFn {
	return &MaxFn{
		fieldName: fieldName,
	}
}

/*
Start a new maximum value for the current field
*/
func (mf *MaxFn) ProcessFirst(scan Scan) {
	mf.val = scan.GetVal(mf.fieldName)
}

/*
Always increment the count since simpleDB does not support null values
*/
func (mf *MaxFn) ProcessNext(scan Scan) {
	newVal := scan.GetVal(mf.fieldName)
	if newVal.CompareTo(mf.val) > 0 {
		mf.val = newVal
	}
}

func (mf *MaxFn) FieldName() string {
	return fmt.Sprintf("%q%q", "maxof", mf.fieldName)
}

func (mf *MaxFn) Value() Constant {
	return mf.val
}
