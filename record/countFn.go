package record

import "fmt"

/*The count aggregate function*/
type CountFn struct {
	fieldName string
	count     int
}

/*
Create a count aggregation function for the specified field
*/
func NewCountFn(fieldName string) *CountFn {
	return &CountFn{
		fieldName: fieldName,
	}
}

/*
Start a new count.
Null values will also be counted.
The current count is set to 1
*/
func (cf *CountFn) ProcessFirst(scan Scan) {
	cf.count = 1
}

/*
Always increment the count since simpleDB does not support null values
*/
func (cf *CountFn) ProcessNext(scan Scan) {
	cf.count++
}

func (cf *CountFn) FieldName() string {
	return fmt.Sprintf("%q%q", "countof", cf.fieldName)
}

func (cf *CountFn) Value() Constant {
	return NewIntConstant(cf.count)
}
