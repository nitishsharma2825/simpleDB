package record

import (
	"slices"
)

/*
Scan class corresponding to the project relational algebra operator
*/
type ProjectScan struct {
	scan      Scan
	fieldList []string
}

func NewProjectScan(s Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		scan:      s,
		fieldList: fieldList,
	}
}

func (pjs *ProjectScan) BeforeFirst() {
	pjs.scan.BeforeFirst()
}

func (pjs *ProjectScan) Next() bool {
	return pjs.scan.Next()
}

func (pjs *ProjectScan) GetInt(fieldName string) int {
	if pjs.HasField(fieldName) {
		return pjs.scan.GetInt(fieldName)
	} else {
		return 0
	}
}

func (pjs *ProjectScan) GetString(fieldName string) string {
	if pjs.HasField(fieldName) {
		return pjs.scan.GetString(fieldName)
	} else {
		return ""
	}
}

func (pjs *ProjectScan) GetVal(fieldName string) Constant {
	if pjs.HasField(fieldName) {
		return pjs.scan.GetVal(fieldName)
	} else {
		return NewNilConstant()
	}
}

func (pjs *ProjectScan) HasField(fieldName string) bool {
	return slices.Contains(pjs.fieldList, fieldName)
}

func (pjs *ProjectScan) Close() {
	pjs.scan.Close()
}
