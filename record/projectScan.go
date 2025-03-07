package record

import (
	"errors"
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

func (pjs *ProjectScan) GetInt(fieldName string) (int, error) {
	if pjs.HasField(fieldName) {
		return pjs.scan.GetInt(fieldName), nil
	} else {
		return 0, errors.New("field not found")
	}
}

func (pjs *ProjectScan) GetString(fieldName string) (string, error) {
	if pjs.HasField(fieldName) {
		return pjs.scan.GetString(fieldName), nil
	} else {
		return "", errors.New("field not found")
	}
}

func (pjs *ProjectScan) GetVal(fieldName string) (Constant, error) {
	if pjs.HasField(fieldName) {
		return pjs.scan.GetVal(fieldName), nil
	} else {
		return NewNilConstant(), errors.New("field not found")
	}
}

func (pjs *ProjectScan) HasField(fieldName string) bool {
	return slices.Contains(pjs.fieldList, fieldName)
}

func (pjs *ProjectScan) Close() {
	pjs.scan.Close()
}
