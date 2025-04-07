package record

import "github.com/nitishsharma2825/simpleDB/tx"

/*
Plan class for the sort operator
*/
type SortPlan struct {
	tx     *tx.Transaction
	plan   Plan
	schema *Schema
	comp   *RecordComparator
}

// Create a sort plan for the specified query
func NewSortPlan(tx *tx.Transaction, plan Plan, sortFields []string) *SortPlan {
	return &SortPlan{
		tx:     tx,
		plan:   plan,
		schema: plan.Schema(),
		comp:   NewRecordComparator(sortFields),
	}
}

/*
Most of the action is in this method
Up to 2 Sorted temporary tables are created,
and are passed into SortScan for final merging
*/
func (sp *SortPlan) Open() Scan {
	source := sp.plan.Open()
	runs := sp.splitIntoRuns(source)
	source.Close()
	for len(runs) > 2 {
		runs = sp.doMergeIterations(runs)
	}
	return NewSortScan(runs, sp.comp)
}

/*
Return the number of blocks in sorted table which is same as it would be in materialized table
Does not include the one time cost of materializing and sorting the records
*/
func (sp *SortPlan) BlocksAccessed() int {
	// does not include the one time cost of sorting
	mp := NewMaterializePlan(sp.tx, sp.plan) // not opened, just for analysis
	return mp.BlocksAccessed()
}

func (sp *SortPlan) RecordsOutput() int {
	return sp.plan.RecordsOutput()
}

func (sp *SortPlan) DistinctValues(fieldName string) int {
	return sp.plan.DistinctValues(fieldName)
}

func (sp *SortPlan) Schema() *Schema {
	return sp.schema
}

/*
A temporary table is created for each sorted run
Returns a list of runs stored in multiple temp tables
*/
func (sp *SortPlan) splitIntoRuns(sourceScan Scan) []*TempTable {
	temps := make([]*TempTable, 0)
	sourceScan.BeforeFirst()
	if !sourceScan.Next() {
		return temps
	}

	currentTemp := NewTempTable(sp.tx, sp.schema)
	temps = append(temps, currentTemp)
	currentScan := currentTemp.Open()
	for sp.copy(sourceScan, currentScan) {
		if sp.comp.compare(sourceScan, currentScan) < 0 {
			// start a new run
			currentScan.Close()
			currentTemp = NewTempTable(sp.tx, sp.schema)
			temps = append(temps, currentTemp)
			currentScan = currentTemp.Open()
		}
	}
	currentScan.Close()
	return temps
}

func (sp *SortPlan) doMergeIterations(runs []*TempTable) []*TempTable {
	result := make([]*TempTable, 0)
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		result = append(result, sp.mergeTwoRuns(p1, p2))
		runs = runs[2:]
	}
	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result
}

func (sp *SortPlan) mergeTwoRuns(p1, p2 *TempTable) *TempTable {
	source1 := p1.Open()
	source2 := p2.Open()
	result := NewTempTable(sp.tx, sp.schema)
	dest := result.Open()

	hasmore1 := source1.Next()
	hasmore2 := source2.Next()
	for hasmore1 && hasmore2 {
		if sp.comp.compare(source1, source2) < 0 {
			hasmore1 = sp.copy(source1, dest)
		} else {
			hasmore2 = sp.copy(source2, dest)
		}
	}

	if hasmore1 {
		for hasmore1 {
			hasmore1 = sp.copy(source1, dest)
		}
	} else {
		for hasmore2 {
			hasmore2 = sp.copy(source2, dest)
		}
	}
	source1.Close()
	source2.Close()
	dest.Close()
	return result
}

func (sp *SortPlan) copy(source Scan, dest UpdateScan) bool {
	dest.Insert()
	for _, fieldName := range sp.schema.Fields() {
		dest.SetVal(fieldName, source.GetVal(fieldName))
	}
	return source.Next()
}
