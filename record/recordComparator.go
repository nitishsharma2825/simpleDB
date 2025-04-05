package record

// A comparator for scans
type RecordComparator struct {
	fields []string
}

func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{
		fields: fields,
	}
}

/*
Compare the current records of the 2 specified scans.
sort fields are considered in turns
When a field is identified for which the records have different values, it is used as result of the comparison
If the 2 records have the same value for all sort fields, then method returns 0
*/
func (rc *RecordComparator) compare(s1 Scan, s2 Scan) int {
	for _, fieldName := range rc.fields {
		val1 := s1.GetVal(fieldName)
		val2 := s2.GetVal(fieldName)
		result := val1.CompareTo(val2)
		if result != 0 {
			return result
		}
	}
	return 0
}
