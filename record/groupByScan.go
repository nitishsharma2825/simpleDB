package record

/*
Scan class for the groupby operator
*/
type GroupByScan struct {
	scan        Scan
	groupFields []string
	aggfns      []AggregateFn
	groupval    *GroupValue
	moregroups  bool
}

func NewGroupByScan(scan Scan, groupFields []string, aggfns []AggregateFn) *GroupByScan {
	s := &GroupByScan{
		scan:        scan,
		groupFields: groupFields,
		aggfns:      aggfns,
	}

	s.BeforeFirst()
	return s
}

/*
Position the scan before the first group
Internally, the underlying scan is always
positioned at the first record of a group, which
means that this method moves to the first underlying record
*/
func (gs *GroupByScan) BeforeFirst() {
	gs.scan.BeforeFirst()
	gs.moregroups = gs.scan.Next()
}

/*
Move to the next group
The key of the group is determined by the group values at the current record
The method repeatedly reads underlying records untill it encounters a record having a different key
The aggregation functions are called for each record in the group
The values of the grouping fields for the group are saved
*/
func (gs *GroupByScan) Next() bool {
	if !gs.moregroups {
		return false
	}

	for _, fn := range gs.aggfns {
		fn.ProcessFirst(gs.scan)
	}

	gs.groupval = NewGroupValue(gs.scan, gs.groupFields)
	for {
		gs.moregroups = gs.scan.Next()
		if !gs.moregroups {
			break
		}
		gv := NewGroupValue(gs.scan, gs.groupFields)
		if !gs.groupval.Equals(gv) {
			break
		}
		for _, fn := range gs.aggfns {
			fn.ProcessNext(gs.scan)
		}
	}
	return true
}

func (gs *GroupByScan) Close() {
	gs.scan.Close()
}
