package record

/*
interface implemented by aggregate function used by groupby operator
*/
type AggregateFn interface {
	/*
		Use the current record of the specified scan
		to be the first record in the group
	*/
	ProcessFirst(Scan)

	/*
		Use the current record of the specified scan
		to be the next record in the group
	*/
	ProcessNext(Scan)

	/*
		Return the name of the new aggregation field
	*/
	FieldName() string

	/*
		Return the computed aggregation value.
	*/
	Value() Constant
}
