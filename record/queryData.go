package record

/*
Data for the SQL select command
*/
type QueryData struct {
	fields []string
	tables []string
	pred   Predicate
}

func NewQueryData(fields, tables []string, pred Predicate) *QueryData {
	return &QueryData{
		fields: fields,
		tables: tables,
		pred:   pred,
	}
}

func (qd *QueryData) Fields() []string {
	return qd.fields
}

func (qd *QueryData) Tables() []string {
	return qd.tables
}

func (qd *QueryData) Pred() Predicate {
	return qd.pred
}

func (qd *QueryData) ToString() string {
	result := "select "
	for _, fieldName := range qd.fields {
		result += fieldName + ", "
	}
	result = result[:len(result)-2]
	result += "from "
	for _, tableName := range qd.tables {
		result += tableName + ", "
	}
	result = result[:len(result)-2]
	predString := qd.pred.ToString()
	if predString != "" {
		result += "where " + predString
	}
	return result
}
