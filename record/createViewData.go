package record

/*
Data for create view statement
*/

type CreateViewData struct {
	ViewName  string
	QueryData *QueryData
}

func NewCreateViewData(viewName string, queryData *QueryData) *CreateViewData {
	return &CreateViewData{
		ViewName:  viewName,
		QueryData: queryData,
	}
}

func (cvd *CreateViewData) ViewDef() string {
	return cvd.QueryData.ToString()
}
