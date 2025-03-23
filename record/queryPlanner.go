package record

import "github.com/nitishsharma2825/simpleDB/tx"

type QueryPlanner interface {
	/*
		Create a plan for the parsed Query
	*/
	CreatePlan(*QueryData, *tx.Transaction) Plan
}
