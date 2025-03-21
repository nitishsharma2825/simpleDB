package record

import "fmt"

/*
A term is a comparison between 2 expression
*/
type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs, rhs Expression) Term {
	return Term{
		lhs: lhs,
		rhs: rhs,
	}
}

/*
Return true if both of the term's expressions
evaluate to the same constant, with respect to the specified scan
*/
func (t Term) IsSatisfied(scan Scan) bool {
	lhsVal := t.lhs.Evaluate(scan)
	rhsVal := t.rhs.Evaluate(scan)
	return lhsVal.Equals(rhsVal)
}

/*
Calculate the extent to which selecting on the term reduces
the number of records output by a query
For example if the reduction factor is 2, then the term cuts the size of output in half
*/
// TODO: Implement the Plan first
func (t Term) ReductionFactor(plan Plan) int {
	return 0
}

/*
Determin if this term is of the form "F=c"
where F is specified Field and c is some constant
If so, method returns that constant
if not, the method returns null
*/
func (t Term) EquatesWithConstant(fieldName string) *Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		result := t.rhs.AsConstant()
		return &result
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		result := t.lhs.AsConstant()
		return &result
	}
	return nil
}

/*
Determine if this term is of the from "F1=F2"
where F1 = specified field and F2 = another field
If so, method returns the name of that field
If not, method returns null
*/
func (t Term) EquatesWithField(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}
	return ""
}

/*
Return true if both the term's expressions apply to the specified schema
*/
func (t Term) AppliesTo(schema *Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t Term) ToString() string {
	return fmt.Sprintf("%q=%q", t.lhs.ToString(), t.rhs.ToString())
}

func (t Term) Lhs() Expression {
	return t.lhs
}

func (t Term) Rhs() Expression {
	return t.rhs
}
