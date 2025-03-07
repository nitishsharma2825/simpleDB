package record

import "fmt"

/*
A predicate is a boolean combination of terms
*/
type Predicate struct {
	terms []Term
}

/*
Create an empty predicate, corresponfing to true
*/
func NewPredicate() *Predicate {
	return &Predicate{terms: make([]Term, 0)}
}

/*
Create a predicate containing a single term
*/
func NewPredicateWithTerm(t Term) *Predicate {
	terms := make([]Term, 0)
	terms = append(terms, t)
	return &Predicate{terms: terms}
}

/*
Modifies the predicate to be conjunction of
itself and specified predicate
*/
func (p *Predicate) ConjoinWith(predicate *Predicate) {
	p.terms = append(p.terms, predicate.terms...)
}

/*
Returns true if the predicate evaluates to true
w.r.t to the specified scan
*/
func (p *Predicate) IsSatisfied(scan Scan) bool {
	for _, term := range p.terms {
		if !term.IsSatisfied(scan) {
			return false
		}
	}
	return true
}

/*
Calculate the extent to which selecting on the predicate
reduces the number of records output by a query
Ex: if reduction factor is 2, then the
predicate cuts the size of the output in half
*/
func (p *Predicate) ReductionFactor(plan Plan) int {
	factor := 1
	for _, term := range p.terms {
		factor = factor * term.ReductionFactor(plan)
	}
	return factor
}

/*
Return the subpredicate that applies to the specified schema
*/
func (p *Predicate) SelectSubPred(schema *Schema) *Predicate {
	result := NewPredicate()
	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

/*
Return the subpredicate consisting of terms that apply
to the union of the 2 specified schemas
but not to either schema separately
*/
func (p *Predicate) JoinSubPred(schema1 *Schema, schema2 *Schema) *Predicate {
	result := NewPredicate()
	newSch := NewSchema()
	newSch.Addall(schema1)
	newSch.Addall(schema2)
	for _, term := range p.terms {
		if !term.AppliesTo(schema1) && !term.AppliesTo(schema2) && term.AppliesTo(newSch) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

/*
Determine if there is a term of the form "F=c"
where F is field and c is constant
*/
func (p *Predicate) EquatesWithConstant(fieldName string) *Constant {
	for _, term := range p.terms {
		result := term.EquatesWithConstant(fieldName)
		if result != nil {
			return result
		}
	}
	return nil
}

/*
Determine if there is a term of the form "F1=F2"
where f1 and f2 are fields
*/
func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, term := range p.terms {
		result := term.EquatesWithField(fieldName)
		if result != "" {
			return result
		}
	}
	return ""
}

func (p *Predicate) ToString() string {
	result := ""
	if len(p.terms) == 0 || p.terms == nil {
		return result
	}
	for _, term := range p.terms {
		if result == "" {
			result += term.ToString()
		} else {
			result += fmt.Sprintf(" and %q", term.ToString())
		}
	}
	return result
}
