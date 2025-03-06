package record

type Expression struct {
	value     *Constant
	fieldName *string
}

func NewExpressionWithConstant(value Constant) Expression {
	return Expression{
		value:     &value,
		fieldName: nil,
	}
}

func NewExpressionWithField(fieldName string) Expression {
	return Expression{
		fieldName: &fieldName,
		value:     nil,
	}
}

/*
Evaluate the expression with respect to the current record of the specified scan
*/
func (exp Expression) Evaluate(scan Scan) Constant {
	if exp.value != nil {
		return *exp.value
	}
	return scan.GetVal(*exp.fieldName)
}

/*
Return true if expression is a field reference
*/
func (exp Expression) IsFieldName() bool {
	return exp.fieldName != nil
}

/*
Return the constant corresponding to a constant expression
or nil if expression does not denote a constant
*/
func (exp Expression) AsConstant() Constant {
	return *exp.value
}

/*
Return the field name corresponding to a constant expression
or nil if expression does not denote a field
*/
func (exp Expression) AsFieldName() string {
	if exp.fieldName != nil {
		return *exp.fieldName
	}
	return ""
}

/*
Determine if all the fields mentioned in the expression
are contained in the specified schema
*/
func (exp Expression) AppliesTo(schema Schema) bool {
	if exp.value != nil {
		return true
	}
	return schema.HasField(*exp.fieldName)
}

func (exp Expression) ToString() string {
	if exp.value != nil {
		return exp.value.ToString()
	}
	if exp.fieldName != nil {
		return *exp.fieldName
	}
	return ""
}
