package sql

import "github.com/nitishsharma2825/simpleDB/record"

// Entire grammar for the SQL subset supported by SimpleDB
// <Field> := TokenIdentifier
// <Constant> := TokenString | TokenNumber
// <Expression> := <Field> | <Constant>
// <Term> := <Expression> = <Expression>
// <Predicate> := <Term> [AND <Predicate>]
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ] [ORDER BY <Field> [, <FieldList>]]
// <SelectList> := <Field> [, <SelectList> ]
// <TableList> := TokenIdentifier [, <TableList> ]
// <UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
// <Create> := <CreateTable> | <CreateView> | <CreateIndex>
// <Insert> := INSERT INTO TokenIdentifier ( <FieldList> ) VALUES ( <ConstList> )
// <FieldList> := <Field> [, <FieldList> ]
// <ConstList> := <Constant> [, <ConstList> ]
// <Delete> := DELETE FROM TokenIdentifier [ WHERE <Predicate> ]
// <Modify> := UPDATE TokenIdentifier SET <Field> = <Expression> [ WHERE <Predicate> ]
// <CreateTable> := CREATE TABLE TokenIdentifier ( <FieldDefs> )
// <FieldDefs> := <FieldDef> [, <FieldDefs> ]
// <FieldDef> := TokenIdentifier <TypeDef>
// <TypeDef> := INT | TEXT | VARCHAR ( TokenNumber )
// <CreateView> := CREATE VIEW TokenIdentifier AS <Query>
// <CreateIndex> := CREATE INDEX TokenIdentifier ON TokenIdentifier ( <Field> )

type Parser struct {
	lexer *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{
		lexer: NewLexer(NewTokenizer(s)),
	}
}

// methods for parsing predicates, terms, expressions and fields

func (p *Parser) Field() (string, error) {
	return p.lexer.EatIdentifier()
}

func (p *Parser) Constant() (*record.Constant, error) {
	if p.lexer.MatchStringValue() {
		val, err := p.lexer.EatStringValue()
		if err != nil {
			return nil, err
		}
		constant := record.NewStringConstant(val)
		return &constant, nil
	} else {
		val, err := p.lexer.EatIntValue()
		if err != nil {
			return nil, err
		}
		constant := record.NewIntConstant(val)
		return &constant, nil
	}
}

func (p *Parser) Expression() (*record.Expression, error) {
	if p.lexer.MatchIdentifier() {
		val, err := p.Field()
		if err != nil {
			return nil, err
		}
		exp := record.NewExpressionWithField(val)
		return &exp, nil
	} else {
		val, err := p.Constant()
		if err != nil {
			return nil, err
		}
		exp := record.NewExpressionWithConstant(*val)
		return &exp, nil
	}
}

func (p *Parser) Term() (*record.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenEqual)
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	term := record.NewTerm(*lhs, *rhs)
	return &term, nil
}

func (p *Parser) Predicate() (*record.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	pred := record.NewPredicateWithTerm(*term)
	if p.lexer.MatchKeyword("and") {
		p.lexer.EatKeyword("and")
		nextPred, err := p.Predicate()
		if err != nil {
			return nil, err
		}
		pred.ConjoinWith(nextPred)
	}
	return pred, nil
}

// methods for parsing queries
func (p *Parser) Query() (*QueryData, error) {
	p.lexer.EatKeyword("select")
	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}
	p.lexer.EatKeyword("from")
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}
	var pred *record.Predicate
	if p.lexer.MatchKeyword("where") {
		p.lexer.EatKeyword("where")
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}

	return NewQueryData(fields, tables, *pred), nil
}

func (p *Parser) selectList() ([]string, error) {
	result := make([]string, 0)
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	result = append(result, field)

	if p.lexer.MatchTokenType(TokenComma) {
		p.lexer.EatTokenType(TokenComma)
		nextFields, err := p.selectList()
		if err != nil {
			return nil, err
		}
		result = append(result, nextFields...)
	}

	return result, nil
}

func (p *Parser) tableList() ([]string, error) {
	result := make([]string, 0)
	table, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	result = append(result, table)

	if p.lexer.MatchTokenType(TokenComma) {
		p.lexer.EatTokenType(TokenComma)
		nextTables, err := p.tableList()
		if err != nil {
			return nil, err
		}
		result = append(result, nextTables...)
	}

	return result, nil
}

// methods for parsing update commands
func (p *Parser) UpdateCmd() (interface{}, error) {
	if p.lexer.MatchKeyword("insert") {
		return p.Insert()
	} else if p.lexer.MatchKeyword("delete") {
		return p.Delete()
	} else if p.lexer.MatchKeyword("update") {
		return p.Modify()
	} else {
		return p.Create()
	}
}

func (p *Parser) Create() (interface{}, error) {
	p.lexer.EatKeyword("create")
	if p.lexer.MatchKeyword("table") {
		return p.CreateTable()
	} else if p.lexer.MatchKeyword("view") {
		return p.CreateView()
	} else {
		return p.CreateIndex()
	}
}

// methods for parsing delete commands
func (p *Parser) Delete() (*DeleteData, error) {
	p.lexer.EatKeyword("delete")
	p.lexer.EatKeyword("from")
	tblName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	pred := record.NewPredicate()
	if p.lexer.MatchKeyword("where") {
		p.lexer.EatKeyword("where")
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(tblName, pred), nil
}

// methods for parsing insert commands
func (p *Parser) Insert() (*InsertData, error) {
	p.lexer.EatKeyword("insert")
	p.lexer.EatKeyword("into")
	tblName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenLeftParen)
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenRightParen)
	p.lexer.EatKeyword("values")
	p.lexer.EatTokenType(TokenLeftParen)
	vals, err := p.constList()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenRightParen)
	return NewInsertData(tblName, fields, vals), nil
}

func (p *Parser) fieldList() ([]string, error) {
	result := make([]string, 0)
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	result = append(result, field)

	if p.lexer.MatchTokenType(TokenComma) {
		p.lexer.EatTokenType(TokenComma)
		nextFields, err := p.fieldList()
		if err != nil {
			return nil, err
		}
		result = append(result, nextFields...)
	}

	return result, nil
}

func (p *Parser) constList() ([]*record.Constant, error) {
	result := make([]*record.Constant, 0)
	constant, err := p.Constant()
	if err != nil {
		return nil, err
	}
	result = append(result, constant)

	if p.lexer.MatchTokenType(TokenComma) {
		p.lexer.EatTokenType(TokenComma)
		nextConstants, err := p.constList()
		if err != nil {
			return nil, err
		}
		result = append(result, nextConstants...)
	}

	return result, nil
}

// methods for parsing modify commands
func (p *Parser) Modify() (*ModifyData, error) {
	p.lexer.EatKeyword("update")
	tblName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	p.lexer.EatKeyword("set")

	fldName, err := p.Field()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenEqual)
	newVal, err := p.Expression()
	if err != nil {
		return nil, err
	}

	pred := record.NewPredicate()
	if p.lexer.MatchKeyword("where") {
		p.lexer.EatKeyword("where")
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}

	return NewModifyData(tblName, fldName, newVal, pred), nil
}

// methods for parsing create table commands
func (p *Parser) CreateTable() (*CreateTableData, error) {
	p.lexer.EatKeyword("table")
	tblName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenLeftParen)
	schema, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenRightParen)
	return NewCreateTableData(tblName, schema), nil
}

func (p *Parser) fieldDefs() (*record.Schema, error) {
	schema, err := p.fieldDef()
	if err != nil {
		return nil, err
	}
	if p.lexer.MatchTokenType(TokenComma) {
		p.lexer.EatTokenType(TokenComma)
		schema2, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}
		schema.Addall(schema2)
	}
	return schema, nil
}

func (p *Parser) fieldDef() (*record.Schema, error) {
	fldName, err := p.Field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(fldName)
}

func (p *Parser) fieldType(fldName string) (*record.Schema, error) {
	schema := record.NewSchema()
	if p.lexer.MatchKeyword("int") {
		p.lexer.EatKeyword("int")
		schema.AddIntField(fldName)
	} else {
		p.lexer.EatKeyword("varchar")
		p.lexer.EatTokenType(TokenLeftParen)
		strLen, err := p.lexer.EatIntValue()
		if err != nil {
			return nil, err
		}
		p.lexer.EatTokenType(TokenRightParen)
		schema.AddStringField(fldName, strLen)
	}
	return schema, nil
}

// methods for parsing create view commands
func (p *Parser) CreateView() (*CreateViewData, error) {
	p.lexer.EatKeyword("view")
	viewName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	p.lexer.EatKeyword("as")
	qd, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewName, qd), nil
}

// methods for parsing create index command
func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	p.lexer.EatKeyword("index")
	idxName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	p.lexer.EatKeyword("on")
	tblName, err := p.lexer.EatIdentifier()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenLeftParen)
	fldName, err := p.Field()
	if err != nil {
		return nil, err
	}
	p.lexer.EatTokenType(TokenRightParen)
	return NewCreateIndexData(idxName, tblName, fldName), nil
}
