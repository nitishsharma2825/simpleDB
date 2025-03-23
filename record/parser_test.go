package record

import (
	"slices"
	"testing"
)

func TestParseField(t *testing.T) {
	p := NewParser("field")

	v, err := p.Field()
	if err != nil {
		t.Fatal(err)
	}

	if v != "field" {
		t.Fatalf("expected %q, got %s\n", "field", v)
	}
}

func TestFieldList(t *testing.T) {
	const src = "first, second, third"
	p := NewParser(src)

	v, err := p.selectList()
	if err != nil {
		t.Fatal(err)
	}

	if len(v) != 3 {
		t.Fatalf("expected 3 fields, got %d\n", len(v))
	}

	exp := []string{"first", "second", "third"}
	for i := range v {
		if v[i] != exp[i] {
			t.Fatalf("expected %q, got %q at position %d\n", exp[i], v[i], i)
		}
	}
}

func TestConstant(t *testing.T) {
	type test struct {
		src string
		exp string
	}

	for _, v := range []test{
		{
			src: "''",
			exp: "",
		},
		{
			src: "'test'",
			exp: "test",
		},
	} {
		p := NewParser(v.src)
		c, err := p.Constant()
		if err != nil {
			t.Fatal(err)
		}

		if c.AsString() != v.exp {
			t.Fatalf("expected %q, got %q\n", v.exp, c.AsString())
		}
	}
}

func TestQuery(t *testing.T) {
	const src = "SELECT first, second FROM atable WHERE first = 1 AND second = 'second'"
	p := NewParser(src)

	qd, err := p.Query()
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(qd.Tables(), []string{"atable"}) {
		t.Fatalf("expected tables %v, got %v\n", []string{"atable"}, qd.Tables())
	}

	if !slices.Equal(qd.Fields(), []string{"first", "second"}) {
		t.Fatalf("expected fields %v, got %v\n", []string{"first", "second"}, qd.Fields())
	}

	pred := qd.Pred()
	if len(pred.Terms()) != 2 {
		t.Fatalf("expected 2 terms, got %d\n", len(pred.Terms()))
	}

	feq1 := pred.Terms()[0]
	if feq1.Lhs().AsFieldName() != "first" {
		t.Fatalf("expected field to be %q, got %q\n", "first", feq1.Lhs().AsFieldName())
	}

	if got := feq1.Rhs().AsConstant().AsInt(); got != 1 {
		t.Fatalf("expected value to be %d, got %d\n", 1, got)
	}

	feq2 := pred.Terms()[1]
	if feq2.Lhs().AsFieldName() != "second" {
		t.Fatalf("expected field to be %q, got %q\n", "second", feq2.Lhs().AsFieldName())
	}

	if got := feq2.Rhs().AsConstant().AsString(); got != "second" {
		t.Fatalf("expected value to be %q, got %q\n", "second", got)
	}
}

func TestUpdateCommandSimple(t *testing.T) {
	const src = "UPDATE atable SET col = 5 WHERE anothercol = 3"

	p := NewParser(src)
	cmd, err := p.UpdateCmd()
	if err != nil {
		t.Fatal(err)
	}

	upd := cmd.(*ModifyData)
	if upd.TblName != "atable" {
		t.Fatalf("expected table %q, got %q\n", "atable", upd.TblName)
	}

	if upd.FldName != "col" {
		t.Fatalf("expected field %q, got %q\n", "col", upd.FldName)
	}

	if v := upd.NewVal.AsConstant().AsInt(); v != 5 {
		t.Fatalf("expected new value to be %d, got %d", 5, upd.NewVal.AsConstant().AsInt())
	}

	terms := upd.Pred.Terms()
	if len(terms) != 1 {
		t.Fatalf("expected 1 term, got %d\n", len(terms))
	}

	if terms[0].Lhs().AsFieldName() != "anothercol" {
		t.Fatalf("expected field to be %q, got %q\n", "first", terms[0].Lhs().AsFieldName())
	}

	if v := terms[0].Rhs().AsConstant().AsInt(); v != 3 {
		t.Fatalf("expected value to be %d, got %d\n", 3, v)
	}
}

func TestDeleteCommand(t *testing.T) {
	const src = "DELETE FROM atable WHERE acol = 5"
	p := NewParser(src)
	cmd, err := p.UpdateCmd()
	if err != nil {
		t.Fatal(err)
	}
	del := cmd.(*DeleteData)

	if del.TblName != "atable" {
		t.Fatalf("expected target table to be %q, got %q\n", "atable", del.TblName)
	}

	terms := del.Pred.Terms()
	if len(terms) != 1 {
		t.Fatalf("expected 1 term, got %d\n", len(terms))
	}

	term := terms[0]
	if term.Lhs().AsFieldName() != "acol" {
		t.Fatalf("expected field to be %q, got %q\n", "acol", term.Lhs().AsFieldName())
	}

	if v := term.Rhs().AsConstant().AsInt(); v != 5 {
		t.Fatalf("expected value to be %d, got %d\n", 5, v)
	}
}

func TestInsertCommand(t *testing.T) {
	const src = "INSERT INTO atable (acol1, acol2) VALUES ('aval', 5)"
	p := NewParser(src)
	cmd, err := p.UpdateCmd()
	if err != nil {
		t.Fatal(err)
	}
	ins := cmd.(*InsertData)

	if ins.TblName != "atable" {
		t.Fatalf("expected target table to be %q, got %q\n", "atable", ins.TblName)
	}

	for i, c := range []string{"acol1", "acol2"} {
		if f := ins.Fields[i]; f != c {
			t.Fatalf("expected field %q at index %d, got %q\n", c, i, f)
		}
	}

	if v := ins.Vals[0].AsString(); v != "aval" {
		t.Fatalf("expected value to be %q, got %q\n", "aval", v)
	}

	if v := ins.Vals[1].AsInt(); v != 5 {
		t.Fatalf("expected value to be %d, got %d\n", 5, v)
	}
}

func TestCreateTableCommand(t *testing.T) {
	const src = "CREATE TABLE atable (name VARCHAR(10), age INT)"
	p := NewParser(src)
	cmd, err := p.UpdateCmd()
	if err != nil {
		t.Fatal(err)
	}

	cr := cmd.(*CreateTableData)
	if cr.TblName != "atable" {
		t.Fatalf("expected target table to be %q, got %q\n", "atable", cr.TblName)
	}

	sch := cr.Schema
	if v := sch.Fields()[0]; v != "name" {
		t.Fatalf("expected field to be %q, got %q\n", "name", v)
	}

	if v := sch.Fields()[1]; v != "age" {
		t.Fatalf("expected field to be %q, got %q\n", "age", v)
	}

	if v := sch.FieldType("name"); v != VARCHAR {
		t.Fatalf("expected field %q to be of type %d, got %d\n", "name", VARCHAR, v)
	}

	if v := sch.FieldType("age"); v != INTEGER {
		t.Fatalf("expected field %q to be of type %d, got %d\n", "name", INTEGER, v)
	}

	if v := sch.Length("name"); v != 10 {
		t.Fatalf("expected field length of %q to be of %d, got %d\n", "name", 10, v)
	}

	if v := sch.Length("age"); v != 0 {
		t.Fatalf("expected field length of %q to be of %d, got %d\n", "age", 0, v)
	}
}

func TestCreateViewData(t *testing.T) {
	const src = "CREATE VIEW view1 AS SELECT first, second FROM atable WHERE first = 1 AND second = 'second'"
	p := NewParser(src)
	cmd, err := p.UpdateCmd()
	if err != nil {
		t.Fatal(err)
	}
	cv := cmd.(*CreateViewData)

	if cv.ViewName != "view1" {
		t.Fatalf("expected view name to be %q, got %q\n", "view1", cv.ViewName)
	}

	qd := cv.QueryData
	if !slices.Equal(qd.Tables(), []string{"atable"}) {
		t.Fatalf("expected tables %v, got %v\n", []string{"atable"}, qd.Tables())
	}

	if !slices.Equal(qd.Fields(), []string{"first", "second"}) {
		t.Fatalf("expected fields %v, got %v\n", []string{"first", "second"}, qd.Fields())
	}

	pred := qd.Pred()
	if len(pred.Terms()) != 2 {
		t.Fatalf("expected 2 terms, got %d\n", len(pred.Terms()))
	}

	feq1 := pred.Terms()[0]
	if feq1.Lhs().AsFieldName() != "first" {
		t.Fatalf("expected field to be %q, got %q\n", "first", feq1.Lhs().AsFieldName())
	}

	if got := feq1.Rhs().AsConstant().AsInt(); got != 1 {
		t.Fatalf("expected value to be %d, got %d\n", 1, got)
	}

	feq2 := pred.Terms()[1]
	if feq2.Lhs().AsFieldName() != "second" {
		t.Fatalf("expected field to be %q, got %q\n", "second", feq2.Lhs().AsFieldName())
	}

	if got := feq2.Rhs().AsConstant().AsString(); got != "second" {
		t.Fatalf("expected value to be %q, got %q\n", "second", got)
	}
}

func TestCreateIndexData(t *testing.T) {
	const src = "CREATE INDEX idx1 ON tbl1 (col1)"
	p := NewParser(src)
	cmd, err := p.UpdateCmd()
	if err != nil {
		t.Fatal(err)
	}
	ci := cmd.(*CreateIndexData)

	if ci.IdxName != "idx1" {
		t.Fatalf("expected index name to be %q, got %q\n", "idx1", ci.IdxName)
	}

	if ci.TblName != "tbl1" {
		t.Fatalf("expected table name to be %q, got %q\n", "tbl1", ci.TblName)
	}

	if ci.FldName != "col1" {
		t.Fatalf("expected index field name to be %q, got %q\n", "col1", ci.FldName)
	}
}
