package record

import "testing"

func TestMatchStringConstant(t *testing.T) {
	const exp = "'testname'"
	lx := NewLexer(NewTokenizer(exp))

	v, err := lx.EatStringValue()
	if err != nil {
		t.Fatal(err)
	}

	if v != "testname" {
		t.Fatalf("expected a value of testname, got %s", v)
	}
}

func TestMatchIntConstant(t *testing.T) {
	lx := NewLexer(NewTokenizer("123"))

	v, err := lx.EatIntValue()
	if err != nil {
		t.Fatal(err)
	}

	if v != 123 {
		t.Fatalf("expected a value of 123, got %d", v)
	}
}

func TestMatchKeywords(t *testing.T) {
	for _, v := range []string{
		"select",
		"from",
		"where",
		"and",
		"insert",
		"into",
		"values",
		"delete",
		"update",
		"set",
		"create",
		"table",
		"varchar",
		"int",
		"view",
		"as",
		"index",
		"on",
	} {
		lx := NewLexer(NewTokenizer(v))

		if err := lx.EatKeyword(v); err != nil {
			t.Fatalf("unexpected %s error for keyword %q", err, v)
		}
	}

	lx := NewLexer(NewTokenizer("notakeyword"))
	val, err := lx.EatIdentifier()
	if err != nil {
		t.Fatal(err.Error())
	}

	if val != "notakeyword" {
		t.Fatalf("unexpected identifier, expected notakeyword, got %q", val)
	}
}
