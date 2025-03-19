package sql

import "testing"

func TestTokenizer(t *testing.T) {
	const src = "SELECT * FROM identifier;"

	tokens, err := Tokenize(src)
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range []string{
		"select",
		"*",
		"from",
		"identifier",
		";",
	} {
		if tokens[i].Lexeme != v {
			t.Fatalf("expected token to be %q, got %v\n", v, tokens[i].Lexeme)
		}
	}

	expected := []Token{
		{
			TokenType: TokenSelect,
			Lexeme:    "select",
			Value:     nil,
			Line:      1,
		},
		{
			TokenType: TokenStar,
			Lexeme:    "*",
			Value:     nil,
			Line:      1,
		},
		{
			TokenType: TokenFrom,
			Lexeme:    "from",
			Value:     nil,
			Line:      1,
		},
		{
			TokenType: TokenIdentifier,
			Lexeme:    "identifier",
			Value:     nil,
			Line:      1,
		},
		{
			TokenType: TokenSemicolon,
			Lexeme:    ";",
			Value:     nil,
			Line:      1,
		},
	}

	for i := range tokens {
		if tokens[i] != expected[i] {
			t.Fatalf("expected token %+v, got %+v\n", expected[i], tokens[i])
		}
	}
}

func TestKeywords(t *testing.T) {
	t.Parallel()

	type test struct {
		src string
		exp tokenType
	}

	for _, tc := range []test{
		{
			src: "CREATE",
			exp: TokenCreate,
		},
		{
			src: "DELETE",
			exp: TokenDelete,
		},
		{
			src: "FROM",
			exp: TokenFrom,
		},
		{
			src: "INSERT",
			exp: TokenInsert,
		},
		{
			src: "INTO",
			exp: TokenInto,
		},
		{
			src: "INDEX",
			exp: TokenIndex,
		},
		{
			src: "SELECT",
			exp: TokenSelect,
		},
		{
			src: "UPDATE",
			exp: TokenUpdate,
		},
		{
			src: "WHERE",
			exp: TokenWhere,
		},
		{
			src: "AND",
			exp: TokenAnd,
		},
		{
			src: "VALUES",
			exp: TokenValues,
		},
		{
			src: "SET",
			exp: TokenSet,
		},
		{
			src: "TABLE",
			exp: TokenTable,
		},
		{
			src: "VARCHAR",
			exp: TokenVarchar,
		},
		{
			src: "INT",
			exp: TokenInt,
		},
		{
			src: "VIEW",
			exp: TokenView,
		},
		{
			src: "AS",
			exp: TokenAs,
		},
		{
			src: "ON",
			exp: TokenOn,
		},
	} {
		tc := tc

		t.Run(tc.src, func(t *testing.T) {

			tokenizer := NewTokenizer(tc.src)
			token, err := tokenizer.NextToken()
			if err != nil {
				t.Fatal(err)
			}

			if token.TokenType != tc.exp {
				t.Fatalf("expected token of type %+v for keyword %q, got %+v\n", tc.exp, tc.src, token.TokenType)
			}
		})
	}
}
