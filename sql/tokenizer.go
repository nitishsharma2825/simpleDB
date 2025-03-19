package sql

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

type tokenType uint16

const (
	// Single character tokens
	TokenLeftParen tokenType = iota
	TokenRightParen
	TokenSemicolon
	TokenComma
	TokenStar

	// one or two character tokens
	TokenEqual
	TokenBang
	TokenLess
	TokenGreater
	TokenBangEqual
	TokenEqualEqual
	TokenLessEqual
	TokenGreaterEqual

	// Identifier
	TokenString
	TokenNumber
	TokenIdentifier

	// keywords
	TokenCreate
	TokenFrom
	TokenDelete
	TokenIndex
	TokenInsert
	TokenInto
	TokenSelect
	TokenUpdate
	TokenWhere
	TokenOrderBy

	TokenBegin
	TokenCommit
	TokenRollback

	TokenAnd
	TokenValues
	TokenSet
	TokenTable
	TokenText
	TokenVarchar
	TokenInt
	TokenView
	TokenAs
	TokenOn

	TokenEOF
)

type Token struct {
	TokenType tokenType
	Lexeme    string
	Value     interface{}
	Line      int
}

type Tokenizer struct {
	source   string
	keywords map[string]tokenType
	start    int
	current  int
	line     int
}

func NewTokenizer(source string) *Tokenizer {
	t := &Tokenizer{
		source:   strings.ToLower(source),
		start:    0,
		current:  0,
		line:     1,
		keywords: make(map[string]tokenType),
	}

	t.keywords["select"] = TokenSelect
	t.keywords["from"] = TokenFrom
	t.keywords["where"] = TokenWhere
	t.keywords["and"] = TokenAnd
	t.keywords["insert"] = TokenInsert
	t.keywords["into"] = TokenInto
	t.keywords["values"] = TokenValues
	t.keywords["delete"] = TokenDelete
	t.keywords["update"] = TokenUpdate
	t.keywords["set"] = TokenSet
	t.keywords["create"] = TokenCreate
	t.keywords["table"] = TokenTable
	t.keywords["int"] = TokenInt
	t.keywords["varchar"] = TokenVarchar
	t.keywords["view"] = TokenView
	t.keywords["as"] = TokenAs
	t.keywords["index"] = TokenIndex
	t.keywords["on"] = TokenOn

	return t
}

func Tokenize(source string) ([]Token, error) {
	t := NewTokenizer(source)
	return t.tokenize()
}

func (t *Tokenizer) tokenize() ([]Token, error) {
	var tokens []Token

	for {
		token, err := t.NextToken()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (t *Tokenizer) NextToken() (Token, error) {
	t.skipWhitespace()
	t.start = t.current
	if t.isAtEnd() {
		return t.makeToken(TokenEOF, nil), io.EOF
	}

	b := t.advance()

	switch b {
	case '(':
		return t.makeToken(TokenLeftParen, nil), nil
	case ')':
		return t.makeToken(TokenRightParen, nil), nil
	case ';':
		return t.makeToken(TokenSemicolon, nil), nil
	case ',':
		return t.makeToken(TokenComma, nil), nil

	case '!':
		if t.match('=') {
			return t.makeToken(TokenBangEqual, nil), nil
		}
		return t.makeToken(TokenBang, nil), nil
	case '=':
		if t.match('=') {
			return t.makeToken(TokenEqualEqual, nil), nil
		}
		return t.makeToken(TokenEqual, nil), nil
	case '<':
		if t.match('=') {
			return t.makeToken(TokenLessEqual, nil), nil
		}
		return t.makeToken(TokenLess, nil), nil
	case '>':
		if t.match('=') {
			return t.makeToken(TokenGreaterEqual, nil), nil
		}
		return t.makeToken(TokenGreater, nil), nil
	case '*':
		return t.makeToken(TokenStar, nil), nil
	case '\'':
		return t.string()
	case '\n':
		t.line++
		t.advance()

	default:
		if isAlpha(b) {
			return t.identifier()
		} else if isDigit(b) {
			return t.number()
		} else {
			return Token{}, errors.New("unexpected character")
		}
	}

	return Token{}, errors.New("unexpected input")
}

func (t *Tokenizer) skipWhitespace() {
	for {
		c := t.peek()
		switch c {
		case ' ', '\r', '\t':
			t.advance()
		case '-': // sql comments
			if t.peekNext() == '-' {
				for t.peek() != '\n' && !t.isAtEnd() {
					t.advance()
				}
			} else {
				return
			}
		default:
			return
		}
	}
}

func (t *Tokenizer) string() (Token, error) {
	for t.peek() != '\'' && !t.isAtEnd() {
		t.advance()
	}

	if t.isAtEnd() {
		return Token{}, errors.New("unterminated string")
	}

	t.advance()

	return t.makeToken(TokenString, t.source[t.start+1:t.current-1]), nil
}

func (t *Tokenizer) number() (Token, error) {
	for isDigit(t.peek()) {
		t.advance()
	}

	return t.makeToken(TokenNumber, t.source[t.start:t.current]), nil
}

func (t *Tokenizer) identifier() (Token, error) {
	for isAlphaNumeric(t.peek()) {
		t.advance()
	}

	text := t.source[t.start:t.current]
	typ, ok := t.keywords[text]
	if !ok {
		typ = TokenIdentifier
	}
	return t.makeToken(typ, nil), nil
}

func (t *Tokenizer) match(expected byte) bool {
	if t.isAtEnd() || t.source[t.current] != expected {
		return false
	}
	t.current++
	return true
}

func (t *Tokenizer) peek() byte {
	if t.isAtEnd() {
		return 0
	}
	return t.source[t.current]
}

func (t *Tokenizer) peekNext() byte {
	if t.current+1 >= len(t.source) {
		return 0
	}
	return t.source[t.current+1]
}

func (t *Tokenizer) advance() byte {
	char := t.source[t.current]
	t.current++
	return char
}

func (t *Tokenizer) isAtEnd() bool {
	return t.current >= len(t.source)
}

func (t *Tokenizer) makeToken(typ tokenType, value interface{}) Token {
	return Token{
		TokenType: typ,
		Lexeme:    t.source[t.start:t.current],
		Value:     value,
		Line:      t.line,
	}
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func isAlpha(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b == '_'
}

func isAlphaNumeric(b byte) bool {
	return isDigit(b) || isAlpha(b)
}

func TokenToIntValue(token Token) (int, error) {
	if token.TokenType != TokenNumber {
		return 0, ErrInvalidSyntax
	}

	return strconv.Atoi(token.Value.(string))
}
