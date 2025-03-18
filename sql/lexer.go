package sql

import (
	"errors"
	"io"
)

var ErrInvalidSyntax = errors.New("invalid syntax")

type Lexer struct {
	tokenizer *Tokenizer
	current   Token
}

func NewLexer(tokenizer *Tokenizer) *Lexer {
	lx := &Lexer{
		tokenizer: tokenizer,
	}
	lx.nextToken()
	return lx
}

func (l *Lexer) MatchTokenType(t tokenType) bool {
	return l.current.TokenType == t
}

func (l *Lexer) MatchIntValue() bool {
	return l.MatchTokenType(TokenInt)
}

func (l *Lexer) MatchStringValue() bool {
	return l.MatchTokenType(TokenString)
}

func (l *Lexer) MatchKeyword(keyword string) bool {
	return l.current.Lexeme == keyword
}

func (l *Lexer) MatchIdentifier() bool {
	return l.MatchTokenType(TokenIdentifier)
}

func (l *Lexer) EatTokenType(t tokenType) error {
	if !l.MatchTokenType(t) {
		return ErrInvalidSyntax
	}
	return l.nextToken()
}

func (l *Lexer) EatIntValue() (int, error) {
	if !l.MatchIntValue() {
		return 0, ErrInvalidSyntax
	}

	defer l.nextToken()
	return TokenToIntValue(l.current)
}

func (l *Lexer) EatStringValue() (string, error) {
	if !l.MatchStringValue() {
		return "", ErrInvalidSyntax
	}

	defer l.nextToken()
	return l.current.Value.(string), nil
}

func (l *Lexer) EatKeyword(kw string) error {
	if !l.MatchKeyword(kw) {
		return ErrInvalidSyntax
	}
	l.nextToken()
	return nil
}

func (l *Lexer) EatIdentifier() (string, error) {
	if !l.MatchIdentifier() {
		return "", ErrInvalidSyntax
	}
	defer l.nextToken()
	return l.current.Value.(string), nil
}

func (l *Lexer) nextToken() error {
	token, err := l.tokenizer.NextToken()
	if err != nil && err != io.EOF {
		return ErrInvalidSyntax
	}
	l.current = token
	return nil
}
