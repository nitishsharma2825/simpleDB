package sql

type PredParser struct {
	lexer *Lexer
}

func NewPredParser(source string) *PredParser {
	return &PredParser{
		lexer: NewLexer(NewTokenizer(source)),
	}
}

func (pp *PredParser) Field() (string, error) {
	return pp.lexer.EatIdentifier()
}

func (pp *PredParser) Constant() {
	if pp.lexer.MatchStringValue() {
		pp.lexer.EatStringValue()
	} else {
		pp.lexer.EatIntValue()
	}
}

func (pp *PredParser) Expression() {
	if pp.lexer.MatchIdentifier() {
		pp.Field()
	} else {
		pp.Constant()
	}
}

func (pp *PredParser) Term() {
	pp.Expression()
	pp.lexer.EatTokenType(TokenEqual)
	pp.Expression()
}

func (pp *PredParser) Predicate() {
	pp.Term()
	if pp.lexer.MatchKeyword("and") {
		pp.lexer.EatTokenType(TokenAnd)
		pp.Predicate()
	}
}
