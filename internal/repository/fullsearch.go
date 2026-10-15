package repository

import (
	"strings"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	P "github.com/IBM/fp-go/v2/predicate"
	R "github.com/IBM/fp-go/v2/record"
)

var (
	// fullSearchStopwords is the English stopword table for the token stage.
	// Built-in text_en keeps stopwords on ArangoDB 3.11, so both the service
	// and repository reject a query whose tokens are all stopwords.
	fullSearchStopwords = map[string]struct{}{
		"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {},
		"be": {}, "but": {}, "by": {}, "for": {}, "from": {}, "had": {},
		"has": {}, "have": {}, "if": {}, "in": {}, "is": {}, "it": {},
		"its": {}, "not": {}, "of": {}, "on": {}, "or": {}, "so": {},
		"than": {}, "that": {}, "the": {}, "their": {}, "then": {},
		"there": {}, "these": {}, "this": {}, "to": {}, "too": {},
		"very": {}, "was": {}, "were": {}, "what": {}, "when": {},
		"which": {}, "while": {}, "who": {}, "will": {}, "with": {},
	}

	// isStopword reports whether a single normalized token is in fullSearchStopwords.
	isStopword = F.Bind2nd(R.Has[string, struct{}], fullSearchStopwords)

	// isNonStopword reports whether a token is not a stopword.
	isNonStopword = P.Not(isStopword)

	// hasNonStopwordToken reports whether a token slice contains at least one
	// non-stopword token. An empty slice returns false.
	hasNonStopwordToken = A.Any(isNonStopword)

	// keepsNonStopwordWord splits a normalized query on whitespace and checks
	// for at least one non-stopword token.
	keepsNonStopwordWord = F.Flow2(strings.Fields, hasNonStopwordToken)
)

// IsAllStopwords reports whether every whitespace-separated word of a
// normalized query is an English stopword (or the query has no words).
func IsAllStopwords(query string) bool {
	return F.Pipe1(
		query,
		P.Not(keepsNonStopwordWord),
	)
}
