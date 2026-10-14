package arangodb

import (
	"strings"
	"unicode"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
	"golang.org/x/text/unicode/norm"
)

// normalizeAutocompleteQuery trims the caller's query, lowercases it, and
// strips its combining marks, so the query lines up with the normalized field
// the analyzers index. Case and accent then make no difference to a match.
func normalizeAutocompleteQuery(q string) string {
	trimmed := strings.TrimSpace(q)
	lowered := strings.ToLower(trimmed)
	decomposed := norm.NFD.String(lowered)

	filtered := F.Pipe2(
		decomposed,
		S.ToRunes,
		A.Filter(P.Not(isCombiningMark)),
	)

	return string(filtered)
}

// isCombiningMark reports whether a rune is a combining mark. The check runs on
// an NFD-decomposed value, where an accented letter is split into its base
// letter and its mark, so dropping the marks folds the accent away.
func isCombiningMark(r rune) bool {
	return unicode.Is(unicode.Mn, r)
}
