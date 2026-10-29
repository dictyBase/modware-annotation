package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	IO "github.com/IBM/fp-go/v2/io"
	S "github.com/IBM/fp-go/v2/string"
)

type (
	// phenotypeSearchField describes one indexed cvterm attribute path, its
	// canonical field label, and its 0-based index within its stage group.
	phenotypeSearchField struct {
		index int
		label string
		attr  string
	}

	// phenotypeSearchStage describes one of the 4 branch stages (prefix,
	// fuzzy, token, phrase): its LET variable prefix, SEARCH expression
	// builder, and score band.
	phenotypeSearchStage struct {
		namePrefix string
		searchExpr func(string) string
		band       string
	}
)

const (
	docAttrSynonyms   = "metadata.synonyms.value"
	docAttrDefinition = "metadata.definition.value"
	docAttrComments   = "metadata.comments"

	fieldPhenotypeLabel      = "label"
	fieldPhenotypeDefinition = "definition"
	fieldPhenotypeComment    = "comment"
)

var (
	// phenotypePrefixFuzzyFields are the 2 cvterm attributes that get prefix
	// (p0..p1) and fuzzy (n0..n1) branches.
	phenotypePrefixFuzzyFields = []phenotypeSearchField{
		{index: 0, label: fieldPhenotypeLabel, attr: docFieldLabel},
		{index: 1, label: fieldSynonym, attr: docAttrSynonyms},
	}

	// phenotypeTextFields are the 4 cvterm attributes that get token (t0..t3)
	// and phrase (h0..h3) branches under text_en.
	phenotypeTextFields = []phenotypeSearchField{
		{index: 0, label: fieldPhenotypeLabel, attr: docFieldLabel},
		{index: 1, label: fieldSynonym, attr: docAttrSynonyms},
		{index: 2, label: fieldPhenotypeDefinition, attr: docAttrDefinition},
		{index: 3, label: fieldPhenotypeComment, attr: docAttrComments},
	}

	phenotypePrefixStage = phenotypeSearchStage{
		namePrefix: "p",
		searchExpr: phenotypePrefixExpr,
		band:       fullSearchPrefixBand,
	}
	phenotypeFuzzyStage = phenotypeSearchStage{
		namePrefix: "n",
		searchExpr: phenotypeFuzzyExpr,
		band:       fullSearchFuzzyBand,
	}
	phenotypeTokenStage = phenotypeSearchStage{
		namePrefix: "t",
		searchExpr: phenotypeTokenExpr,
		band:       fullSearchTokenBand,
	}
	phenotypePhraseStage = phenotypeSearchStage{
		namePrefix: "h",
		searchExpr: phenotypePhraseExpr,
		band:       fullSearchPhraseBand,
	}

	// phenotypePrefixFuzzyBuilders are the prefix and fuzzy builders applied
	// across phenotypePrefixFuzzyFields via A.Ap.
	phenotypePrefixFuzzyBuilders = []func(phenotypeSearchField) queryBranch{
		F.Bind1st(buildPhenotypeSearchBranch, phenotypePrefixStage),
		F.Bind1st(buildPhenotypeSearchBranch, phenotypeFuzzyStage),
	}

	// phenotypeTokenPhraseBuilders are the token and phrase builders applied
	// across phenotypeTextFields via A.Ap.
	phenotypeTokenPhraseBuilders = []func(phenotypeSearchField) queryBranch{
		F.Bind1st(buildPhenotypeSearchBranch, phenotypeTokenStage),
		F.Bind1st(buildPhenotypeSearchBranch, phenotypePhraseStage),
	}

	// buildPhenotypeSearchQuery memoizes the assembled 12-branch AQL statement
	// so the query string is built once on first call and reused thereafter.
	buildPhenotypeSearchQuery = IO.Memoize(assemblePhenotypeSearchQuery)
)

// phenotypePrefixExpr builds the STARTS_WITH expression for a cvterm attribute.
func phenotypePrefixExpr(attr string) string {
	return fmt.Sprintf(
		"ANALYZER(STARTS_WITH(d.%s, @q), %q)",
		attr,
		fullSearchNormAnalyzer,
	)
}

// phenotypeFuzzyExpr builds the NGRAM_MATCH expression for a cvterm attribute.
func phenotypeFuzzyExpr(attr string) string {
	return fmt.Sprintf(
		"NGRAM_MATCH(d.%s, @q, @th, %q)",
		attr,
		fullSearchNgramAnalyzer,
	)
}

// phenotypeTokenExpr builds the IN TOKENS expression for a cvterm text attribute.
func phenotypeTokenExpr(attr string) string {
	return fmt.Sprintf(
		"ANALYZER(d.%s IN TOKENS(@q, %q), %q)",
		attr,
		fullSearchTextAnalyzer,
		fullSearchTextAnalyzer,
	)
}

// phenotypePhraseExpr builds the PHRASE expression for a cvterm text attribute.
func phenotypePhraseExpr(attr string) string {
	return fmt.Sprintf(
		"PHRASE(d.%s, @q, %q)",
		attr,
		fullSearchTextAnalyzer,
	)
}

// buildPhenotypeSearchBranch builds one queryBranch for stage and field f.
func buildPhenotypeSearchBranch(
	stage phenotypeSearchStage,
	f phenotypeSearchField,
) queryBranch {
	name := fmt.Sprintf("%s%d", stage.namePrefix, f.index)
	search := stage.searchExpr(f.attr)
	parts := F.Pipe4(
		A.Empty[any](),
		A.Push[any](name),
		A.Push[any](search),
		A.Push[any](f.label),
		A.Push[any](stage.band),
	)
	body := formatWithTemplate(PhenotypeSearchBranchQ, parts)

	return F.Pipe2(
		queryBranch{},
		nameLens.Set(name),
		bodyLens.Set(body),
	)
}

// assemblePhenotypeSearchQuery generates the complete 12-branch AQL statement.
func assemblePhenotypeSearchQuery() string {
	prefixFuzzy := F.Pipe1(
		phenotypePrefixFuzzyBuilders,
		A.Ap[queryBranch](phenotypePrefixFuzzyFields),
	)
	tokenPhrase := F.Pipe1(
		phenotypeTokenPhraseBuilders,
		A.Ap[queryBranch](phenotypeTextFields),
	)
	branches := F.Pipe1(
		A.From(prefixFuzzy, tokenPhrase),
		A.Flatten[queryBranch],
	)
	mergeBody := F.Pipe4(
		branches,
		A.Map(nameLens.Get),
		A.Intersperse(", "),
		A.Fold(S.Monoid),
		S.Format[string](PhenotypeSearchMergeQ),
	)
	mergeBranch := F.Pipe1(
		queryBranch{},
		bodyLens.Set(mergeBody),
	)

	return F.Pipe4(
		branches,
		F.Bind2nd(A.Append[queryBranch], mergeBranch),
		A.Map(bodyLens.Get),
		A.Intersperse("\n"),
		A.Fold(S.Monoid),
	)
}
