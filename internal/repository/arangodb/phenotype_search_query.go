package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	IO "github.com/IBM/fp-go/v2/io"
	L "github.com/IBM/fp-go/v2/optics/lens"
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

	// phenotypeSearchBranchState carries the stage, the field specification,
	// and the queryBranch under construction through the branch builder pipeline.
	phenotypeSearchBranchState struct {
		stage  phenotypeSearchStage
		field  phenotypeSearchField
		branch queryBranch
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

	// Lenses on phenotypeSearchField.
	phenotypeFieldIndexLens = L.MakeLens(
		func(f phenotypeSearchField) int { return f.index },
		func(f phenotypeSearchField, v int) phenotypeSearchField {
			f.index = v
			return f
		},
	)
	phenotypeFieldLabelLens = L.MakeLens(
		func(f phenotypeSearchField) string { return f.label },
		func(f phenotypeSearchField, v string) phenotypeSearchField {
			f.label = v
			return f
		},
	)
	phenotypeFieldAttrLens = L.MakeLens(
		func(f phenotypeSearchField) string { return f.attr },
		func(f phenotypeSearchField, v string) phenotypeSearchField {
			f.attr = v
			return f
		},
	)

	// Lenses on phenotypeSearchBranchState and composed lenses into field and branch.
	phenotypeSearchStageLens = L.MakeLens(
		func(s phenotypeSearchBranchState) phenotypeSearchStage { return s.stage },
		func(s phenotypeSearchBranchState, v phenotypeSearchStage) phenotypeSearchBranchState {
			s.stage = v
			return s
		},
	)
	phenotypeSearchFieldLens = L.MakeLens(
		func(s phenotypeSearchBranchState) phenotypeSearchField { return s.field },
		func(s phenotypeSearchBranchState, v phenotypeSearchField) phenotypeSearchBranchState {
			s.field = v
			return s
		},
	)
	phenotypeSearchBranchLens = L.MakeLens(
		func(s phenotypeSearchBranchState) queryBranch { return s.branch },
		func(s phenotypeSearchBranchState, v queryBranch) phenotypeSearchBranchState {
			s.branch = v
			return s
		},
	)
	phenotypeStateIndexLens = F.Pipe1(
		phenotypeSearchFieldLens,
		L.Compose[phenotypeSearchBranchState](phenotypeFieldIndexLens),
	)
	phenotypeStateLabelLens = F.Pipe1(
		phenotypeSearchFieldLens,
		L.Compose[phenotypeSearchBranchState](phenotypeFieldLabelLens),
	)
	phenotypeStateAttrLens = F.Pipe1(
		phenotypeSearchFieldLens,
		L.Compose[phenotypeSearchBranchState](phenotypeFieldAttrLens),
	)
	phenotypeStateNameLens = F.Pipe1(
		phenotypeSearchBranchLens,
		L.Compose[phenotypeSearchBranchState](nameLens),
	)
	phenotypeStateBodyLens = F.Pipe1(
		phenotypeSearchBranchLens,
		L.Compose[phenotypeSearchBranchState](bodyLens),
	)

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

// setPhenotypeSearchName writes the stage LET variable name (<prefix><index>)
// into s.branch.name.
func setPhenotypeSearchName(
	s phenotypeSearchBranchState,
) phenotypeSearchBranchState {
	stage := phenotypeSearchStageLens.Get(s)
	name := fmt.Sprintf("%s%d", stage.namePrefix, phenotypeStateIndexLens.Get(s))

	return F.Pipe1(s, phenotypeStateNameLens.Set(name))
}

// phenotypeSearchBranchParts builds the 4-item slice for PhenotypeSearchBranchQ
// in verb order.
func phenotypeSearchBranchParts(st phenotypeSearchBranchState) []any {
	stage := phenotypeSearchStageLens.Get(st)
	attr := phenotypeStateAttrLens.Get(st)

	return F.Pipe4(
		A.Empty[any](),
		A.Push[any](phenotypeStateNameLens.Get(st)),
		A.Push[any](stage.searchExpr(attr)),
		A.Push[any](phenotypeStateLabelLens.Get(st)),
		A.Push[any](stage.band),
	)
}

// setPhenotypeSearchBranchBody flips phenotypeStateBodyLens.Set so it accepts
// the rendered body string last.
func setPhenotypeSearchBranchBody(
	s phenotypeSearchBranchState,
) func(string) phenotypeSearchBranchState {
	return F.Pipe1(s, F.Flip(phenotypeStateBodyLens.Set))
}

// setPhenotypeSearchBody builds the AQL branch body from s and stores it in
// s.branch.body.
func setPhenotypeSearchBody(
	s phenotypeSearchBranchState,
) phenotypeSearchBranchState {
	return F.Pipe3(
		s,
		phenotypeSearchBranchParts,
		F.Bind1st(formatWithTemplate, PhenotypeSearchBranchQ),
		setPhenotypeSearchBranchBody(s),
	)
}

// buildPhenotypeSearchBranch builds one queryBranch for stage and field f.
func buildPhenotypeSearchBranch(
	stage phenotypeSearchStage,
	f phenotypeSearchField,
) queryBranch {
	return F.Pipe5(
		phenotypeSearchBranchState{},
		phenotypeSearchStageLens.Set(stage),
		phenotypeSearchFieldLens.Set(f),
		setPhenotypeSearchName,
		setPhenotypeSearchBody,
		phenotypeSearchBranchLens.Get,
	)
}

// appendPhenotypeSearchMergeBranch builds the merge block from the 12 branch
// names and appends it as a branch.
func appendPhenotypeSearchMergeBranch(branches []queryBranch) []queryBranch {
	setBody := F.Pipe1(queryBranch{}, F.Flip(bodyLens.Set))

	return F.Pipe6(
		branches,
		A.Map(nameLens.Get),
		A.Intersperse(", "),
		A.Fold(S.Monoid),
		S.Format[string](PhenotypeSearchMergeQ),
		setBody,
		F.Bind1st(A.Append, branches),
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

	return F.Pipe5(
		A.From(prefixFuzzy, tokenPhrase),
		A.Flatten[queryBranch],
		appendPhenotypeSearchMergeBranch,
		A.Map(bodyLens.Get),
		A.Intersperse("\n"),
		A.Fold(S.Monoid),
	)
}
