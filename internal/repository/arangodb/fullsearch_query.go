package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	EQ "github.com/IBM/fp-go/v2/eq"
	F "github.com/IBM/fp-go/v2/function"
	L "github.com/IBM/fp-go/v2/optics/lens"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
)

type (
	fullSearchColl int

	// fullSearchField describes one searched annotation or cvterm field and
	// its 0-based index within its stage group.
	fullSearchField struct {
		index      int
		label      string
		coll       fullSearchColl
		targetBind string
		isGenotype bool
	}

	// fullSearchStage describes one of the 4 branch stages (prefix, fuzzy,
	// token, phrase): its LET variable prefix, SEARCH expression builder,
	// and score band.
	fullSearchStage struct {
		namePrefix string
		searchExpr func(fullSearchField) string
		band       string
	}

	// fullSearchBranchState carries the stage, the field specification, and
	// the queryBranch under construction through the branch builder pipeline.
	fullSearchBranchState struct {
		stage  fullSearchStage
		field  fullSearchField
		branch queryBranch
	}
)

const (
	fullSearchCollAnno fullSearchColl = iota
	fullSearchCollTerm
)

var (
	// fullSearchPrefixFuzzyFields are the 9 fields that get prefix (p0..p8)
	// and fuzzy (n0..n8) branches.
	fullSearchPrefixFuzzyFields = []fullSearchField{
		{index: 0, label: fieldSynonym, coll: fullSearchCollAnno, targetBind: bindSynonymTermID},
		{index: 1, label: fieldGenotype, coll: fullSearchCollAnno, targetBind: bindGenotypeTermID, isGenotype: true},
		{index: 2, label: fieldSystematicName, coll: fullSearchCollAnno, targetBind: bindSysnameTermID},
		{index: 3, label: fieldGeneticModification, coll: fullSearchCollAnno, targetBind: bindMuttypeTermID},
		{index: 4, label: fieldMutagenesisMethod, coll: fullSearchCollAnno, targetBind: bindMutmethodTermID},
		{index: 5, label: fieldPhenotype, coll: fullSearchCollTerm, targetBind: bindPhenotypeGraphID},
		{index: 6, label: fieldEnvironment, coll: fullSearchCollTerm, targetBind: bindEnvGraphID},
		{index: 7, label: fieldAssay, coll: fullSearchCollTerm, targetBind: bindAssayGraphID},
		{index: 8, label: fieldCharacteristic, coll: fullSearchCollTerm, targetBind: bindCharGraphID},
	}

	// fullSearchTextFields are the 5 fields that get token (t0..t4) and
	// phrase (h0..h4) branches under text_en.
	fullSearchTextFields = []fullSearchField{
		{index: 0, label: fieldPublicNote, coll: fullSearchCollAnno, targetBind: bindPubnoteTermID},
		{index: 1, label: fieldPhenotype, coll: fullSearchCollTerm, targetBind: bindPhenotypeGraphID},
		{index: 2, label: fieldEnvironment, coll: fullSearchCollTerm, targetBind: bindEnvGraphID},
		{index: 3, label: fieldAssay, coll: fullSearchCollTerm, targetBind: bindAssayGraphID},
		{index: 4, label: fieldCharacteristic, coll: fullSearchCollTerm, targetBind: bindCharGraphID},
	}

	// Lenses on fullSearchField.
	fieldIndexLens = L.MakeLens(
		func(f fullSearchField) int { return f.index },
		func(f fullSearchField, v int) fullSearchField {
			f.index = v
			return f
		},
	)
	fieldLabelLens = L.MakeLens(
		func(f fullSearchField) string { return f.label },
		func(f fullSearchField, v string) fullSearchField {
			f.label = v
			return f
		},
	)
	fieldCollLens = L.MakeLens(
		func(f fullSearchField) fullSearchColl { return f.coll },
		func(f fullSearchField, v fullSearchColl) fullSearchField {
			f.coll = v
			return f
		},
	)
	fieldTargetBindLens = L.MakeLens(
		func(f fullSearchField) string { return f.targetBind },
		func(f fullSearchField, v string) fullSearchField {
			f.targetBind = v
			return f
		},
	)
	fieldIsGenotypeLens = L.MakeLens(
		func(f fullSearchField) bool { return f.isGenotype },
		func(f fullSearchField, v bool) fullSearchField {
			f.isGenotype = v
			return f
		},
	)

	// Lenses on fullSearchBranchState and composed lenses into field and branch.
	fullSearchStageLens = L.MakeLens(
		func(s fullSearchBranchState) fullSearchStage { return s.stage },
		func(s fullSearchBranchState, v fullSearchStage) fullSearchBranchState {
			s.stage = v
			return s
		},
	)
	fullSearchFieldLens = L.MakeLens(
		func(s fullSearchBranchState) fullSearchField { return s.field },
		func(s fullSearchBranchState, v fullSearchField) fullSearchBranchState {
			s.field = v
			return s
		},
	)
	fullSearchBranchLens = L.MakeLens(
		func(s fullSearchBranchState) queryBranch { return s.branch },
		func(s fullSearchBranchState, v queryBranch) fullSearchBranchState {
			s.branch = v
			return s
		},
	)
	fullSearchStateIndexLens = F.Pipe1(
		fullSearchFieldLens,
		L.Compose[fullSearchBranchState](fieldIndexLens),
	)
	fullSearchStateLabelLens = F.Pipe1(
		fullSearchFieldLens,
		L.Compose[fullSearchBranchState](fieldLabelLens),
	)
	fullSearchStateTargetBindLens = F.Pipe1(
		fullSearchFieldLens,
		L.Compose[fullSearchBranchState](fieldTargetBindLens),
	)
	fullSearchStateNameLens = F.Pipe1(
		fullSearchBranchLens,
		L.Compose[fullSearchBranchState](nameLens),
	)
	fullSearchStateBodyLens = F.Pipe1(
		fullSearchBranchLens,
		L.Compose[fullSearchBranchState](bodyLens),
	)

	// isTermCollField reports whether a field lives on the cvterm collection.
	fullSearchCollEq = EQ.FromStrictEquals[fullSearchColl]()
	isTermCollField  = F.Pipe2(
		fullSearchCollTerm,
		EQ.Equals(fullSearchCollEq),
		P.ContraMap(fieldCollLens.Get),
	)

	// docAttrForField selects "value" for annotation fields and "label" for
	// cvterm fields.
	docAttrForField = F.Pipe1(
		isTermCollField,
		P.Fold(annoDocAttr, termDocAttr),
	)

	// prefixExprForField selects the genotype whole+allele prefix pair or
	// the single-analyzer STARTS_WITH expression.
	prefixExprForField = F.Pipe1(
		fieldIsGenotypeLens.Get,
		P.Fold(standardFullSearchPrefixExpr, genotypeFullSearchPrefixExpr),
	)

	// templateForField selects FullSearchAnnoBranchQ or FullSearchTermBranchQ.
	templateForField = F.Pipe1(
		isTermCollField,
		P.Fold(annoBranchTemplate, termBranchTemplate),
	)

	// Stage definitions for the 4 full-search branch types.
	fullSearchPrefixStage = fullSearchStage{
		namePrefix: "p",
		searchExpr: prefixExprForField,
		band:       fullSearchPrefixBand,
	}
	fullSearchFuzzyStage = fullSearchStage{
		namePrefix: "n",
		searchExpr: fuzzyExprForField,
		band:       fullSearchFuzzyBand,
	}
	fullSearchTokenStage = fullSearchStage{
		namePrefix: "t",
		searchExpr: tokenExprForField,
		band:       fullSearchTokenBand,
	}
	fullSearchPhraseStage = fullSearchStage{
		namePrefix: "h",
		searchExpr: phraseExprForField,
		band:       fullSearchPhraseBand,
	}

	// prefixFuzzyBranchBuilders are the prefix and fuzzy builders applied
	// across fullSearchPrefixFuzzyFields via A.Ap.
	prefixFuzzyBranchBuilders = []func(fullSearchField) queryBranch{
		F.Bind1st(buildFullSearchBranch, fullSearchPrefixStage),
		F.Bind1st(buildFullSearchBranch, fullSearchFuzzyStage),
	}

	// tokenPhraseBranchBuilders are the token and phrase builders applied
	// across fullSearchTextFields via A.Ap.
	tokenPhraseBranchBuilders = []func(fullSearchField) queryBranch{
		F.Bind1st(buildFullSearchBranch, fullSearchTokenStage),
		F.Bind1st(buildFullSearchBranch, fullSearchPhraseStage),
	}
)

// annoDocAttr returns the indexed attribute name on the annotation collection.
func annoDocAttr(fullSearchField) string {
	return docFieldValue
}

// termDocAttr returns the indexed attribute name on the cvterm collection.
func termDocAttr(fullSearchField) string {
	return docFieldLabel
}

// annoBranchTemplate returns the AQL template for an annotation branch.
func annoBranchTemplate(fullSearchField) string {
	return FullSearchAnnoBranchQ
}

// termBranchTemplate returns the AQL template for a cvterm branch.
func termBranchTemplate(fullSearchField) string {
	return FullSearchTermBranchQ
}

// formatWithTemplate substitutes the 5 slice items into tpl.
func formatWithTemplate(tpl string, parts []any) string {
	return fmt.Sprintf(tpl, parts...)
}

// standardFullSearchPrefixExpr builds the STARTS_WITH expression for a
// non-genotype annotation or cvterm field.
func standardFullSearchPrefixExpr(f fullSearchField) string {
	attr := docAttrForField(f)

	return fmt.Sprintf(
		"ANALYZER(STARTS_WITH(d.%s, @q), %q)",
		attr,
		fullSearchNormAnalyzer,
	)
}

// genotypeFullSearchPrefixExpr builds the whole-value and allele-token
// STARTS_WITH pair for the genotype field.
func genotypeFullSearchPrefixExpr(fullSearchField) string {
	return fmt.Sprintf(
		"(ANALYZER(STARTS_WITH(d.%s, @q), %q) OR ANALYZER(STARTS_WITH(d.%s, @q), %q))",
		docFieldValue,
		fullSearchNormAnalyzer,
		docFieldValue,
		fullSearchDelimAnalyzer,
	)
}

// fuzzyExprForField builds the NGRAM_MATCH expression for a prefix/fuzzy field.
func fuzzyExprForField(f fullSearchField) string {
	attr := docAttrForField(f)

	return fmt.Sprintf(
		"NGRAM_MATCH(d.%s, @q, @th, %q)",
		attr,
		fullSearchNgramAnalyzer,
	)
}

// tokenExprForField builds the IN TOKENS expression for a text field.
func tokenExprForField(f fullSearchField) string {
	attr := docAttrForField(f)

	return fmt.Sprintf(
		"ANALYZER(d.%s IN TOKENS(@q, %q), %q)",
		attr,
		fullSearchTextAnalyzer,
		fullSearchTextAnalyzer,
	)
}

// phraseExprForField builds the PHRASE expression for a text field.
func phraseExprForField(f fullSearchField) string {
	attr := docAttrForField(f)

	return fmt.Sprintf(
		"PHRASE(d.%s, @q, %q)",
		attr,
		fullSearchTextAnalyzer,
	)
}

// setFullSearchName writes the stage LET variable name (<prefix><index>) into s.branch.name.
func setFullSearchName(s fullSearchBranchState) fullSearchBranchState {
	stage := fullSearchStageLens.Get(s)
	name := fmt.Sprintf("%s%d", stage.namePrefix, fullSearchStateIndexLens.Get(s))

	return F.Pipe1(s, fullSearchStateNameLens.Set(name))
}

// fullSearchBranchParts builds the 5-item slice for FullSearchAnnoBranchQ or FullSearchTermBranchQ in verb order.
func fullSearchBranchParts(st fullSearchBranchState) []any {
	stage := fullSearchStageLens.Get(st)
	field := fullSearchFieldLens.Get(st)

	return F.Pipe5(
		// Start with an empty []any slice for the fmt.Sprintf arguments.
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name (for example, "p0" or "t1").
		A.Push[any](fullSearchStateNameLens.Get(st)),
		// 2nd %s: append the AQL SEARCH expression on d.value or d.label.
		A.Push[any](stage.searchExpr(field)),
		// 3rd %s: append the bind parameter name of the required cvterm _id or graph_id.
		A.Push[any](fullSearchStateTargetBindLens.Get(st)),
		// 4th %q: append the canonical field label returned in each row.
		A.Push[any](fullSearchStateLabelLens.Get(st)),
		// 5th %s: append the AQL score band expression.
		A.Push[any](stage.band),
	)
}

// setFullSearchBranchBody flips fullSearchStateBodyLens.Set so it accepts the rendered body string last.
func setFullSearchBranchBody(
	s fullSearchBranchState,
) func(string) fullSearchBranchState {
	return F.Pipe1(s, F.Flip(fullSearchStateBodyLens.Set))
}

// setFullSearchBody builds the AQL branch body from s and stores it in s.branch.body.
func setFullSearchBody(s fullSearchBranchState) fullSearchBranchState {
	tpl := F.Pipe2(s, fullSearchFieldLens.Get, templateForField)

	return F.Pipe3(
		s,
		fullSearchBranchParts,
		F.Bind1st(formatWithTemplate, tpl),
		setFullSearchBranchBody(s),
	)
}

// buildFullSearchBranch builds one queryBranch for stage and field f.
func buildFullSearchBranch(
	stage fullSearchStage,
	f fullSearchField,
) queryBranch {
	return F.Pipe5(
		fullSearchBranchState{},
		fullSearchStageLens.Set(stage),
		fullSearchFieldLens.Set(f),
		setFullSearchName,
		setFullSearchBody,
		fullSearchBranchLens.Get,
	)
}

// prefixFuzzyBranches builds the 18 prefix (p0..p8) and fuzzy (n0..n8) branches.
func prefixFuzzyBranches() []queryBranch {
	return F.Pipe1(
		prefixFuzzyBranchBuilders,
		A.Ap[queryBranch](fullSearchPrefixFuzzyFields),
	)
}

// tokenPhraseBranches builds the 10 token (t0..t4) and phrase (h0..h4) branches.
func tokenPhraseBranches() []queryBranch {
	return F.Pipe1(
		tokenPhraseBranchBuilders,
		A.Ap[queryBranch](fullSearchTextFields),
	)
}

// appendFullSearchMergeBranch builds the merge block from the 28 branch names and appends it as a branch.
func appendFullSearchMergeBranch(branches []queryBranch) []queryBranch {
	setBody := F.Pipe1(queryBranch{}, F.Flip(bodyLens.Set))

	return F.Pipe6(
		branches,
		A.Map(nameLens.Get),
		A.Intersperse(", "),
		A.Fold(S.Monoid),
		S.Format[string](FullSearchMergeQ),
		setBody,
		F.Bind1st(A.Append, branches),
	)
}

// buildFullSearchQuery generates the complete 28-branch AQL statement.
func buildFullSearchQuery() string {
	prefixFuzzy := prefixFuzzyBranches()
	tokenPhrase := tokenPhraseBranches()

	return F.Pipe5(
		A.From(prefixFuzzy, tokenPhrase),
		A.Flatten[queryBranch],
		appendFullSearchMergeBranch,
		A.Map(bodyLens.Get),
		A.Intersperse("\n"),
		A.Fold(S.Monoid),
	)
}
