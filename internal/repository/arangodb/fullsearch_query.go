package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	EQ "github.com/IBM/fp-go/v2/eq"
	F "github.com/IBM/fp-go/v2/function"
	IO "github.com/IBM/fp-go/v2/io"
	L "github.com/IBM/fp-go/v2/optics/lens"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
)

type (
	fullSearchColl int

	// fullSearchField describes one searched annotation or cvterm branch
	// specification and its 0-based index within its stage group.
	fullSearchField struct {
		index       int
		label       string
		coll        fullSearchColl
		targetBind  string
		valueFilter string
		isGenotype  bool
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

	// fullSearchAnnoPrefixTargets lists the 4 whole-value prefix cvterm ID
	// bind parameters combined into branch p0.
	fullSearchAnnoPrefixTargets = "@synonym_term_id, @sysname_term_id, @muttype_term_id, @mutmethod_term_id"
	// fullSearchAnnoPrefixFieldExpr maps a matched whole-value prefix cvterm
	// ID to its canonical field label.
	fullSearchAnnoPrefixFieldExpr = `e._to == @synonym_term_id ? "synonym" : ` +
		`(e._to == @sysname_term_id ? "systematic_name" : ` +
		`(e._to == @muttype_term_id ? "genetic_modification" : "mutagenesis_method"))`
	// fullSearchAnnoFuzzyTargets lists the 3 fuzzy-searched annotation cvterm
	// ID bind parameters combined into branch n0 (excluding mutagenesis_method
	// and genetic_modification).
	fullSearchAnnoFuzzyTargets = "@synonym_term_id, @genotype_term_id, @sysname_term_id"
	// fullSearchAnnoFuzzyFieldExpr maps a matched fuzzy cvterm ID to its
	// canonical field label.
	fullSearchAnnoFuzzyFieldExpr = `e._to == @synonym_term_id ? "synonym" : ` +
		`(e._to == @genotype_term_id ? "genotype" : "systematic_name")`
	// Quoted canonical field label literals for AQL RETURN expressions.
	quotedFieldGenotype       = `"` + fieldGenotype + `"`
	quotedFieldPublicNote     = `"` + fieldPublicNote + `"`
	quotedFieldPhenotype      = `"` + fieldPhenotype + `"`
	quotedFieldEnvironment    = `"` + fieldEnvironment + `"`
	quotedFieldAssay          = `"` + fieldAssay + `"`
	quotedFieldCharacteristic = `"` + fieldCharacteristic + `"`
)

var (
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
	fieldValueFilterLens = L.MakeLens(
		func(f fullSearchField) string { return f.valueFilter },
		func(f fullSearchField, v string) fullSearchField {
			f.valueFilter = v
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

	// fullSearchPrefixFields are the 6 prefix branches (p0..p5): combined
	// whole-value annotation prefix (p0), genotype prefix (p1), and the 4
	// ontology cvterm prefix branches (p2..p5).
	fullSearchPrefixFields = []fullSearchField{
		F.Pipe5(
			fullSearchField{},
			fieldIndexLens.Set(0),
			fieldLabelLens.Set(fullSearchAnnoPrefixFieldExpr),
			fieldCollLens.Set(fullSearchCollAnno),
			fieldTargetBindLens.Set(fullSearchAnnoPrefixTargets),
			fieldValueFilterLens.Set(annoPlaceholderValueFilter),
		),
		F.Pipe6(
			fullSearchField{},
			fieldIndexLens.Set(1),
			fieldLabelLens.Set(quotedFieldGenotype),
			fieldCollLens.Set(fullSearchCollAnno),
			fieldTargetBindLens.Set("@"+bindGenotypeTermID),
			fieldValueFilterLens.Set(annoPlaceholderValueFilter),
			fieldIsGenotypeLens.Set(true),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(2),
			fieldLabelLens.Set(quotedFieldPhenotype),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindPhenotypeGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(3),
			fieldLabelLens.Set(quotedFieldEnvironment),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindEnvGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(4),
			fieldLabelLens.Set(quotedFieldAssay),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindAssayGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(5),
			fieldLabelLens.Set(quotedFieldCharacteristic),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindCharGraphID),
		),
	}

	// fullSearchFuzzyFields are the 5 fuzzy branches (n0..n4): combined
	// annotation fuzzy for synonym, genotype, and systematic_name (n0), and
	// the 4 ontology cvterm fuzzy branches (n1..n4).
	fullSearchFuzzyFields = []fullSearchField{
		F.Pipe5(
			fullSearchField{},
			fieldIndexLens.Set(0),
			fieldLabelLens.Set(fullSearchAnnoFuzzyFieldExpr),
			fieldCollLens.Set(fullSearchCollAnno),
			fieldTargetBindLens.Set(fullSearchAnnoFuzzyTargets),
			fieldValueFilterLens.Set(annoFuzzyExcludedValueFilter),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(1),
			fieldLabelLens.Set(quotedFieldPhenotype),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindPhenotypeGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(2),
			fieldLabelLens.Set(quotedFieldEnvironment),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindEnvGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(3),
			fieldLabelLens.Set(quotedFieldAssay),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindAssayGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(4),
			fieldLabelLens.Set(quotedFieldCharacteristic),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindCharGraphID),
		),
	}

	// fullSearchTextFields are the 5 fields that get token (t0..t4) and
	// phrase (h0..h4) branches under text_en.
	fullSearchTextFields = []fullSearchField{
		F.Pipe5(
			fullSearchField{},
			fieldIndexLens.Set(0),
			fieldLabelLens.Set(quotedFieldPublicNote),
			fieldCollLens.Set(fullSearchCollAnno),
			fieldTargetBindLens.Set("@"+bindPubnoteTermID),
			fieldValueFilterLens.Set(annoPlaceholderValueFilter),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(1),
			fieldLabelLens.Set(quotedFieldPhenotype),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindPhenotypeGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(2),
			fieldLabelLens.Set(quotedFieldEnvironment),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindEnvGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(3),
			fieldLabelLens.Set(quotedFieldAssay),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindAssayGraphID),
		),
		F.Pipe4(
			fullSearchField{},
			fieldIndexLens.Set(4),
			fieldLabelLens.Set(quotedFieldCharacteristic),
			fieldCollLens.Set(fullSearchCollTerm),
			fieldTargetBindLens.Set(bindCharGraphID),
		),
	}

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
	fullSearchStateNamePrefixLens = F.Pipe1(
		fullSearchStageLens,
		L.Compose[fullSearchBranchState](stageNamePrefixLens),
	)
	fullSearchStateSearchExprLens = F.Pipe1(
		fullSearchStageLens,
		L.Compose[fullSearchBranchState](stageSearchExprLens),
	)
	fullSearchStateBandLens = F.Pipe1(
		fullSearchStageLens,
		L.Compose[fullSearchBranchState](stageBandLens),
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
	fullSearchStateValueFilterLens = F.Pipe1(
		fullSearchFieldLens,
		L.Compose[fullSearchBranchState](fieldValueFilterLens),
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
	isTermCollState = F.Pipe1(
		isTermCollField,
		P.ContraMap(fullSearchFieldLens.Get),
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

	// selectFullSearchBranchParts selects the 6-item annotation parts or the
	// 5-item cvterm parts.
	selectFullSearchBranchParts = F.Pipe1(
		isTermCollState,
		P.Fold(annoFullSearchBranchParts, termFullSearchBranchParts),
	)

	// Lenses on fullSearchStage.
	stageNamePrefixLens = L.MakeLens(
		func(s fullSearchStage) string { return s.namePrefix },
		func(s fullSearchStage, v string) fullSearchStage {
			s.namePrefix = v
			return s
		},
	)
	stageSearchExprLens = L.MakeLens(
		func(s fullSearchStage) func(fullSearchField) string { return s.searchExpr },
		func(s fullSearchStage, v func(fullSearchField) string) fullSearchStage {
			s.searchExpr = v
			return s
		},
	)
	stageBandLens = L.MakeLens(
		func(s fullSearchStage) string { return s.band },
		func(s fullSearchStage, v string) fullSearchStage {
			s.band = v
			return s
		},
	)

	// Stage definitions for the 4 full-search branch types.
	fullSearchPrefixStage = F.Pipe3(
		fullSearchStage{},
		stageNamePrefixLens.Set("p"),
		stageSearchExprLens.Set(prefixExprForField),
		stageBandLens.Set(fullSearchPrefixBand),
	)
	fullSearchFuzzyStage = F.Pipe3(
		fullSearchStage{},
		stageNamePrefixLens.Set("n"),
		stageSearchExprLens.Set(fuzzyExprForField),
		stageBandLens.Set(fullSearchFuzzyBand),
	)
	fullSearchTokenStage = F.Pipe3(
		fullSearchStage{},
		stageNamePrefixLens.Set("t"),
		stageSearchExprLens.Set(tokenExprForField),
		stageBandLens.Set(fullSearchTokenBand),
	)
	fullSearchPhraseStage = F.Pipe3(
		fullSearchStage{},
		stageNamePrefixLens.Set("h"),
		stageSearchExprLens.Set(phraseExprForField),
		stageBandLens.Set(fullSearchPhraseBand),
	)

	// tokenPhraseBranchBuilders are the token and phrase builders applied
	// across fullSearchTextFields via A.Ap.
	tokenPhraseBranchBuilders = []func(fullSearchField) queryBranch{
		F.Bind1st(buildFullSearchBranch, fullSearchTokenStage),
		F.Bind1st(buildFullSearchBranch, fullSearchPhraseStage),
	}

	// buildFullSearchQuery memoizes the assembled 21-branch AQL statement so
	// the query string is built once on first call and reused thereafter.
	buildFullSearchQuery = IO.Memoize(assembleFullSearchQuery)
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

// formatWithTemplate substitutes the slice items into tpl.
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
	name := fmt.Sprintf(
		"%s%d",
		fullSearchStateNamePrefixLens.Get(s),
		fullSearchStateIndexLens.Get(s),
	)

	return F.Pipe1(s, fullSearchStateNameLens.Set(name))
}

// fullSearchSearchExpr evaluates the stage SEARCH expression for st.field.
func fullSearchSearchExpr(st fullSearchBranchState) string {
	exprFn := fullSearchStateSearchExprLens.Get(st)
	field := fullSearchFieldLens.Get(st)

	return exprFn(field)
}

// annoFullSearchBranchParts builds the 6-item slice for FullSearchAnnoBranchQ in verb order.
func annoFullSearchBranchParts(st fullSearchBranchState) []any {
	return F.Pipe6(
		// Start with an empty []any slice for the fmt.Sprintf arguments.
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name (for example, "p0" or "t0").
		A.Push[any](fullSearchStateNameLens.Get(st)),
		// 2nd %s: append the AQL SEARCH expression on d.value.
		A.Push[any](fullSearchSearchExpr(st)),
		// 3rd %s: append the post-SEARCH value filter expression.
		A.Push[any](fullSearchStateValueFilterLens.Get(st)),
		// 4th %s: append the target cvterm _id list in e._to IN [...].
		A.Push[any](fullSearchStateTargetBindLens.Get(st)),
		// 5th %s: append the canonical field label expression returned in each row.
		A.Push[any](fullSearchStateLabelLens.Get(st)),
		// 6th %s: append the AQL score band expression.
		A.Push[any](fullSearchStateBandLens.Get(st)),
	)
}

// termFullSearchBranchParts builds the 5-item slice for FullSearchTermBranchQ in verb order.
func termFullSearchBranchParts(st fullSearchBranchState) []any {
	return F.Pipe5(
		// Start with an empty []any slice for the fmt.Sprintf arguments.
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name (for example, "p2" or "t1").
		A.Push[any](fullSearchStateNameLens.Get(st)),
		// 2nd %s: append the AQL SEARCH expression on d.label.
		A.Push[any](fullSearchSearchExpr(st)),
		// 3rd %s: append the bind parameter name of the required graph_id.
		A.Push[any](fullSearchStateTargetBindLens.Get(st)),
		// 4th %s: append the quoted canonical field label returned in each row.
		A.Push[any](fullSearchStateLabelLens.Get(st)),
		// 5th %s: append the AQL score band expression.
		A.Push[any](fullSearchStateBandLens.Get(st)),
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
		selectFullSearchBranchParts,
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

// appendFullSearchMergeBranch builds the merge block from the 21 branch names and appends it as a branch.
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

// assembleFullSearchQuery generates the complete 21-branch AQL statement.
func assembleFullSearchQuery() string {
	prefix := F.Pipe1(
		fullSearchPrefixFields,
		A.Map(F.Bind1st(buildFullSearchBranch, fullSearchPrefixStage)),
	)
	fuzzy := F.Pipe1(
		fullSearchFuzzyFields,
		A.Map(F.Bind1st(buildFullSearchBranch, fullSearchFuzzyStage)),
	)
	tokenPhrase := F.Pipe1(
		tokenPhraseBranchBuilders,
		A.Ap[queryBranch](fullSearchTextFields),
	)

	return F.Pipe5(
		A.From(prefix, fuzzy, tokenPhrase),
		A.Flatten[queryBranch],
		appendFullSearchMergeBranch,
		A.Map(bodyLens.Get),
		A.Intersperse("\n"),
		A.Fold(S.Monoid),
	)
}
