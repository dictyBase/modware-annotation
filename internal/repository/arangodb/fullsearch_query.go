package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	L "github.com/IBM/fp-go/v2/optics/lens"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
)

type (
	fullSearchColl int

	// fullSearchField describes one searched annotation or cvterm field.
	fullSearchField struct {
		label      string
		coll       fullSearchColl
		targetBind string
		isGenotype bool
	}

	// fullSearchBranchSpec holds all parameters of one AQL LET branch.
	fullSearchBranchSpec struct {
		name     string
		field    fullSearchField
		search   string
		band     string
		template string
	}

	// fullSearchSpecSet pairs a slice of fields with a scalar spec builder.
	fullSearchSpecSet struct {
		fields  []fullSearchField
		builder func(int, fullSearchField) fullSearchBranchSpec
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
		{label: fieldSynonym, coll: fullSearchCollAnno, targetBind: bindSynonymTermID},
		{label: fieldGenotype, coll: fullSearchCollAnno, targetBind: bindGenotypeTermID, isGenotype: true},
		{label: fieldSystematicName, coll: fullSearchCollAnno, targetBind: bindSysnameTermID},
		{label: fieldGeneticModification, coll: fullSearchCollAnno, targetBind: bindMuttypeTermID},
		{label: fieldMutagenesisMethod, coll: fullSearchCollAnno, targetBind: bindMutmethodTermID},
		{label: fieldPhenotype, coll: fullSearchCollTerm, targetBind: bindPhenotypeGraphID},
		{label: fieldEnvironment, coll: fullSearchCollTerm, targetBind: bindEnvGraphID},
		{label: fieldAssay, coll: fullSearchCollTerm, targetBind: bindAssayGraphID},
		{label: fieldCharacteristic, coll: fullSearchCollTerm, targetBind: bindCharGraphID},
	}

	// fullSearchTextFields are the 5 fields that get token (t0..t4) and
	// phrase (h0..h4) branches under text_en.
	fullSearchTextFields = []fullSearchField{
		{label: fieldPublicNote, coll: fullSearchCollAnno, targetBind: bindPubnoteTermID},
		{label: fieldPhenotype, coll: fullSearchCollTerm, targetBind: bindPhenotypeGraphID},
		{label: fieldEnvironment, coll: fullSearchCollTerm, targetBind: bindEnvGraphID},
		{label: fieldAssay, coll: fullSearchCollTerm, targetBind: bindAssayGraphID},
		{label: fieldCharacteristic, coll: fullSearchCollTerm, targetBind: bindCharGraphID},
	}

	specTemplateLens = L.MakeLens(
		func(spec fullSearchBranchSpec) string { return spec.template },
		func(spec fullSearchBranchSpec, tpl string) fullSearchBranchSpec {
			spec.template = tpl
			return spec
		},
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
		isGenotypeFullSearchField,
		P.Fold(standardFullSearchPrefixExpr, genotypeFullSearchPrefixExpr),
	)

	// templateForField selects FullSearchAnnoBranchQ or FullSearchTermBranchQ.
	templateForField = F.Pipe1(
		isTermCollField,
		P.Fold(annoBranchTemplate, termBranchTemplate),
	)

	// fullSearchSpecSets lists the 4 scalar spec groups in statement order:
	// 9 prefix, 9 fuzzy, 5 token, and 5 phrase branches (28 total).
	fullSearchSpecSets = []fullSearchSpecSet{
		{fields: fullSearchPrefixFuzzyFields, builder: fullSearchPrefixSpec},
		{fields: fullSearchPrefixFuzzyFields, builder: fullSearchFuzzySpec},
		{fields: fullSearchTextFields, builder: fullSearchTokenSpec},
		{fields: fullSearchTextFields, builder: fullSearchPhraseSpec},
	}
)

// isTermCollField reports whether a field lives on the cvterm collection.
func isTermCollField(f fullSearchField) bool {
	return f.coll == fullSearchCollTerm
}

// isGenotypeFullSearchField reports whether a field needs the comma+space
// delimiter analyzer in addition to the whole-value norm analyzer.
func isGenotypeFullSearchField(f fullSearchField) bool {
	return f.isGenotype
}

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

// withBranchTemplate resolves the AQL branch template of spec.field onto spec.
func withBranchTemplate(spec fullSearchBranchSpec) fullSearchBranchSpec {
	tpl := templateForField(spec.field)

	return F.Pipe1(spec, specTemplateLens.Set(tpl))
}

// renderFullSearchBranch renders one branch spec into a queryBranch.
func renderFullSearchBranch(spec fullSearchBranchSpec) queryBranch {
	parts := A.From[any](
		spec.name,
		spec.search,
		spec.field.targetBind,
		spec.field.label,
		spec.band,
	)

	return queryBranch{
		name: spec.name,
		body: fmt.Sprintf(spec.template, parts...),
	}
}

// fullSearchBranch resolves the branch template and renders the AQL LET block.
func fullSearchBranch(spec fullSearchBranchSpec) queryBranch {
	return F.Pipe2(
		spec,
		withBranchTemplate,
		renderFullSearchBranch,
	)
}

// fullSearchPrefixSpec builds the prefix branch spec p<i> for field f.
func fullSearchPrefixSpec(i int, f fullSearchField) fullSearchBranchSpec {
	return fullSearchBranchSpec{
		name:   fmt.Sprintf("p%d", i),
		field:  f,
		search: prefixExprForField(f),
		band:   fullSearchPrefixBand,
	}
}

// fullSearchFuzzySpec builds the fuzzy branch spec n<i> for field f.
func fullSearchFuzzySpec(i int, f fullSearchField) fullSearchBranchSpec {
	return fullSearchBranchSpec{
		name:   fmt.Sprintf("n%d", i),
		field:  f,
		search: fuzzyExprForField(f),
		band:   fullSearchFuzzyBand,
	}
}

// fullSearchTokenSpec builds the token branch spec t<i> for field f.
func fullSearchTokenSpec(i int, f fullSearchField) fullSearchBranchSpec {
	return fullSearchBranchSpec{
		name:   fmt.Sprintf("t%d", i),
		field:  f,
		search: tokenExprForField(f),
		band:   fullSearchTokenBand,
	}
}

// fullSearchPhraseSpec builds the phrase branch spec h<i> for field f.
func fullSearchPhraseSpec(i int, f fullSearchField) fullSearchBranchSpec {
	return fullSearchBranchSpec{
		name:   fmt.Sprintf("h%d", i),
		field:  f,
		search: phraseExprForField(f),
		band:   fullSearchPhraseBand,
	}
}

// specsFromSet expands one spec set into its branch specs in statement order.
func specsFromSet(set fullSearchSpecSet) []fullSearchBranchSpec {
	return F.Pipe1(
		set.fields,
		A.MapWithIndex(set.builder),
	)
}

// fullSearchMergeBlock builds the trailing FLATTEN/COLLECT merge block.
func fullSearchMergeBlock(branches []queryBranch) string {
	return F.Pipe3(
		branches,
		A.Map(nameLens.Get),
		S.Join(", "),
		S.Format[string](FullSearchMergeQ),
	)
}

// appendFullSearchMerge extracts the rendered LET bodies of the branches and
// appends the trailing merge block.
func appendFullSearchMerge(branches []queryBranch) []string {
	merge := fullSearchMergeBlock(branches)

	return F.Pipe2(
		branches,
		A.Map(bodyLens.Get),
		A.Push(merge),
	)
}

// buildFullSearchQuery generates the complete 28-branch AQL statement.
func buildFullSearchQuery() string {
	return F.Pipe4(
		fullSearchSpecSets,
		A.Chain(specsFromSet),
		A.Map(fullSearchBranch),
		appendFullSearchMerge,
		S.Join("\n"),
	)
}
