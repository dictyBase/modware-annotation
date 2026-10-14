package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	monoid "github.com/IBM/fp-go/v2/monoid"
	L "github.com/IBM/fp-go/v2/optics/lens"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
	"github.com/dictyBase/fp-go-loom/strutils"
)

type (
	// autocompleteFieldSpec describes one searched tag: its label, the bind
	// parameter name of its cvterm identifier, and whether the value is a
	// comma-separated genotype that needs the delimiter analyzer.
	autocompleteFieldSpec struct {
		tag        string
		termBind   string
		isGenotype bool
	}

	// queryBranch is one built LET branch of the autocomplete statement.
	queryBranch struct {
		name string
		body string
	}

	// branchSet pairs the fields a stage covers with the builder that turns
	// one field into one branch.
	branchSet struct {
		fields  []autocompleteFieldSpec
		builder func(autocompleteFieldSpec) queryBranch
	}
)

var (
	// autocompleteFields are the two searched tags of the dicty_annotation
	// ontology, in the order their branches appear in the statement.
	autocompleteFields = []autocompleteFieldSpec{
		{tag: fieldSynonym, termBind: bindSynonymTermID, isGenotype: false},
		{tag: fieldGenotype, termBind: bindGenotypeTermID, isGenotype: true},
	}

	// isGenotypeSpec selects the value expression a field needs.
	isGenotypeSpec = func(spec autocompleteFieldSpec) bool {
		return spec.isGenotype
	}

	// prefixSearchExprForSpec is the whole-value prefix expression, or the
	// whole-value and allele-token pair a genotype value needs.
	prefixSearchExprForSpec = F.Pipe1(
		isGenotypeSpec,
		P.Fold(synonymPrefixExpr, genotypePrefixExpr),
	)

	// nameLens and bodyLens focus the two parts of a built branch.
	nameLens = L.MakeLens(
		func(b queryBranch) string { return b.name },
		func(b queryBranch, v string) queryBranch {
			b.name = v
			return b
		},
	)
	bodyLens = L.MakeLens(
		func(b queryBranch) string { return b.body },
		func(b queryBranch, v string) queryBranch {
			b.body = v
			return b
		},
	)

	// branchSets are the two stages of the statement: the prefix stage and
	// the fuzzy stage, each covering both fields.
	branchSets = []branchSet{
		{fields: autocompleteFields, builder: prefixBranch},
		{fields: autocompleteFields, builder: fuzzyBranch},
	}
)

// synonymPrefixExpr is the whole-value prefix expression of a synonym branch.
func synonymPrefixExpr(autocompleteFieldSpec) string {
	return fmt.Sprintf(
		"ANALYZER(STARTS_WITH(d.%s, @q), %q)",
		docFieldValue,
		autocompleteNormAnalyzer,
	)
}

// genotypePrefixExpr adds the allele-token prefix expression that the
// delimiter analyzer needs for an interior allele of a comma-separated value.
func genotypePrefixExpr(autocompleteFieldSpec) string {
	return fmt.Sprintf(
		"(ANALYZER(STARTS_WITH(d.%s, @q), %q) OR ANALYZER(STARTS_WITH(d.%s, @q), %q))",
		docFieldValue,
		autocompleteNormAnalyzer,
		docFieldValue,
		autocompleteDelimAnalyzer,
	)
}

// autocompleteFuzzyExpr is the n-gram similarity expression of a fuzzy branch.
func autocompleteFuzzyExpr(field string) string {
	return fmt.Sprintf(
		"NGRAM_MATCH(d.%s, @q, @th, %q)",
		field,
		autocompleteNgramAnalyzer,
	)
}

// prefixBranch builds the prefix branch of one field.
func prefixBranch(spec autocompleteFieldSpec) queryBranch {
	name := strutils.JoinStrings("p_", spec.tag)
	parts := A.From[any](
		name,
		prefixSearchExprForSpec(spec),
		spec.termBind,
		spec.tag,
		autocompletePrefixScore,
	)

	return queryBranch{name: name, body: fmt.Sprintf(AutocompleteBranchQ, parts...)}
}

// fuzzyBranch builds the n-gram branch of one field.
func fuzzyBranch(spec autocompleteFieldSpec) queryBranch {
	name := strutils.JoinStrings("n_", spec.tag)
	parts := A.From[any](
		name,
		autocompleteFuzzyExpr(docFieldValue),
		spec.termBind,
		spec.tag,
		autocompleteFuzzyScore,
	)

	return queryBranch{name: name, body: fmt.Sprintf(AutocompleteBranchQ, parts...)}
}

// branchesFromSet builds every branch of one stage.
func branchesFromSet(set branchSet) []queryBranch {
	return F.Pipe1(set.fields, A.Map(set.builder))
}

// autocompleteBranches builds all four branches in statement order.
func autocompleteBranches() []queryBranch {
	return F.Pipe1(branchSets, A.Chain(branchesFromSet))
}

// mergeBlock builds the merge block that deduplicates the branches by
// entry_id and returns the top rows.
func mergeBlock(branches []queryBranch) string {
	names := F.Pipe2(
		branches,
		A.Map(nameLens.Get),
		joinAutocompleteParts(", "),
	)
	format := S.Format[string](AutocompleteMergeQ)

	return format(names)
}

// joinAutocompleteParts joins strings with sep through the fp-go string
// monoid, so query assembly stays point-free.
func joinAutocompleteParts(sep string) func([]string) string {
	return monoid.Fold(S.IntersperseMonoid(sep))
}

// buildAutocompleteQuery builds the complete 4-branch autocomplete statement.
func buildAutocompleteQuery() string {
	branches := autocompleteBranches()
	merge := mergeBlock(branches)

	return F.Pipe3(
		branches,
		A.Map(bodyLens.Get),
		A.Push(merge),
		joinAutocompleteParts("\n"),
	)
}
