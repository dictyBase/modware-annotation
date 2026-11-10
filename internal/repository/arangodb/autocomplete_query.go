package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	IO "github.com/IBM/fp-go/v2/io"
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

	// branchBuildState carries the field specification and the branch under
	// construction through the branch builder pipeline.
	branchBuildState struct {
		spec   autocompleteFieldSpec
		branch queryBranch
	}
)

const (
	// annoPlaceholderValueFilter excludes the placeholder values stored on
	// ontology-backed annotations ("novalue" and "value").
	annoPlaceholderValueFilter = `d.value NOT IN ["novalue", "value"]`
	// annoFuzzyExcludedValueFilter excludes placeholder values and high-frequency
	// mutagenesis method and genetic modification boilerplate values from the
	// 2-gram fuzzy scan over d.value.
	annoFuzzyExcludedValueFilter = `d.value NOT IN [` +
		`"novalue", "value", ` +
		`"Restriction Enzyme-Mediated Integration", ` +
		`"endogenous insertion", "exogenous insertion", ` +
		`"Homologous Recombination", "exogenous mutation", ` +
		`"Random Insertion", "endogenous mutation", ` +
		`"Extrachromosomal"]`
	// autocompleteFuzzyBranchName is the AQL variable name of the combined
	// synonym and genotype fuzzy branch.
	autocompleteFuzzyBranchName = "n_value"
	// autocompleteFuzzyTargets is the target cvterm list of the combined
	// synonym and genotype fuzzy branch.
	autocompleteFuzzyTargets = "@synonym_term_id, @genotype_term_id"
	// autocompleteFuzzyFieldExpr maps the matched cvterm target back to its
	// canonical field label.
	autocompleteFuzzyFieldExpr = `e._to == @synonym_term_id ? "synonym" : "genotype"`
)

var (
	// tagLens, termBindLens, and isGenotypeLens focus the fields of a
	// searched tag specification.
	tagLens = L.MakeLens(
		func(s autocompleteFieldSpec) string { return s.tag },
		func(s autocompleteFieldSpec, v string) autocompleteFieldSpec {
			s.tag = v
			return s
		},
	)
	termBindLens = L.MakeLens(
		func(s autocompleteFieldSpec) string { return s.termBind },
		func(s autocompleteFieldSpec, v string) autocompleteFieldSpec {
			s.termBind = v
			return s
		},
	)
	isGenotypeLens = L.MakeLens(
		func(s autocompleteFieldSpec) bool { return s.isGenotype },
		func(s autocompleteFieldSpec, v bool) autocompleteFieldSpec {
			s.isGenotype = v
			return s
		},
	)

	// autocompleteFields are the two searched tags of the dicty_annotation
	// ontology, in the order their prefix branches appear in the statement.
	autocompleteFields = []autocompleteFieldSpec{
		F.Pipe3(
			autocompleteFieldSpec{},
			tagLens.Set(fieldSynonym),
			termBindLens.Set(bindSynonymTermID),
			isGenotypeLens.Set(false),
		),
		F.Pipe3(
			autocompleteFieldSpec{},
			tagLens.Set(fieldGenotype),
			termBindLens.Set(bindGenotypeTermID),
			isGenotypeLens.Set(true),
		),
	}

	// prefixSearchExprForSpec is the whole-value prefix expression, or the
	// whole-value and allele-token pair a genotype value needs.
	prefixSearchExprForSpec = F.Pipe1(
		isGenotypeLens.Get,
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

	// specLens and branchLens focus the two parts of a branch build state,
	// and the composed state* lenses focus the nested fields directly.
	specLens = L.MakeLens(
		func(s branchBuildState) autocompleteFieldSpec { return s.spec },
		func(s branchBuildState, v autocompleteFieldSpec) branchBuildState {
			s.spec = v
			return s
		},
	)
	branchLens = L.MakeLens(
		func(s branchBuildState) queryBranch { return s.branch },
		func(s branchBuildState, v queryBranch) branchBuildState {
			s.branch = v
			return s
		},
	)
	stateTagLens      = F.Pipe1(specLens, L.Compose[branchBuildState](tagLens))
	stateTermBindLens = F.Pipe1(specLens, L.Compose[branchBuildState](termBindLens))
	stateNameLens     = F.Pipe1(branchLens, L.Compose[branchBuildState](nameLens))
	stateBodyLens     = F.Pipe1(branchLens, L.Compose[branchBuildState](bodyLens))

	// buildAutocompleteQuery memoizes the assembled 3-branch AQL statement so
	// the query string is built once on first call and reused thereafter.
	buildAutocompleteQuery = IO.Memoize(assembleAutocompleteQuery)
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

// setPrefixName writes the prefix LET variable name into s.branch.name.
func setPrefixName(s branchBuildState) branchBuildState {
	name := strutils.JoinStrings("p_", stateTagLens.Get(s))

	return F.Pipe1(s, stateNameLens.Set(name))
}

// prefixBranchParts builds the 6-item slice for AutocompleteBranchQ in verb order.
func prefixBranchParts(st branchBuildState) []any {
	searchExpr := F.Pipe2(st, specLens.Get, prefixSearchExprForSpec)
	termBind := strutils.JoinStrings("@", stateTermBindLens.Get(st))
	quotedTag := strutils.JoinStrings(`"`, stateTagLens.Get(st), `"`)

	return F.Pipe6(
		// Start with an empty []any slice for the fmt.Sprintf arguments.
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name (for example, "p_synonym").
		A.Push[any](stateNameLens.Get(st)),
		// 2nd %s: append the AQL SEARCH expression.
		A.Push[any](searchExpr),
		// 3rd %s: append the post-SEARCH value filter expression.
		A.Push[any](annoPlaceholderValueFilter),
		// 4th %s: append the cvterm ID bind parameter reference (for example, "@synonym_term_id").
		A.Push[any](termBind),
		// 5th %s: append the quoted field label returned in each row.
		A.Push[any](quotedTag),
		// 6th %s: append the AQL prefix score expression ("1000 + BM25(d)").
		A.Push[any](autocompletePrefixScore),
	)
}

// formatBranchBody substitutes the 6 slice items into the AutocompleteBranchQ template.
func formatBranchBody(parts []any) string {
	return fmt.Sprintf(AutocompleteBranchQ, parts...)
}

// setBranchBody flips stateBodyLens.Set so it accepts the rendered body string last.
func setBranchBody(s branchBuildState) func(string) branchBuildState {
	return F.Pipe1(s, F.Flip(stateBodyLens.Set))
}

// setPrefixBody builds the AQL branch body from s and stores it in s.branch.body.
func setPrefixBody(s branchBuildState) branchBuildState {
	return F.Pipe3(
		s,
		prefixBranchParts,
		formatBranchBody,
		setBranchBody(s),
	)
}

// prefixBranch builds the prefix branch of one field.
func prefixBranch(spec autocompleteFieldSpec) queryBranch {
	return F.Pipe4(
		branchBuildState{},
		specLens.Set(spec),
		setPrefixName,
		setPrefixBody,
		branchLens.Get,
	)
}

// assembleAutocompleteQuery builds the complete 3-branch autocomplete statement.
func assembleAutocompleteQuery() string {
	fuzzyParts := F.Pipe6(
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name ("n_value").
		A.Push[any](autocompleteFuzzyBranchName),
		// 2nd %s: append the AQL NGRAM_MATCH expression on the value attribute.
		A.Push[any](autocompleteFuzzyExpr(docFieldValue)),
		// 3rd %s: append the post-SEARCH value filter expression.
		A.Push[any](annoFuzzyExcludedValueFilter),
		// 4th %s: append the combined cvterm ID bind parameters.
		A.Push[any](autocompleteFuzzyTargets),
		// 5th %s: append the field label expression.
		A.Push[any](autocompleteFuzzyFieldExpr),
		// 6th %s: append the AQL fuzzy score expression ("BM25(d)").
		A.Push[any](autocompleteFuzzyScore),
	)
	fuzzyBody := F.Pipe1(fuzzyParts, formatBranchBody)
	fuzzy := F.Pipe2(
		queryBranch{},
		nameLens.Set(autocompleteFuzzyBranchName),
		bodyLens.Set(fuzzyBody),
	)

	return F.Pipe6(
		autocompleteFields,
		A.Map(prefixBranch),
		A.Push(fuzzy),
		appendMergeBranch,
		A.Map(bodyLens.Get),
		A.Intersperse("\n"),
		A.Fold(S.Monoid),
	)
}

// appendMergeBranch builds the merge block from the branch names and appends it as a branch.
func appendMergeBranch(branches []queryBranch) []queryBranch {
	setBody := F.Pipe1(queryBranch{}, F.Flip(bodyLens.Set))

	return F.Pipe6(
		branches,
		A.Map(nameLens.Get),
		A.Intersperse(", "),
		A.Fold(S.Monoid),
		S.Format[string](AutocompleteMergeQ),
		setBody,
		F.Bind1st(A.Append, branches),
	)
}
