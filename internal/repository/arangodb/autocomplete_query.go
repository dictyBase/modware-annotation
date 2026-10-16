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

var (
	// autocompleteFields are the two searched tags of the dicty_annotation
	// ontology, in the order their branches appear in the statement.
	autocompleteFields = []autocompleteFieldSpec{
		{tag: fieldSynonym, termBind: bindSynonymTermID, isGenotype: false},
		{tag: fieldGenotype, termBind: bindGenotypeTermID, isGenotype: true},
	}

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

	// autocompleteBranchBuilders are the two stage builders of the statement:
	// the prefix stage and the fuzzy stage, each applied across autocompleteFields.
	autocompleteBranchBuilders = []func(autocompleteFieldSpec) queryBranch{
		prefixBranch,
		fuzzyBranch,
	}

	// buildAutocompleteQuery memoizes the assembled 4-branch AQL statement so
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

// prefixBranchParts builds the 5-item slice for AutocompleteBranchQ in verb order.
func prefixBranchParts(st branchBuildState) []any {
	return F.Pipe5(
		// Start with an empty []any slice for the fmt.Sprintf arguments.
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name (for example, "p_synonym").
		A.Push[any](stateNameLens.Get(st)),
		// 2nd %s: read st.spec, select its AQL SEARCH expression, and append it.
		A.Push[any](F.Pipe2(st, specLens.Get, prefixSearchExprForSpec)),
		// 3rd %s: append the cvterm ID bind parameter name (for example, "synonym_term_id").
		A.Push[any](stateTermBindLens.Get(st)),
		// 4th %q: append the field label returned in each row (for example, "synonym").
		A.Push[any](stateTagLens.Get(st)),
		// 5th %s: append the AQL prefix score expression ("1000 + BM25(d)").
		A.Push[any](autocompletePrefixScore),
	)
}

// formatBranchBody substitutes the 5 slice items into the AutocompleteBranchQ template.
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

// setFuzzyName writes the fuzzy LET variable name into s.branch.name.
func setFuzzyName(s branchBuildState) branchBuildState {
	name := strutils.JoinStrings("n_", stateTagLens.Get(s))

	return F.Pipe1(s, stateNameLens.Set(name))
}

// fuzzyBranchParts builds the 5-item slice for a fuzzy AutocompleteBranchQ in verb order.
func fuzzyBranchParts(st branchBuildState) []any {
	return F.Pipe5(
		// Start with an empty []any slice for the fmt.Sprintf arguments.
		A.Empty[any](),
		// 1st %s: append the AQL LET variable name (for example, "n_synonym").
		A.Push[any](stateNameLens.Get(st)),
		// 2nd %s: append the AQL NGRAM_MATCH expression on the value attribute.
		A.Push[any](autocompleteFuzzyExpr(docFieldValue)),
		// 3rd %s: append the cvterm ID bind parameter name (for example, "synonym_term_id").
		A.Push[any](stateTermBindLens.Get(st)),
		// 4th %q: append the field label returned in each row (for example, "synonym").
		A.Push[any](stateTagLens.Get(st)),
		// 5th %s: append the AQL fuzzy score expression ("BM25(d)").
		A.Push[any](autocompleteFuzzyScore),
	)
}

// setFuzzyBody builds the fuzzy AQL branch body from s and stores it in s.branch.body.
func setFuzzyBody(s branchBuildState) branchBuildState {
	return F.Pipe3(
		s,
		fuzzyBranchParts,
		formatBranchBody,
		setBranchBody(s),
	)
}

// fuzzyBranch builds the n-gram branch of one field.
func fuzzyBranch(spec autocompleteFieldSpec) queryBranch {
	return F.Pipe4(
		branchBuildState{},
		specLens.Set(spec),
		setFuzzyName,
		setFuzzyBody,
		branchLens.Get,
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

// assembleAutocompleteQuery builds the complete 4-branch autocomplete statement.
func assembleAutocompleteQuery() string {
	return F.Pipe5(
		autocompleteBranchBuilders,
		A.Ap[queryBranch](autocompleteFields),
		appendMergeBranch,
		A.Map(bodyLens.Get),
		A.Intersperse("\n"),
		A.Fold(S.Monoid),
	)
}
