# Phenotype Ontology Search Implementation Plan

## Table of Contents

- [1. Intent and Scope](#1-intent-and-scope)
- [2. Protobuf Contract (`dictybaseapis`)](#2-protobuf-contract-dictybaseapis)
- [3. Repository Boundary (`internal/repository/repository.go`)](#3-repository-boundary-internalrepositoryrepositorygo)
- [4. ArangoSearch View Links (`internal/repository/arangodb/fullsearch.go` and `autocomplete.go`)](#4-arangosearch-view-links-internalrepositoryarangodbfullsearchgo-and-autocompletego)
- [5. ArangoDB AQL Templates (`internal/repository/arangodb/phenotype_search_statement.go`)](#5-arangodb-aql-templates-internalrepositoryarangodbphenotype_search_statementgo)
- [6. ArangoDB AQL Builder (`internal/repository/arangodb/phenotype_search_query.go`)](#6-arangodb-aql-builder-internalrepositoryarangodbphenotype_search_querygo)
- [7. ArangoDB Repository Pipeline (`internal/repository/arangodb/phenotype_search.go`)](#7-arangodb-repository-pipeline-internalrepositoryarangodbphenotype_searchgo)
- [8. Service Layer Pipeline (`internal/app/service/phenotype_search.go`)](#8-service-layer-pipeline-internalappservicephenotype_searchgo)
- [9. Test Suite](#9-test-suite)

## 1. Intent and Scope

This document defines the complete implementation for `SearchPhenotypes` in `modware-annotation` using `fp-go` v2 (`Option`, `Either`, `IO`, `IOEither`, and lenses) and `fp-go-loom`.

- **Three Phenotype Ontologies:** The search queries `cvterm` documents across all three phenotype ontologies used by stock search:
  1. `Dicty Phenotypes` (`phenotypeOntology`)
  2. `Dicty Environment` (`environmentOntology`)
  3. `Dictyostelium Assay` (`assayOntology`)
- **Four Ontology Text and Prose Fields:** The search queries four fields on the `cvterm` document (`dbTerm` in `go-obograph/storage/arangodb/schema.go`):
  1. `d.label`: term name (prefix, fuzzy, token, phrase)
  2. `d.metadata.synonyms.value`: alternate term names (prefix, fuzzy, token, phrase)
  3. `d.metadata.definition.value`: term definition prose (token, phrase)
  4. `d.metadata.comments`: term comment prose (token, phrase)
- **Twelve Branches and Four Score Bands:** The AQL builder assembles 12 `LET` branches (`p0..p1`, `n0..n1`, `t0..t3`, `h0..h3`) across the four additive score bands (`1000 + BM25(d)`, `500 + BM25(d)`, `250 + BM25(d)`, `BM25(d)`). Each branch traverses `1..1 INBOUND d GRAPH @anno_cvterm_graph` to active strain annotations (`ann.is_obsolete == false` and `DBS...` `entry_id`). The merge block deduplicates matches by `ann.entry_id` (`COLLECT eid = x.id`), keeps the highest-scoring branch hit per strain, and projects `entry_id` (`ann.entry_id`), `field` (`label`, `synonym`, `definition`, or `comment`), `name` (`d.label`), `definition` (`d.metadata.definition.value`), and `score`.

## 2. Protobuf Contract (`dictybaseapis`)

The protobuf contract lives on the `feat/phenotype-search` branch of `dictybaseapis` (`dictybase/annotation/annotation.proto`):

```protobuf
// SearchPhenotypes returns a ranked list of strain phenotype matches for a
// search text, deduplicated by entry_id. The default list length is 50.
rpc SearchPhenotypes(PhenotypeSearchParameters) returns (PhenotypeSearchResultCollection) {}

// PhenotypeSearchField names the cvterm field that produced a phenotype
// search match.
enum PhenotypeSearchField {
  PHENOTYPE_SEARCH_FIELD_UNSPECIFIED = 0;
  // label of an active cvterm in a phenotype ontology.
  PHENOTYPE_SEARCH_FIELD_LABEL = 1;
  // metadata.synonyms.value of an active cvterm in a phenotype ontology.
  PHENOTYPE_SEARCH_FIELD_SYNONYM = 2;
  // metadata.definition.value of an active cvterm in a phenotype ontology.
  PHENOTYPE_SEARCH_FIELD_DEFINITION = 3;
  // metadata.comments of an active cvterm in a phenotype ontology.
  PHENOTYPE_SEARCH_FIELD_COMMENT = 4;
}

// PhenotypeSearchParameters is the input of a SearchPhenotypes call.
message PhenotypeSearchParameters {
  message Data {
    // Resource name. By default it is phenotype.
    string type = 1;
    PhenotypeSearchAttributes attributes = 2 [(buf.validate.field).required = true];
  }
  Data data = 1 [(buf.validate.field).required = true];
}

// PhenotypeSearchAttributes holds the search text of a phenotype search
// request. The match runs against active strain (DBS...) entries over
// cvterm label, synonyms, definition, and comments fields across the
// Dicty Phenotypes, Dicty Environment, and Dictyostelium Assay ontologies,
// deduplicated by entry_id.
message PhenotypeSearchAttributes {
  // Search text. The minimum length is 2 characters.
  string query = 1 [(buf.validate.field).string.min_len = 2];
  // Maximum number of results. The server returns 50 when this field is
  // absent or zero, and it never returns more than 50.
  int64 limit = 2 [(buf.validate.field).int64 = {
    gte: 0
    lte: 50
  }];
}

// PhenotypeSearchResult is one phenotype full search match, deduplicated by
// entry_id.
message PhenotypeSearchResult {
  // entry_id of the matched strain, for example DBS0236126.
  string entry_id = 1;
  // Field that produced the match.
  PhenotypeSearchField field = 2;
  // Name of the matched phenotype ontology term (cvterm label).
  string name = 3;
  // Definition of the matched phenotype ontology term.
  string definition = 4;
  // Rank score.
  double score = 5;
}

// PhenotypeSearchResultCollection returns the results of a phenotype search
// request. An empty data list is valid, because a query can match no
// term.
message PhenotypeSearchResultCollection {
  repeated PhenotypeSearchResult data = 1;
  // next_cursor is always 0, limit is the effective limit of the
  // request, and total is the number of rows in data.
  Meta meta = 2;
}
```

## 3. Repository Boundary (`internal/repository/repository.go`)

Add `PhenotypeSearchQuery`, `PhenotypeSearchResult`, and `SearchPhenotypes` to `internal/repository/repository.go`:

```go
// PhenotypeSearchQuery holds the input of a phenotype ontology full search.
// The repository normalizes Query, rejects all-stopword queries, and clamps Limit.
type PhenotypeSearchQuery struct {
	// Query is the search text. The caller sends it untrimmed;
	// the repository trims it, lowercases it, and strips combining marks.
	Query string
	// Limit is the maximum number of results. A value at or below 0
	// becomes 50. A value above 50 becomes 50.
	Limit int
}

// PhenotypeSearchResult is one phenotype full search match, deduplicated by
// EntryID.
type PhenotypeSearchResult struct {
	// EntryID is the strain identifier of the matched annotation, for
	// example DBS0236126.
	EntryID string
	// Field is the canonical cvterm field label that matched ("label",
	// "synonym", "definition", or "comment").
	Field string
	// Name is the matched cvterm label.
	Name string
	// Definition is the stored cvterm definition prose.
	Definition string
	// Score ranks the match across the four bands (prefix >= 1000,
	// phrase >= 500, token >= 250, fuzzy >= 0).
	Score float64
}

type TaggedAnnotationRepository interface {
	// ... existing methods
	// SearchPhenotypes returns at most Limit ranked strain phenotype matches
	// for the normalized query of params as a lazy IOEither pipeline.
	SearchPhenotypes(
		ctx context.Context,
		params *PhenotypeSearchQuery,
	) IOE.IOEither[error, []*PhenotypeSearchResult]
}
```

## 4. ArangoSearch View Links (`internal/repository/arangodb/fullsearch.go` and `autocomplete.go`)

Update `internal/repository/arangodb/fullsearch.go` to index `metadata.definition.value`, `metadata.synonyms.value`, and `metadata.comments` on the `cvterm` link of `annotation_full_search`:

```go
const (
	// Document attributes on the cvterm collection.
	docFieldLabel      = "label"
	docFieldGraphID    = "graph_id"
	docFieldDeprecated = "deprecated"
	docFieldMetadata   = "metadata"
	docFieldDefinition = "definition"
	docFieldSynonyms   = "synonyms"
	docFieldComments   = "comments"
)
```

Update `fullSearchViewProperties` in `internal/repository/arangodb/fullsearch.go`:

```go
func fullSearchViewProperties(
	annotColl, cvtermColl string,
) driver.ArangoSearchViewProperties {
	annotFields := driver.ArangoSearchFields{
		docFieldValue: {
			Analyzers: []string{
				fullSearchNormAnalyzer,
				fullSearchDelimAnalyzer,
				fullSearchNgramAnalyzer,
				fullSearchTextAnalyzer,
			},
		},
		docFieldEntryID:    {Analyzers: []string{fullSearchNormAnalyzer}},
		docFieldIsObsolete: {},
	}
	cvtermMetadataFields := driver.ArangoSearchFields{
		docFieldDefinition: {
			Fields: driver.ArangoSearchFields{
				docFieldValue: {
					Analyzers: []string{fullSearchTextAnalyzer},
				},
			},
		},
		docFieldSynonyms: {
			Fields: driver.ArangoSearchFields{
				docFieldValue: {
					Analyzers: []string{
						fullSearchNormAnalyzer,
						fullSearchNgramAnalyzer,
						fullSearchTextAnalyzer,
					},
				},
			},
		},
		docFieldComments: {
			Analyzers: []string{fullSearchTextAnalyzer},
		},
	}
	cvtermFields := driver.ArangoSearchFields{
		docFieldLabel: {
			Analyzers: []string{
				fullSearchNormAnalyzer,
				fullSearchNgramAnalyzer,
				fullSearchTextAnalyzer,
			},
		},
		docFieldMetadata:   {Fields: cvtermMetadataFields},
		docFieldGraphID:    {},
		docFieldDeprecated: {},
	}

	return driver.ArangoSearchViewProperties{
		Links: driver.ArangoSearchLinks{
			annotColl: {
				Analyzers: []string{fullSearchIdentityAnalyzer},
				Fields:    annotFields,
			},
			cvtermColl: {
				Analyzers: []string{fullSearchIdentityAnalyzer},
				Fields:    cvtermFields,
			},
		},
	}
}
```

Update `sameFieldShape` in `internal/repository/arangodb/autocomplete.go` to use `EQ.ContraMap`, `EQ.Semigroup`, and `R.Eq` so view reconciliation compares `Analyzers` and nested `Fields` functionally without imperative loops or branches:

```go
var (
	strOrd        = ORD.FromStrictCompare[string]()
	sortAnalyzers = A.Sort(strOrd)
	stringSliceEq = A.Eq(S.Eq)

	// analyzersEq compares two elements by their analyzer lists. It sorts
	// both lists first, so the analyzer order does not matter.
	analyzersEq = F.Pipe2(
		stringSliceEq,
		EQ.ContraMap(sortAnalyzers),
		EQ.ContraMap(elementAnalyzers),
	)
)

func elementAnalyzers(
	el driver.ArangoSearchElementProperties,
) []string {
	return el.Analyzers
}

func elementFields(
	el driver.ArangoSearchElementProperties,
) driver.ArangoSearchFields {
	return el.Fields
}

// sameFieldShape reports whether two field maps carry the same analyzers and
// nested field definitions. Two elements match when the analyzers match AND
// the nested fields match. The nested check recurses through sameFieldShape.
func sameFieldShape(
	a, b driver.ArangoSearchFields,
) bool {
	nestedFieldsEq := F.Pipe1(
		EQ.FromEquals(sameFieldShape),
		EQ.ContraMap(elementFields),
	)
	// bothMatch combines two element Eqs with logical AND.
	bothMatch := EQ.Monoid[driver.ArangoSearchElementProperties]().Concat
	elementEq := bothMatch(analyzersEq, nestedFieldsEq)

	return R.Eq[string](elementEq).Equals(a, b)
}
```

## 5. ArangoDB AQL Templates (`internal/repository/arangodb/phenotype_search_statement.go`)

Create `internal/repository/arangodb/phenotype_search_statement.go`:

```go
package arangodb

const (
	// phenotypeSearchTargetsQ resolves the 3 phenotype ontology cv._id values
	// in one round trip. Missing ontologies are absent from the returned map
	// and default to "" in Go.
	phenotypeSearchTargetsQ = `LET graph_rows = (
  FOR cv IN @@cv_collection
    FILTER cv.metadata.namespace IN @term_ontologies
    RETURN {
      k: cv.metadata.namespace,
      v: cv._id
    }
)
RETURN {
  graphs: ZIP(graph_rows[*].k, graph_rows[*].v)
}`

	// PhenotypeSearchBranchQ is one branch of the phenotype search statement
	// over the cvterm collection with an inbound traversal to active strain
	// annotations. The builder fills it with fmt.Sprintf in this 4-verb order:
	//
	//  1. %s — branch variable (p0..p1, n0..n1, t0..t3, h0..h3)
	//  2. %s — SEARCH expression on d.label, d.metadata.synonyms.value,
	//          d.metadata.definition.value, or d.metadata.comments
	//  3. %q — canonical field label reported in each row ("label",
	//          "synonym", "definition", or "comment")
	//  4. %s — score band expression
	PhenotypeSearchBranchQ = `LET %s = (
  FOR d IN annotation_full_search
    SEARCH d.deprecated == false
      AND %s
      AND d.graph_id IN [@phenotype_graph_id, @env_graph_id, @assay_graph_id]
    FOR ann IN 1..1 INBOUND d GRAPH @anno_cvterm_graph
      FILTER ann.is_obsolete == false
        AND STARTS_WITH(LOWER(ann.entry_id), "dbs")
      SORT BM25(d) DESC, ann.entry_id ASC, ann._key ASC
      LIMIT @limit
      RETURN {
        id: ann.entry_id,
        f: %q,
        name: NOT_NULL(d.label, ""),
        def: NOT_NULL(d.metadata.definition.value, ""),
        s: %s
      }
)`

	// PhenotypeSearchMergeQ merges the 12 branches of the phenotype search
	// statement, keeps one best row per strain entry_id, and returns at most
	// @limit rows.
	PhenotypeSearchMergeQ = `LET hits = FLATTEN([%s])
LET best = (
  FOR x IN hits
    COLLECT eid = x.id INTO grp = x
    LET top = FIRST(
      FOR m IN grp
        SORT m.s DESC, m.f ASC, m.name ASC
        RETURN m
    )
    RETURN top
)
FOR x IN best
  SORT x.s DESC, x.id ASC
  LIMIT @limit
  RETURN {
    id: x.id,
    f: x.f,
    name: x.name,
    def: x.def,
    s: x.s
  }`
)
```

## 6. ArangoDB AQL Builder (`internal/repository/arangodb/phenotype_search_query.go`)

Create `internal/repository/arangodb/phenotype_search_query.go`:

```go
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
```

## 7. ArangoDB Repository Pipeline (`internal/repository/arangodb/phenotype_search.go`)

Create `internal/repository/arangodb/phenotype_search.go`:

```go
package arangodb

import (
	"context"
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	E "github.com/IBM/fp-go/v2/either"
	fperrors "github.com/IBM/fp-go/v2/errors"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	O "github.com/IBM/fp-go/v2/option"
	P "github.com/IBM/fp-go/v2/predicate"
	eithererr "github.com/dictyBase/fp-go-loom/eithererr"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type (
	// phenotypeSearchTargets holds the resolved ontology cv._id values
	// returned by phenotypeSearchTargetsQ.
	phenotypeSearchTargets struct {
		Graphs map[string]string `json:"graphs"`
	}

	// phenotypeSearchRequest is the validated state of one phenotype search call.
	phenotypeSearchRequest struct {
		ctx     context.Context
		query   string
		limit   int
		targets phenotypeSearchTargets
	}

	// phenotypeSearchQueryParams holds the finished AQL statement and bind map.
	phenotypeSearchQueryParams struct {
		ctx        context.Context
		statement  string
		bindParams map[string]any
	}

	// phenotypeSearchRow is one raw row of the phenotype search projection.
	phenotypeSearchRow struct {
		ID         string  `json:"id"`
		Field      string  `json:"f"`
		Name       string  `json:"name"`
		Definition string  `json:"def"`
		Score      float64 `json:"s"`
	}
)

const (
	msgNilPhenotypeSearchQuery   = "expect a non-nil phenotype search query"
	msgEmptyPhenotypeSearchQuery = "expect a non-empty phenotype search query"
)

var (
	phenotypeSearchCtxLens = L.MakeLens(
		func(r phenotypeSearchRequest) context.Context { return r.ctx },
		func(r phenotypeSearchRequest, ctx context.Context) phenotypeSearchRequest {
			r.ctx = ctx
			return r
		},
	)
	phenotypeSearchQueryLens = L.MakeLens(
		func(r phenotypeSearchRequest) string { return r.query },
		func(r phenotypeSearchRequest, q string) phenotypeSearchRequest {
			r.query = q
			return r
		},
	)
	phenotypeSearchLimitLens = L.MakeLens(
		func(r phenotypeSearchRequest) int { return r.limit },
		func(r phenotypeSearchRequest, l int) phenotypeSearchRequest {
			r.limit = l
			return r
		},
	)
	phenotypeSearchTargetsLens = L.MakeLens(
		func(r phenotypeSearchRequest) phenotypeSearchTargets { return r.targets },
		func(r phenotypeSearchRequest, t phenotypeSearchTargets) phenotypeSearchRequest {
			r.targets = t
			return r
		},
	)
	phenotypeSearchParamsCtxLens = L.MakeLens(
		func(p phenotypeSearchQueryParams) context.Context { return p.ctx },
		func(
			p phenotypeSearchQueryParams,
			ctx context.Context,
		) phenotypeSearchQueryParams {
			p.ctx = ctx
			return p
		},
	)
	phenotypeSearchStmtLens = L.MakeLens(
		func(p phenotypeSearchQueryParams) string { return p.statement },
		func(p phenotypeSearchQueryParams, s string) phenotypeSearchQueryParams {
			p.statement = s
			return p
		},
	)
	phenotypeSearchBindLens = L.MakeLens(
		func(p phenotypeSearchQueryParams) map[string]any { return p.bindParams },
		func(
			p phenotypeSearchQueryParams,
			b map[string]any,
		) phenotypeSearchQueryParams {
			p.bindParams = b
			return p
		},
	)

	hasPhenotypeRequestText = F.Pipe1(
		predord.MinStrLen(1),
		P.ContraMap(phenotypeSearchQueryLens.Get),
	)
	keepsPhenotypeNonStopword = F.Pipe2(
		repo.IsAllStopwords,
		P.Not,
		P.ContraMap(phenotypeSearchQueryLens.Get),
	)

	isPresentPhenotypeSearchQuery = P.IsNonZero[*repo.PhenotypeSearchQuery]()
	nilPhenotypeQueryErr          = eithererr.ConstErr[*repo.PhenotypeSearchQuery](msgNilPhenotypeSearchQuery)
	emptyPhenotypeRequestErr      = eithererr.ConstErr[phenotypeSearchRequest](msgEmptyPhenotypeSearchQuery)
	checkPhenotypeQueryNotNil     = E.FromPredicate(isPresentPhenotypeSearchQuery, nilPhenotypeQueryErr)
	checkPhenotypeNotEmpty    = E.FromPredicate(hasPhenotypeRequestText, emptyPhenotypeRequestErr)
	checkPhenotypeNotStopword = E.FromPredicate(keepsPhenotypeNonStopword, stopwordPhenotypeSearchError)

	isNonEmptyPhenotypeSearchRows = predarrays.IsNonEmpty[phenotypeSearchRow]()
	mapPhenotypeSearchRows        = A.Map(toPhenotypeSearchResult)
	toPhenotypeSearchResults      = F.Flow2(
		O.FromPredicate(isNonEmptyPhenotypeSearchRows),
		O.Fold(emptyPhenotypeSearchResults, mapPhenotypeSearchRows),
	)

	runPhenotypeSearchTargetsFault = fperrors.OnError(
		"error in resolving phenotype search targets",
	)
	readPhenotypeSearchTargetsFault = fperrors.OnError(
		"error in reading phenotype search targets",
	)
	runPhenotypeSearchQueryFault = fperrors.OnError(
		"error in running phenotype search query",
	)
)

// stopwordPhenotypeSearchError reports a query whose tokens are all English stopwords.
func stopwordPhenotypeSearchError(req phenotypeSearchRequest) error {
	return fmt.Errorf(
		"expect a query with at least one non-stopword, received %q",
		req.query,
	)
}

// newPhenotypeSearchRequest normalizes the query and clamps the limit.
func newPhenotypeSearchRequest(
	params *repo.PhenotypeSearchQuery,
) phenotypeSearchRequest {
	normalized := F.Pipe1(params.Query, repo.NormalizeAutocompleteQuery)
	limit := F.Pipe1(params.Limit, clampFullSearchLimit)

	return F.Pipe2(
		phenotypeSearchRequest{},
		phenotypeSearchQueryLens.Set(normalized),
		phenotypeSearchLimitLens.Set(limit),
	)
}

// validatePhenotypeSearchRequest rejects a nil request, an empty normalized
// query, and an all-stopword query before any AQL statement runs.
func validatePhenotypeSearchRequest(
	params *repo.PhenotypeSearchQuery,
) E.Either[error, phenotypeSearchRequest] {
	return F.Pipe4(
		params,
		checkPhenotypeQueryNotNil,
		E.Map[error](newPhenotypeSearchRequest),
		E.Chain(checkPhenotypeNotEmpty),
		E.Chain(checkPhenotypeNotStopword),
	)
}

// phenotypeSearchTargetBindParams builds the bind parameters for phenotypeSearchTargetsQ.
func (ar *arangorepository) phenotypeSearchTargetBindParams() map[string]any {
	return map[string]any{
		cvCollectionBind: ar.onto.Cv.Name(),
		"term_ontologies": []string{
			phenotypeOntology,
			environmentOntology,
			assayOntology,
		},
	}
}

// readPhenotypeSearchTargetsRow reads a contextResult into phenotypeSearchTargets.
func readPhenotypeSearchTargetsRow(
	row *contextResult,
) IOE.IOEither[error, phenotypeSearchTargets] {
	return F.Pipe1(
		IOE.TryCatchError(func() (phenotypeSearchTargets, error) {
			var targets phenotypeSearchTargets
			err := row.Read(&targets)

			return targets, err
		}),
		IOE.MapLeft[phenotypeSearchTargets](readPhenotypeSearchTargetsFault),
	)
}

// resolvePhenotypeSearchTargets resolves the 3 ontology cv._id values in a
// single AQL round trip. Unloaded ontologies are absent from the returned map
// and default to "" when bound.
func (ar *arangorepository) resolvePhenotypeSearchTargets(
	req phenotypeSearchRequest,
) IOE.IOEither[error, phenotypeSearchTargets] {
	ctx := phenotypeSearchCtxLens.Get(req)

	return F.Pipe2(
		IOE.TryCatchError(func() (*contextResult, error) {
			return ar.getRow(
				ctx,
				phenotypeSearchTargetsQ,
				ar.phenotypeSearchTargetBindParams(),
			)
		}),
		IOE.MapLeft[*contextResult](runPhenotypeSearchTargetsFault),
		IOE.Chain(readPhenotypeSearchTargetsRow),
	)
}

// phenotypeSearchBindParams builds the 7-entry bind map for the 12-branch query.
func (ar *arangorepository) phenotypeSearchBindParams(
	req phenotypeSearchRequest,
) map[string]any {
	targets := phenotypeSearchTargetsLens.Get(req)
	graphs := targets.Graphs

	return map[string]any{
		"q":                  phenotypeSearchQueryLens.Get(req),
		"th":                 fullSearchNgramThreshold,
		"limit":              phenotypeSearchLimitLens.Get(req),
		"anno_cvterm_graph":  ar.anno.annotg.Name(),
		bindPhenotypeGraphID: graphs[phenotypeOntology],
		bindEnvGraphID:       graphs[environmentOntology],
		bindAssayGraphID:     graphs[assayOntology],
	}
}

// buildPhenotypeSearchQueryParams assembles the 12-branch statement and bind map.
func (ar *arangorepository) buildPhenotypeSearchQueryParams(
	req phenotypeSearchRequest,
) phenotypeSearchQueryParams {
	ctx := phenotypeSearchCtxLens.Get(req)
	statement := buildPhenotypeSearchQuery()
	bindParams := ar.phenotypeSearchBindParams(req)

	return F.Pipe3(
		phenotypeSearchQueryParams{},
		phenotypeSearchParamsCtxLens.Set(ctx),
		phenotypeSearchStmtLens.Set(statement),
		phenotypeSearchBindLens.Set(bindParams),
	)
}

// runPhenotypeSearchRows runs the phenotype search statement over SearchRows
// and wraps each fault with runPhenotypeSearchQueryFault.
func (ar *arangorepository) runPhenotypeSearchRows(
	params phenotypeSearchQueryParams,
) IOE.IOEither[error, dbRows] {
	ctx := phenotypeSearchParamsCtxLens.Get(params)
	stmt := phenotypeSearchStmtLens.Get(params)
	binds := phenotypeSearchBindLens.Get(params)

	return F.Pipe1(
		IOE.TryCatchError(func() (dbRows, error) {
			return ar.searchRows(ctx, stmt, binds)
		}),
		IOE.MapLeft[dbRows](runPhenotypeSearchQueryFault),
	)
}

// emptyPhenotypeSearchResults returns a non-nil empty slice when no rows match.
func emptyPhenotypeSearchResults() []*repo.PhenotypeSearchResult {
	return []*repo.PhenotypeSearchResult{}
}

// toPhenotypeSearchResult maps one projection row to a repository PhenotypeSearchResult.
func toPhenotypeSearchResult(row phenotypeSearchRow) *repo.PhenotypeSearchResult {
	return &repo.PhenotypeSearchResult{
		EntryID:    row.ID,
		Field:      row.Field,
		Name:       row.Name,
		Definition: row.Definition,
		Score:      row.Score,
	}
}

// SearchPhenotypes returns at most Limit ranked strain phenotype matches for the
// normalized query of params as a lazy IOEither pipeline.
func (ar *arangorepository) SearchPhenotypes(
	ctx context.Context,
	params *repo.PhenotypeSearchQuery,
) IOE.IOEither[error, []*repo.PhenotypeSearchResult] {
	baseCtx := normalizeQueryContext(ctx)
	withCtx := phenotypeSearchCtxLens.Set(baseCtx)
	rowFault := scanSearchRows[phenotypeSearchRow](
		"error in reading phenotype search row",
	)

	return F.Pipe7(
		IOE.Of[error](params),
		IOE.ChainEitherK(validatePhenotypeSearchRequest),
		IOE.Map[error](withCtx),
		IOE.Bind(phenotypeSearchTargetsLens.Set, ar.resolvePhenotypeSearchTargets),
		IOE.Map[error](ar.buildPhenotypeSearchQueryParams),
		IOE.Chain(ar.runPhenotypeSearchRows),
		IOE.Chain(rowFault),
		IOE.Map[error](toPhenotypeSearchResults),
	)
}
```

## 8. Service Layer Pipeline (`internal/app/service/phenotype_search.go`)

Create `internal/app/service/phenotype_search.go`:

```go
package service

import (
	"context"

	A "github.com/IBM/fp-go/v2/array"
	E "github.com/IBM/fp-go/v2/either"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	O "github.com/IBM/fp-go/v2/option"
	P "github.com/IBM/fp-go/v2/predicate"
	R "github.com/IBM/fp-go/v2/record"
	S "github.com/IBM/fp-go/v2/string"
	"github.com/bufbuild/protovalidate-go"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

const (
	fieldPhenotypeLabel      = "label"
	fieldPhenotypeDefinition = "definition"
	fieldPhenotypeComment    = "comment"
)

type (
	// phenotypeSearchInput is the boundary record of the handler: the caller
	// context, the request, and the repository travel as data rather than
	// being captured in a closure.
	phenotypeSearchInput struct {
		ctx     context.Context
		request *annotation.PhenotypeSearchParameters
		repo    repository.TaggedAnnotationRepository
	}

	// phenotypeSearchParams is the validated and normalized search input.
	phenotypeSearchParams struct {
		query string
		limit int
	}

	// phenotypeSearchState is the state of one handler run, once the query is
	// normalized and the repository call has been made.
	phenotypeSearchState struct {
		ctx   context.Context
		repo  repository.TaggedAnnotationRepository
		query *repository.PhenotypeSearchQuery
		rows  []*repository.PhenotypeSearchResult
	}

	// phenotypeSearchOutcome is the boundary record the handler folds into.
	phenotypeSearchOutcome struct {
		collection *annotation.PhenotypeSearchResultCollection
		err        error
	}
)

var (
	// phenotypeSearchRowsLens focuses the search results of a handler run, so
	// IOE.Bind can attach the repository result to the state.
	phenotypeSearchRowsLens = L.MakeLens(
		func(s phenotypeSearchState) []*repository.PhenotypeSearchResult { return s.rows },
		func(
			s phenotypeSearchState,
			rows []*repository.PhenotypeSearchResult,
		) phenotypeSearchState {
			s.rows = rows
			return s
		},
	)

	// hasPhenotypeSearchQueryText reports whether the normalized query still
	// holds text after whitespace and combining-mark stripping.
	hasPhenotypeSearchQueryText = F.Pipe1(
		predord.MinStrLen(1),
		P.ContraMap(phenotypeSearchQueryText),
	)

	// hasLongEnoughPhenotypeSearchQuery reports whether the normalized query
	// reaches the documented minimum length in characters.
	hasLongEnoughPhenotypeSearchQuery = F.Pipe2(
		predarrays.MinLen[rune](minFullSearchQueryLength),
		P.ContraMap(S.ToRunes),
		P.ContraMap(phenotypeSearchQueryText),
	)

	// hasNonStopwordPhenotypeSearchQuery reports whether the normalized query
	// retains at least one non-stopword word.
	hasNonStopwordPhenotypeSearchQuery = F.Pipe2(
		repository.IsAllStopwords,
		P.Not,
		P.ContraMap(phenotypeSearchQueryText),
	)

	emptyPhenotypeQueryErr    = invalidParamStatusOf[phenotypeSearchParams](msgEmptyFullSearchQuery)
	shortPhenotypeQueryErr    = invalidParamStatusOf[phenotypeSearchParams](msgShortFullSearchQuery)
	stopwordPhenotypeQueryErr = invalidParamStatusOf[phenotypeSearchParams](msgStopwordFullSearchQuery)

	checkPhenotypeQueryText     = E.FromPredicate(hasPhenotypeSearchQueryText, emptyPhenotypeQueryErr)
	checkPhenotypeQueryLength   = E.FromPredicate(hasLongEnoughPhenotypeSearchQuery, shortPhenotypeQueryErr)
	checkPhenotypeQueryStopword = E.FromPredicate(hasNonStopwordPhenotypeSearchQuery, stopwordPhenotypeQueryErr)

	searchPhenotypeHits = F.Flow2(
		rawSearchPhenotypeHits,
		IOE.MapLeft[[]*repository.PhenotypeSearchResult](serverErrorStatus),
	)

	phenotypeSearchFieldMap = map[string]annotation.PhenotypeSearchField{
		fieldPhenotypeLabel:      annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
		fieldSynonym:             annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_SYNONYM,
		fieldPhenotypeDefinition: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_DEFINITION,
		fieldPhenotypeComment:    annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_COMMENT,
	}

	lookupPhenotypeSearchField = F.Bind2nd(
		R.Lookup[string, annotation.PhenotypeSearchField],
		phenotypeSearchFieldMap,
	)

	toPhenotypeSearchField = F.Flow2(
		lookupPhenotypeSearchField,
		O.GetOrElse(F.Constant(
			annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_UNSPECIFIED,
		)),
	)
)

// SearchPhenotypes returns at most 50 ranked strain phenotype matches
// for a search query of at least 2 normalized characters.
func (srv *AnnotationService) SearchPhenotypes(
	ctx context.Context,
	r *annotation.PhenotypeSearchParameters,
) (*annotation.PhenotypeSearchResultCollection, error) {
	seed := phenotypeSearchInput{ctx: ctx, request: r, repo: srv.repo}
	result := F.Pipe5(
		IOE.Of[error](seed),
		IOE.ChainEitherK(validatedPhenotypeSearch),
		IOE.Bind(phenotypeSearchRowsLens.Set, searchPhenotypeHits),
		IOE.Map[error](toPhenotypeSearchResultCollection),
		ioeutils.ToEither[error, *annotation.PhenotypeSearchResultCollection],
		E.Fold(failedPhenotypeSearchOutcome, successfulPhenotypeSearchOutcome),
	)

	return result.collection, result.err
}

// validatedPhenotypeSearch is the pure guard chain of the phenotype search handler.
func validatedPhenotypeSearch(
	in phenotypeSearchInput,
) E.Either[error, phenotypeSearchState] {
	valErr := protovalidate.Validate(in.request)
	toState := F.Bind1st(newPhenotypeSearchState, in)

	return F.Pipe6(
		E.TryCatchError(in.request, valErr),
		E.Map[error](newPhenotypeSearchParams),
		E.Chain(checkPhenotypeQueryText),
		E.Chain(checkPhenotypeQueryLength),
		E.Chain(checkPhenotypeQueryStopword),
		E.Map[error](toState),
		E.MapLeft[phenotypeSearchState](invalidParamStatus),
	)
}

// newPhenotypeSearchParams normalizes the request query and turns a non-positive
// limit into the service default.
func newPhenotypeSearchParams(
	r *annotation.PhenotypeSearchParameters,
) phenotypeSearchParams {
	attrs := r.GetData().GetAttributes()
	requested := int(attrs.GetLimit())
	effective := F.Pipe2(
		requested,
		O.FromPredicate(isPositiveLimit),
		O.GetOrElse(F.Constant(defaultFullSearchLimit)),
	)
	rawQuery := attrs.GetQuery()
	query := repository.NormalizeAutocompleteQuery(rawQuery)

	return phenotypeSearchParams{
		query: query,
		limit: effective,
	}
}

// phenotypeSearchQueryText extracts the query text of normalized parameters.
func phenotypeSearchQueryText(params phenotypeSearchParams) string {
	return params.query
}

// newPhenotypeSearchState starts one phenotype search handler run against a repository.
func newPhenotypeSearchState(
	in phenotypeSearchInput,
	params phenotypeSearchParams,
) phenotypeSearchState {
	return phenotypeSearchState{
		ctx:  in.ctx,
		repo: in.repo,
		query: &repository.PhenotypeSearchQuery{
			Query: params.query,
			Limit: params.limit,
		},
	}
}

// rawSearchPhenotypeHits invokes the repository phenotype search.
func rawSearchPhenotypeHits(
	s phenotypeSearchState,
) IOE.IOEither[error, []*repository.PhenotypeSearchResult] {
	return s.repo.SearchPhenotypes(s.ctx, s.query)
}

// toProtoPhenotypeSearchResult maps one repository phenotype search result onto
// its protobuf wire representation.
func toProtoPhenotypeSearchResult(
	row *repository.PhenotypeSearchResult,
) *annotation.PhenotypeSearchResult {
	field := toPhenotypeSearchField(row.Field)

	return &annotation.PhenotypeSearchResult{
		EntryId:    row.EntryID,
		Field:      field,
		Name:       row.Name,
		Definition: row.Definition,
		Score:      row.Score,
	}
}

// toPhenotypeSearchResultCollection projects a completed handler run onto the
// protobuf response collection.
func toPhenotypeSearchResultCollection(
	s phenotypeSearchState,
) *annotation.PhenotypeSearchResultCollection {
	data := F.Pipe1(s.rows, A.Map(toProtoPhenotypeSearchResult))
	count := len(data)
	limit := int64(s.query.Limit)
	total := int64(count)

	return &annotation.PhenotypeSearchResultCollection{
		Data: data,
		Meta: &annotation.Meta{
			Limit: limit,
			Total: total,
		},
	}
}

// failedPhenotypeSearchOutcome is the boundary projection of a failed handler run.
func failedPhenotypeSearchOutcome(err error) phenotypeSearchOutcome {
	return phenotypeSearchOutcome{err: err}
}

// successfulPhenotypeSearchOutcome is the boundary projection of a completed
// handler run.
func successfulPhenotypeSearchOutcome(
	collection *annotation.PhenotypeSearchResultCollection,
) phenotypeSearchOutcome {
	return phenotypeSearchOutcome{collection: collection}
}
```

## 9. Test Suite

### 9.1 Repository Tests (`internal/repository/arangodb/phenotype_search_test.go`)

- **`TestEnsureFullSearchCreatesAssets` (update in `fullsearch_test.go`):** Assert that `termLink.Fields[docFieldMetadata].Fields` indexes `definition.value` (`text_en`), `synonyms.value` (`annotation_search_norm`, `annotation_search_ngram`, `text_en`), and `comments` (`text_en`).
- **`TestBuildPhenotypeSearchQueryBranchCount`:** Assert that `buildPhenotypeSearchQuery()` contains `p0..p1`, `n0..n1`, `t0..t3`, `h0..h3`, and `LET hits = FLATTEN([p0, p1, n0, n1, t0, t1, t2, t3, h0, h1, h2, h3])`.
- **`TestValidatePhenotypeSearchRequest`:** Test `validatePhenotypeSearchRequest` directly for `nil` input, empty string, whitespace/combining marks, all-stopword queries, and limit clamping.
- **`TestSearchPhenotypesAcrossThreeOntologiesAndFourFields`:** Seed terms in `Dicty Phenotypes`, `Dicty Environment`, and `Dictyostelium Assay` with `label`, `metadata.synonyms`, `metadata.definition`, and `metadata.comments`, and attach active strain annotations (`DBS...`) via `AddAnnotation`. Verify:
  1. Prefix match (`>= 1000`) on `label` and `metadata.synonyms.value` ranks above phrase (`500..750`), token (`250..500`), and fuzzy (`< 250`).
  2. Matches on `metadata.definition.value` and `metadata.comments` return the strain `EntryID` (`DBS...`), `Field` (`"definition"` or `"comment"`), term `Name`, term `Definition`, and band score.
  3. Matches in `Dicty Environment` (`in suspension`) and `Dictyostelium Assay` (`confocal microscopy`) return their linked strain `EntryID`s alongside `Dicty Phenotypes`.
  4. Deprecated terms (`deprecated: true`), obsolete annotations (`is_obsolete: true`), non-strain annotations (`DDB_G...`, `DBP...`), and terms in non-phenotype ontologies (`dicty_annotation`) are excluded.
  5. Duplicate matches across multiple branches or multiple phenotype terms for the same strain `entry_id` are deduplicated to the single highest-scoring hit.

### 9.2 Service Unit Tests (`internal/app/service/phenotype_search_test.go`)

- Extend `stubFullSearchRepo` (or add `phenotypeParams *repository.PhenotypeSearchQuery` and `phenotypeRows []*repository.PhenotypeSearchResult`) to implement `SearchPhenotypes`.
- Test `validatedPhenotypeSearch` and `srv.SearchPhenotypes` for:
  - Mapping `EntryID` (`GetEntryId()`), `Field` (`GetField()` -> `PhenotypeSearchField`), `Name`, `Definition`, `Score`, `Meta.Limit`, and `Meta.Total`.
  - Query normalization (`"  AX4  "` -> `"ax4"`) and default limit (`0` -> `50`).
  - `codes.InvalidArgument` on `nil` `Data`, `nil` `Attributes`, raw short query (`"a"`), padded short query (`" a "`), combining-mark-only query, and all-stopword query (`"the and"`).
  - `codes.Internal`, `codes.Canceled`, and `codes.DeadlineExceeded` propagation from repository errors.

### 9.3 Service End-to-End ArangoDB Tests (`internal/app/service/phenotype_search_arango_test.go`)

- Reuse `fullSearchEndToEndSetup` over `bufconn` with `dicty_annotation.json` and `dicty_phenotypes.json` loaded.
- Seed environment and assay ontology terms plus synonym/definition/comment fields, create strain annotations (`DBS...`) linked to those terms, poll `client.SearchPhenotypes`, and assert ranking and `entry_id`, `field`, `name`, `definition`, and `score` projection end-to-end.
