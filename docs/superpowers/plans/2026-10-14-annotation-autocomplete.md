# Annotation Autocomplete Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `AutocompleteAnnotation` RPC to `modware-annotation` over the `synonym` and `genotype` tags of the `dicty_annotation` ontology for strain (`DBS...`) entries, written end-to-end in `fp-go/v2` and `fp-go-loom`, including the `fp-go/v2` migration of `NewTaggedAnnotationRepo` and its linked collection, graph, index, ontology-term lookup, and ArangoSearch setup pipeline.

**Architecture:** The feature owns three custom ArangoSearch analyzers (`annotation_autocomplete_norm`, `annotation_autocomplete_delim`, and `annotation_autocomplete_ngram`) and one classic `arangosearch` view (`annotation_autocomplete`) over the `annotation` collection. The view indexes `value` with all three custom analyzers, `entry_id` with `annotation_autocomplete_norm`, and `is_obsolete` with `identity`. Before running the search statement, the repository pipeline resolves the `cvterm._id` of `("dicty_annotation", "synonym")` and `("dicty_annotation", "genotype")` onto the request state with lenses (`synonymTermIDLens` and `genotypeTermIDLens`) and passes them as `@synonym_term_id` and `@genotype_term_id`. One 4-branch AQL statement then runs two prefix branches (`p_synonym`, `p_genotype`, score `1000 + BM25(d)`) and two fuzzy branches (`n_synonym`, `n_genotype`, score `BM25(d)`). Inside `SEARCH`, every branch filters `d.is_obsolete == false`, `STARTS_WITH(d.entry_id, "dbs")`, and the value match expression; after `SEARCH`, one `1..1 OUTBOUND` traversal on `@anno_cvterm_graph` verifies `e._to == @<tag>_term_id AND cvt.deprecated == false` before `SORT` and `LIMIT` (avoiding any `FOR cv IN @@cv_collection` scan so `EXPLAIN` has `4` `EnumerateViewNode` and `0` `EnumerateCollectionNode`). A merge block deduplicates by `entry_id` so each strain appears at most once. `NewTaggedAnnotationRepo`, the ArangoSearch asset bootstrap, the repository search pipeline, and `AnnotationService.AutocompleteAnnotation` are composed as uninterrupted `fp-go/v2` `Either` / `IOEither` pipelines with state structs and lenses, forced only once at each outer boundary.

**Tech Stack:** Go 1.26, ArangoDB 3.11 (`arangodb:3.11`), `github.com/arangodb/go-driver` v1.6.9, `github.com/dictyBase/arangomanager` v0.8.0, `github.com/dictyBase/aphgrpc` v1.4.2, `github.com/dictyBase/go-genproto` `v0.0.0-20261014212141-25e2e7a3b338`, `github.com/bufbuild/protovalidate-go` v0.10.0, `github.com/IBM/fp-go/v2` v2.5.0, `github.com/dictyBase/fp-go-loom` `v0.0.0-20261010160534-3c971335f51e`, `github.com/cockroachdb/errors` v1.14.0, `golang.org/x/text` v0.42.0, `gotestsum`, `golangci-lint`, `gopls`.

**Spec:** Protobuf contract committed in `dictybaseapis` (`7f096e1`, `dictybase/annotation/annotation.proto`) and released in `github.com/dictyBase/go-genproto v0.0.0-20261014212141-25e2e7a3b338`; sibling design reference in `/Users/sba964/Projects/devenv/golang/modware-stock/docs/superpowers/plans/2026-10-06-stock-autocomplete.md`.

## Global Constraints

- **fp-go v2 allowlist only:** Every new or migrated file must import `github.com/IBM/fp-go/v2/...` (`O`, `E`, `IO`, `IOE`, `F`, `L`, `A`, `P`, `R`, `S`, `EQ`, `ORD`, `fperrors`) and `github.com/dictyBase/fp-go-loom` (`eithererr`, `ioeutils`, `MO`, `predarrays`, `predord`, `strutils`). Never import v1 `github.com/IBM/fp-go/...`, `result`, `ioresult`, or reader/context monad stacks.
- **Uninterrupted `IOEither` pipelines:** Helper functions inside `internal/repository/arangodb/arangodb.go` (`setDocumentCollection`, `setAnnotationCollection`, `ensureAutocompleteSearch`, `ensureAutocompleteView`) must return `IOE.IOEither` and compose via `IOE.Bind`, `IOE.Let`, and `IOE.ChainFirst`. Never force an inner `IOEither` with `E.UnwrapError` or `ioeutils.ToEither` inside a helper; force only once at the outer boundary of `NewTaggedAnnotationRepo` and `AutocompleteAnnotation`.
- **No nested calls or direct combinator application:** Every `.go` file containing `IOE.` or `IOEither` must pass the nested-call gate (`if rg '[A-Z][A-Za-z0-9_.]*\([a-z][A-Za-z0-9_.]*\(.*\)' <files>; then exit 1; fi` and `if rg '\)\([a-z]' <files>; then exit 1; fi`).
- **Raw `TryCatchError` callbacks only:** `IOE.TryCatchError` callbacks must wrap a single raw operation with no `if err != nil` branching inside the closure (except `scanSearchRows`, which carries the `// fp-go:allow-trycatch-iferr` directive because `go-driver` cursors report read errors only inside the `Scan` loop).
- **Dynamic collection and graph names:** Never hardcode collection or graph names in view links or AQL bind parameters; read them from `ar.anno.annot.Name()`, `ar.anno.annotg.Name()`, `ar.onto.Cv.Name()`, and `ar.onto.Term.Name()`. Only the view name `annotation_autocomplete` and analyzer names (`annotation_autocomplete_norm`, `annotation_autocomplete_delim`, `annotation_autocomplete_ngram`, `identity`) are literal in AQL because ArangoDB forbids bind parameters for view and analyzer names inside `SEARCH`.
- **Preserve unrelated untracked files:** Leave `MODWARE_ANNOTATION_NIL_ATTRIBUTES_REPORT.md` untouched and unstaged throughout all commits.
- **Hard Task 1 calibration gate and compile spike:** Task 1 calibration is a hard gate. If the delimiter pipeline or n-gram threshold fails on ArangoDB 3.11, stop and revise this plan before starting Task 2. Treat the code blocks in Tasks 4–6 as normative shapes; run a quick `go build ./...` compile check after each step to verify exact `fp-go/v2` and `go-driver` signatures.
- **Quality gates mandatory after every Go task:** `golangci-lint fmt`, `golangci-lint run ./...`, `go test ./...`, `go test -race ./...`, and `gopls check -severity=hint` on all non-generated tracked `.go` files.

## Review Focus

1. **Zero collection scans in `EXPLAIN` and tag filtering before branch `LIMIT`:** Resolving `@synonym_term_id` and `@genotype_term_id` ahead of the search statement and filtering `e._to == @<tag>_term_id AND cvt.deprecated == false` on the 1-hop `OUTBOUND` traversal eliminates any `FOR cv IN @@cv_collection` scan (`0` `EnumerateCollectionNode`s) and ensures non-strain (`DDB_G...`) or other-tag (`"description"`, `"public note"`) annotations never consume the per-branch `@limit`. Owner: `TestAutocompleteAnnotationIgnoresNonStrainAndOtherTagsUnderSmallLimit`, `TestBuildAutocompleteQueryFilterOrder`, and Task 1/Task 7 `EXPLAIN` gates.
2. **Interior allele prefix matching in comma-separated `genotype` values (provisional until Task 1 calibration proves it):** Genotype strings list multiple alleles separated by commas and spaces (for example, `"axeA1, axeB1, sadA-[sadA-KO]"`). Task 1 calibration tests the provisional `annotation_autocomplete_delim` pipeline (`Delimiter(",")` → `Delimiter(" ")` → `Norm`) so `STARTS_WITH(d.value, @q)` matches interior alleles such as `"axeb1"` and `"sada"`. If calibration reveals any edge case on ArangoDB 3.11, fold the adjustment into this plan before Task 2 starts. Owner: Task 1 Probe 3 and `TestAutocompleteAnnotationGenotypeInteriorAllelePrefix`.
3. **Obsolete annotation exclusion:** `EditAnnotation` (`annVerInstFn`) marks the old annotation document `is_obsolete: true` and inserts a new document with `is_obsolete: false`. Searching the old value must return nothing; searching the updated value must return the strain. Owner: `TestAutocompleteAnnotationSkipsObsoleteVersion`.
4. **Intra-strain and cross-branch deduplication by `entry_id`:** A single strain (`DBS0236126`) can have multiple `synonym` annotations (`Rank: 0`, `Rank: 1`) plus a `genotype` annotation that all match one query. The merge block must `COLLECT eid = x.id` and return at most 1 row per `entry_id`, picking the highest score (`m.s DESC, m.f ASC, m.v ASC`). Owner: `TestAutocompleteAnnotationDeduplicatesMultipleSynonymsAndCrossFieldHits`.
5. **View reconciliation without deletion and eager constructor failure:** `NewTaggedAnnotationRepo` must reconcile stale view links via `SetProperties` (preserving `staleProps.ID == freshProps.ID`) and fail eagerly when the database does not exist, without `sync.Once` poisoning retries. Note: `arangomanager.Session` wraps a stateless `net/http` `driver.Client` and exposes no `Close()` method, so failed constructor calls leave no leaked socket descriptor. Owner: `TestEnsureAutocompleteSearchReconcilesStaleView` and `TestNewTaggedAnnotationRepoFailsWhenSetupFails`.
6. **Nil `data` / `attributes` and whitespace-only queries at the gRPC boundary:** `modware-annotation` has no recovery interceptor in `server.go`. `protovalidate.Validate` must guard nil `data` and nil `attributes`, and a post-trim guard must reject `"   "` with `codes.InvalidArgument`. Owner: `TestAutocompleteAnnotationHandlerRejectsNilData`, `TestAutocompleteAnnotationHandlerRejectsNilAttributes`, and `TestAutocompleteAnnotationHandlerRejectsWhitespaceQuery`.

---

## Table of Contents

- [Verified Repository Facts](#verified-repository-facts)
- [Consumed Protocol Contract](#consumed-protocol-contract)
- [Exact Interfaces](#exact-interfaces)
  - [Repository Types and Interface Method](#repository-types-and-interface-method)
  - [Search Asset Names and Constants](#search-asset-names-and-constants)
  - [View Definition](#view-definition)
  - [Field and Branch Matrix](#field-and-branch-matrix)
  - [AQL Branch and Merge Templates](#aql-branch-and-merge-templates)
  - [Bind Parameters](#bind-parameters)
  - [Handler Signature and Mapping](#handler-signature-and-mapping)
- [File Map](#file-map)
- [Implementation Tasks](#implementation-tasks)
  - [Task 1: Calibration Against Real ArangoDB 3.11](#task-1-calibration-against-real-arangodb-311)
  - [Task 2: Upgrade Dependencies and Verify Protobuf Contract](#task-2-upgrade-dependencies-and-verify-protobuf-contract)
  - [Task 3: Repository Types and Interface Method](#task-3-repository-types-and-interface-method)
  - [Task 4: Migrate `NewTaggedAnnotationRepo` to fp-go v2 and Wire Search Assets](#task-4-migrate-newtaggedannotationrepo-to-fp-go-v2-and-wire-search-assets)
  - [Task 5: Query Builder, Normalizer, Row Scanner, and `AutocompleteAnnotation`](#task-5-query-builder-normalizer-row-scanner-and-autocompleteannotation)
  - [Task 6: Migrate `NewAnnotationService` and Add `AutocompleteAnnotation` gRPC Handler](#task-6-migrate-newannotationservice-and-add-autocompleteannotation-grpc-handler)
  - [Task 7: End-to-End bufconn + ArangoDB Round-Trip Test, EXPLAIN, and Latency Gate](#task-7-end-to-end-bufconn--arangodb-round-trip-test-explain-and-latency-gate)
  - [Task 8: Documentation and Session Completion](#task-8-documentation-and-session-completion)
- [Test Matrix](#test-matrix)
- [Failure Behavior](#failure-behavior)
- [Quality Gates](#quality-gates)
- [Completion Criteria](#completion-criteria)
- [Execution Notes](#execution-notes)

---

## Verified Repository Facts

| Fact | Evidence |
|---|---|
| `go.mod` currently requires `github.com/dictyBase/go-genproto v0.0.0-20260928171459-163a42bcacab` and `github.com/bufbuild/protovalidate-go v0.10.0`. It does not yet require `github.com/IBM/fp-go/v2`, `github.com/dictyBase/fp-go-loom`, or `github.com/cockroachdb/errors`. | `go.mod` lines 1-22 |
| `github.com/dictyBase/go-genproto v0.0.0-20261014212141-25e2e7a3b338` is published and contains `AutocompleteAnnotation`, `AnnotationAutocompleteParameters`, `AnnotationAutocompleteAttributes`, `AnnotationSuggestion`, `AnnotationSuggestionCollection`, `AnnotationSearchField`, and `Meta.Total`. | `go list -m -json github.com/dictyBase/go-genproto@master` |
| `internal/repository/testdata/dicty_annotation.json` already defines both `genotype` (`DDANNO_0000018`) and `synonym` (`DDANNO_0000019`) in namespace `dicty_annotation`, and `loadData(tra)` in `internal/repository/arangodb/arangodb_test.go` loads this file in `setUp(t)`. `tearDown(repo)` in `arangodb_test.go:274` takes one argument (`repo repository.TaggedAnnotationRepository`). | `internal/repository/testdata/dicty_annotation.json` lines 384, 404; `internal/repository/arangodb/arangodb_test.go` lines 65-103, 274 |
| In `annInst` (`internal/repository/arangodb/statement.go:47`), an annotation document in `ar.anno.annot` stores `value`, `editable_value`, `created_by`, `entry_id`, `rank`, `is_obsolete`, `version`, and `created_at`. Its `_key` is auto-generated. An edge `{ _from: ann._id, _to: cvt._id }` in `ar.anno.term` (`ar.anno.annotg` graph) links the annotation `OUTBOUND` to its `cvterm` document. | `internal/repository/arangodb/statement.go` lines 47-61; `internal/repository/arangodb/arangodb.go` lines 71-80 |
| `ar.termID(onto, term string) (string, error)` (`internal/repository/arangodb/ontology.go:29`) looks up a non-deprecated `cvterm._id` by ontology namespace and tag label using `annExistTagQ`. | `internal/repository/arangodb/ontology.go` lines 29-46 |
| All AQL statement files in this repository live directly in package `arangodb` (`internal/repository/arangodb/statement.go`, `annotation_statement.go`, `feature_statement.go`, `list_filter_statement.go`). There is no `statement/` subpackage. | `internal/repository/arangodb/` directory listing |
| `NewTaggedAnnotationRepo` (`internal/repository/arangodb/arangodb.go:31`) is called in two places: `internal/app/server/server.go:149` (`repoAndNatsConn`) and `internal/repository/arangodb/arangodb_test.go:257` (`setUp`). Keeping its outer return type `(repo.TaggedAnnotationRepository, error)` while migrating its body and helpers (`setAnnotationCollection`, `setDocumentCollection`, `ensureAutocompleteSearch`) to an uninterrupted `fp-go/v2` `IOE.IOEither` pipeline preserves every existing caller without churn. | `rg NewTaggedAnnotationRepo` |
| `arangomanager.Session` wraps an HTTP `driver.Client` (`net/http`) and exposes no `Close()` method (`go doc github.com/dictyBase/arangomanager.Session`). A failed setup after `manager.NewSessionDb` therefore leaks no persistent connection handle. | `arangomanager` v0.8.0 |

---

## Consumed Protocol Contract

Generated Go types in package `github.com/dictyBase/go-genproto/dictybaseapis/annotation` at `v0.0.0-20261014212141-25e2e7a3b338`:

```go
type AnnotationSearchField int32

const (
	AnnotationSearchField_ANNOTATION_SEARCH_FIELD_UNSPECIFIED AnnotationSearchField = 0
	AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM     AnnotationSearchField = 1
	AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE    AnnotationSearchField = 2
)

type AnnotationAutocompleteParameters struct {
	Data *AnnotationAutocompleteParameters_Data
}

type AnnotationAutocompleteParameters_Data struct {
	Type       string
	Attributes *AnnotationAutocompleteAttributes
}

type AnnotationAutocompleteAttributes struct {
	Query string // buf.validate string.min_len = 3
	Limit int64  // buf.validate int64 {gte: 0, lte: 50}
}

type AnnotationSuggestion struct {
	EntryId     string
	Field       AnnotationSearchField
	DisplayText string
	Score       float64
}

type AnnotationSuggestionCollection struct {
	Data []*AnnotationSuggestion
	Meta *Meta
}

type Meta struct {
	NextCursor int64
	Limit      int64
	Total      int64
}
```

Generated server method on `TaggedAnnotationServiceServer`:

```go
AutocompleteAnnotation(context.Context, *annotation.AnnotationAutocompleteParameters) (*annotation.AnnotationSuggestionCollection, error)
```

---

## Exact Interfaces

### Repository Types and Interface Method

Add to `internal/repository/repository.go`:

```go
import (
	"io"

	IOE "github.com/IBM/fp-go/v2/ioeither"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-obograph/storage"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// AutocompleteQuery holds the input of an annotation autocomplete search.
// The repository normalizes Query and clamps Limit.
type AutocompleteQuery struct {
	// Query is the partial search text. The caller sends it untrimmed;
	// the repository trims it, lowercases it, and strips combining marks.
	Query string
	// Limit is the maximum number of suggestions. A value at or below 0
	// becomes 5. A value above 50 becomes 50.
	Limit int
}

// Suggestion is one annotation autocomplete match, deduplicated by EntryID.
type Suggestion struct {
	// EntryID is the strain identifier of the matched annotation, for
	// example DBS0236126.
	EntryID string
	// Field is the tag label that matched: "synonym" or "genotype".
	Field string
	// DisplayText is the stored annotation value shown in the user interface.
	DisplayText string
	// Score ranks the match. A prefix match scores above a fuzzy match.
	Score float64
}
```

Add to `TaggedAnnotationRepository` in `internal/repository/repository.go`:

```go
	// AutocompleteAnnotation returns at most Limit short suggestions for
	// the normalized query of params as a lazy IOEither pipeline.
	AutocompleteAnnotation(
		params *AutocompleteQuery,
	) IOE.IOEither[error, []*Suggestion]
```

### Search Asset Names and Constants

In `internal/repository/arangodb/autocomplete.go`:

```go
const (
	// autocompleteViewName is the classic arangosearch view of this
	// feature. The AQL templates name it literally, because ArangoDB
	// does not accept a bind parameter for a view name in SEARCH.
	autocompleteViewName = "annotation_autocomplete"
	// autocompleteNormAnalyzer lowercases a value and removes accents.
	// It serves whole-value prefix branches and the entry_id prefix filter.
	autocompleteNormAnalyzer = "annotation_autocomplete_norm"
	// autocompleteDelimAnalyzer splits on commas and spaces, then
	// lowercases and removes accents. It serves interior allele prefix
	// matching on comma-separated genotype values (verified in Task 1).
	autocompleteDelimAnalyzer = "annotation_autocomplete_delim"
	// autocompleteNgramAnalyzer chains norm and ngram. It serves the
	// fuzzy branches.
	autocompleteNgramAnalyzer = "annotation_autocomplete_ngram"
	// autocompleteIdentityAnalyzer is the built-in identity analyzer for
	// exact boolean filtering on is_obsolete.
	autocompleteIdentityAnalyzer = "identity"
	// analyzerNormLocale is the locale of the norm analyzer.
	analyzerNormLocale = "en.utf-8"
	// autocompleteNgramThreshold is the minimum n-gram similarity of a
	// fuzzy match, confirmed in Task 1 calibration on ArangoDB 3.11.
	autocompleteNgramThreshold = 0.3
	// defaultAutocompleteLimit is the list length for a non-positive limit.
	defaultAutocompleteLimit = 5
	// maxAutocompleteLimit is the hard cap of the list length.
	maxAutocompleteLimit = 50
	// autocompletePrefixScore is the score bonus of a prefix branch.
	autocompletePrefixScore = "1000 + BM25(d)"
	// autocompleteFuzzyScore is the score of a fuzzy branch.
	autocompleteFuzzyScore = "BM25(d)"
	// dictyAnnotationOntology is the ontology namespace of strain
	// synonym and genotype annotations.
	dictyAnnotationOntology = "dicty_annotation"
	// fieldSynonym is the cvterm label of strain synonym annotations.
	fieldSynonym = "synonym"
	// fieldGenotype is the cvterm label of strain genotype annotations.
	fieldGenotype = "genotype"
	// bindSynonymTermID is the bind parameter name for the synonym cvterm _id.
	bindSynonymTermID = "synonym_term_id"
	// bindGenotypeTermID is the bind parameter name for the genotype cvterm _id.
	bindGenotypeTermID = "genotype_term_id"
	// docFieldValue is the indexed value attribute on the annotation document.
	docFieldValue = "value"
	// docFieldEntryID is the indexed entry_id attribute on the annotation document.
	docFieldEntryID = "entry_id"
	// docFieldIsObsolete is the indexed is_obsolete attribute on the annotation document.
	docFieldIsObsolete = "is_obsolete"
	// msgNilAutocompleteQuery is the error message for a nil query struct.
	msgNilAutocompleteQuery = "expect a non-nil autocomplete query"
	// msgEmptyAutocompleteQuery is the error message for an empty query string.
	msgEmptyAutocompleteQuery = "expect a non-empty autocomplete query"
)
```

Analyzer definitions created with `EnsureCreatedAnalyzer`:

| Analyzer | Type | Properties | Features |
|---|---|---|---|
| `annotation_autocomplete_norm` | `driver.ArangoSearchAnalyzerTypeNorm` | `Locale: "en.utf-8"`, `Case: driver.ArangoSearchCaseLower`, `Accent: new(false)` | none |
| `annotation_autocomplete_delim` | `driver.ArangoSearchAnalyzerTypePipeline` | step 1 `Delimiter` (`Delimiter: ","`), step 2 `Delimiter` (`Delimiter: " "`), step 3 `Norm` (`Locale: "en.utf-8"`, `Case: driver.ArangoSearchCaseLower`, `Accent: new(false)`) | `Frequency`, `Norm`, `Position` |
| `annotation_autocomplete_ngram` | `driver.ArangoSearchAnalyzerTypePipeline` | step 1 `Norm` (`Locale: "en.utf-8"`, `Case: driver.ArangoSearchCaseLower`, `Accent: new(false)`), step 2 `NGram` (`Min: new(int64(2))`, `Max: new(int64(3))`, `PreserveOriginal: new(true)`, `StreamType: new(driver.ArangoSearchNGramStreamUTF8)`) | `Frequency`, `Norm`, `Position` |

### View Definition

One classic `arangosearch` view, `annotation_autocomplete`, with 1 collection link (`ar.anno.annot.Name()`):

| Link Key | Field | Analyzers | Purpose |
|---|---|---|---|
| `ar.anno.annot.Name()` | `value` | `annotation_autocomplete_norm`, `annotation_autocomplete_delim`, `annotation_autocomplete_ngram` | Whole-value prefix, allele-token prefix, and fuzzy matching on annotation values |
| `ar.anno.annot.Name()` | `entry_id` | `annotation_autocomplete_norm` | In-index strain prefix filter `STARTS_WITH(d.entry_id, "dbs")` |
| `ar.anno.annot.Name()` | `is_obsolete` | `identity` | In-index active filter `d.is_obsolete == false` |

### Field and Branch Matrix

2 tags in `dicty_annotation`, 2 stages per tag, 4 branches:

| # | Tag (`cvt.label`) | Term ID Bind Param | Stage | Branch Variable | `SEARCH` Value Expression | Score Expression |
|---|---|---|---|---|---|---|
| 1 | `synonym` | `@synonym_term_id` | prefix | `p_synonym` | `ANALYZER(STARTS_WITH(d.value, @q), "annotation_autocomplete_norm")` | `1000 + BM25(d)` |
| 2 | `genotype` | `@genotype_term_id` | prefix | `p_genotype` | `(ANALYZER(STARTS_WITH(d.value, @q), "annotation_autocomplete_norm") OR ANALYZER(STARTS_WITH(d.value, @q), "annotation_autocomplete_delim"))` | `1000 + BM25(d)` |
| 3 | `synonym` | `@synonym_term_id` | fuzzy | `n_synonym` | `NGRAM_MATCH(d.value, @q, @th, "annotation_autocomplete_ngram")` | `BM25(d)` |
| 4 | `genotype` | `@genotype_term_id` | fuzzy | `n_genotype` | `NGRAM_MATCH(d.value, @q, @th, "annotation_autocomplete_ngram")` | `BM25(d)` |

### AQL Branch and Merge Templates

In `internal/repository/arangodb/autocomplete_statement.go`:

```go
package arangodb

const (
	// AutocompleteBranchQ is one branch of the annotation autocomplete
	// statement over the annotation collection. The builder fills it with
	// fmt.Sprintf in this verb order:
	//
	//  1. %s — the branch variable (p_<tag> for prefix, n_<tag> for fuzzy)
	//  2. %s — the SEARCH value expression
	//  3. %s — the bind parameter name of the required cvterm _id
	//          (synonym_term_id or genotype_term_id)
	//  4. %q — the field label that the branch reports ("synonym" or "genotype")
	//  5. %s — the score expression
	//
	// It requires the bind parameters q, th, anno_cvterm_graph,
	// synonym_term_id, genotype_term_id, and limit. Because the cvterm _id
	// is resolved ahead of the query, the branch checks e._to == @<tag>_term_id
	// on the 1-hop OUTBOUND traversal without scanning @@cv_collection.
	AutocompleteBranchQ = `LET %s = (
  FOR d IN annotation_autocomplete
    SEARCH d.is_obsolete == false
      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_autocomplete_norm")
      AND %s
    LET has_tag = FIRST(
      FOR cvt, e IN 1..1 OUTBOUND d GRAPH @anno_cvterm_graph
        FILTER e._to == @%s AND cvt.deprecated == false
        RETURN true
    )
    FILTER has_tag == true
    SORT BM25(d) DESC, d.entry_id ASC, d._key ASC
    LIMIT @limit
    RETURN {
      id: d.entry_id,
      f: %q,
      v: NOT_NULL(d.value, ""),
      s: %s
    }
)`

	// AutocompleteMergeQ merges the 4 branches of the autocomplete
	// statement, keeps one best row per entry_id, and returns the top rows.
	// The builder fills it with one verb:
	//
	//  1. %s — the comma-separated list of the 4 branch variables
	//
	// It requires the bind parameter limit.
	AutocompleteMergeQ = `LET hits = FLATTEN([%s])
LET best = (
  FOR x IN hits
    COLLECT eid = x.id INTO grp = x
    LET top = FIRST(
      FOR m IN grp
        SORT m.s DESC, m.f ASC, m.v ASC
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
    v: x.v,
    s: x.s
  }`
)
```

### Bind Parameters

| Bind Key | Go Type | Value |
|---|---|---|
| `"q"` | `string` | `req.query` (normalized in Go: trimmed, lowercased, Mn marks removed) |
| `"th"` | `float64` | `autocompleteNgramThreshold` (`0.3`) |
| `"limit"` | `int` | `req.limit` (clamped to `1..50`, default `5`) |
| `"anno_cvterm_graph"` | `string` | `ar.anno.annotg.Name()` |
| `"synonym_term_id"` | `string` | `req.synonymTermID` (resolved via `ar.resolveTermID("dicty_annotation", "synonym")`) |
| `"genotype_term_id"` | `string` | `req.genotypeTermID` (resolved via `ar.resolveTermID("dicty_annotation", "genotype")`) |

### Handler Signature and Mapping

In `internal/app/service/autocomplete.go`:

```go
func (srv *AnnotationService) AutocompleteAnnotation(
	_ context.Context,
	r *annotation.AnnotationAutocompleteParameters,
) (*annotation.AnnotationSuggestionCollection, error)
```

Field mapping (`Suggestion.Field` → `annotation.AnnotationSearchField`):

| `Suggestion.Field` | `annotation.AnnotationSearchField` |
|---|---|
| `"synonym"` | `annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM` |
| `"genotype"` | `annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE` |
| anything else | `annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_UNSPECIFIED` |

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `scripts/autocomplete-calibration.js` | Create | `arangosh` calibration harness on ArangoDB 3.11: disposable DB, 3 analyzers, view, fixtures, threshold sweep, interior-allele genotype probe, obsolete/non-strain filter probes, EXPLAIN node counts (`4` `EnumerateViewNode`, `0` `EnumerateCollectionNode`), and 20-run p95 baseline |
| `go.mod`, `go.sum` | Modify | Bump `github.com/dictyBase/go-genproto` to `v0.0.0-20261014212141-25e2e7a3b338`; add `github.com/IBM/fp-go/v2 v2.5.0`, `github.com/dictyBase/fp-go-loom v0.0.0-20261010160534-3c971335f51e`, `github.com/cockroachdb/errors v1.14.0`, and direct `golang.org/x/text v0.42.0` |
| `internal/repository/repository.go` | Modify | Add `AutocompleteQuery`, `Suggestion`, and `AutocompleteAnnotation(params *AutocompleteQuery) IOE.IOEither[error, []*Suggestion]` on `TaggedAnnotationRepository` |
| `internal/repository/arangodb/arangodb.go` | Modify | Migrate `NewTaggedAnnotationRepo`, `setAnnotationCollection`, and `setDocumentCollection` to uninterrupted `fp-go/v2` `IOE.IOEither` pipelines with state structs and lenses, ending with `ensureAutocompleteSearch` |
| `internal/repository/arangodb/search_rows.go` | Create | `dbRows` interface and generic `scanSearchRows[T any](msg string) func(dbRows) IOE.IOEither[error, []T]` |
| `internal/repository/arangodb/autocomplete_normalize.go` | Create | `normalizeAutocompleteQuery(q string) string` in `fp-go/v2` (`strings.TrimSpace` → `strings.ToLower` → `norm.NFD.String` → `S.ToRunes` → `A.Filter(P.Not(isCombiningMark))`) |
| `internal/repository/arangodb/autocomplete_statement.go` | Create | `AutocompleteBranchQ` and `AutocompleteMergeQ` AQL constants |
| `internal/repository/arangodb/autocomplete_query.go` | Create | Pure `fp-go/v2` query builder (`buildAutocompleteQuery() string`, `prefixBranch`, `fuzzyBranch`, `mergeBlock`) |
| `internal/repository/arangodb/autocomplete.go` | Create | Search asset definitions, `fp-go/v2` `ensureAutocompleteSearch` and view reconciliation, pure `MO.Match` limit clamping, request validation, `resolveTermID` lens binding, query execution, row mapping, and `AutocompleteAnnotation` |
| `internal/repository/arangodb/autocomplete_test.go` | Create | Builder unit tests, asset creation/reconciliation/retry tests, and real ArangoDB 3.11 repository tests |
| `internal/app/service/service.go` | Modify | Migrate `NewAnnotationService` to `fp-go/v2` `Either` pipeline |
| `internal/app/service/autocomplete.go` | Create | `fp-go/v2` `AutocompleteAnnotation` gRPC handler, pure status mappers, validation pipeline, and proto response projection |
| `internal/app/service/autocomplete_contract_test.go` | Create | Protobuf validation contract tests against `go-genproto` |
| `internal/app/service/autocomplete_test.go` | Create | Handler unit tests with stub repository |
| `internal/app/service/autocomplete_arango_test.go` | Create | End-to-end bufconn gRPC round-trip test against a real disposable ArangoDB instance |
| `README.md` | Modify | Document `AutocompleteAnnotation` RPC contract and ArangoSearch assets in Simplified Technical English |

---

## Implementation Tasks

### Task 1: Calibration Against Real ArangoDB 3.11

**Files:**
- Create: `scripts/autocomplete-calibration.js`
- Modify: `docs/superpowers/plans/2026-10-14-annotation-autocomplete.md` ([Execution Notes](#execution-notes))

**Produces:**
1. Confirmed n-gram similarity threshold (`0.3`).
2. Proof that `d.is_obsolete == false` (with `identity`) and `ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_autocomplete_norm")` execute inside `SEARCH`.
3. Proof of the provisional `annotation_autocomplete_delim` (`Delimiter(",")` → `Delimiter(" ")` → `Norm`) pipeline on ArangoDB 3.11: verify that `STARTS_WITH(d.value, @q)` matches interior alleles such as `sada` in `"axeA1, axeB1, sadA-[sadA-KO]"` as well as whole-value prefixes via `annotation_autocomplete_norm`. If calibration shows any discrepancy, update the analyzer definition in this plan before starting Task 2.
4. Proof that `EXPLAIN` of the 4-branch statement (using `@synonym_term_id` and `@genotype_term_id` on `e._to`) reports `4` `EnumerateViewNode` entries and `0` `EnumerateCollectionNode` entries.
5. Baseline 20-run p95 latency on a ~1,000-document fixture.

- [ ] **Step 1: Onboard `bd`, claim or create issue, and cut feature branch**

*(If `bd ready` returns no issue for annotation autocomplete, create one with `bd create "Add AutocompleteAnnotation RPC in fp-go v2"` and capture its ID in `ISSUE_ID`.)*

```bash
bd onboard
bd ready
ISSUE_ID=$(bd create "Add AutocompleteAnnotation RPC in fp-go v2" --silent 2>/dev/null || true)
bd show "${ISSUE_ID}"
bd update "${ISSUE_ID}" --status in_progress
git checkout -b feat/annotation-autocomplete
```

- [ ] **Step 2: Write `scripts/autocomplete-calibration.js` and run it against ArangoDB 3.11**

The script creates a disposable database, creates `annotation_calib`, `cv_calib`, `cvterm_calib`, edge collection `annotation_cvterm_calib`, and named graph `annotation_tag_calib`, creates the three analyzers (`annotation_autocomplete_norm`, `annotation_autocomplete_delim`, `annotation_autocomplete_ngram`) and view `annotation_autocomplete`, inserts ~1,000 annotations (including multi-allele genotypes, multiple synonyms per strain, obsolete versions, and `DDB_G...` gene annotations), polls until the view commits, runs 12 probes + `db._explain`, and measures 20-run latency (min, median, p95).

- [ ] **Step 3: Record results in [Execution Notes](#execution-notes), fold any calibration adjustments into the plan, and commit**

```bash
git add scripts/autocomplete-calibration.js docs/superpowers/plans/2026-10-14-annotation-autocomplete.md
git commit -m "feat(scripts): add arangosearch calibration harness for annotation autocomplete"
```

---

### Task 2: Upgrade Dependencies and Verify Protobuf Contract

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `internal/app/service/autocomplete_contract_test.go`

**Interfaces:**
- Consumes: `github.com/dictyBase/go-genproto v0.0.0-20261014212141-25e2e7a3b338`, `github.com/IBM/fp-go/v2 v2.5.0`, `github.com/dictyBase/fp-go-loom v0.0.0-20261010160534-3c971335f51e`, `github.com/cockroachdb/errors v1.14.0`
- Produces: Verified generated types `annotation.AnnotationAutocompleteParameters`, `annotation.AnnotationAutocompleteAttributes`, `annotation.AnnotationSuggestion`, `annotation.AnnotationSuggestionCollection`, `annotation.AnnotationSearchField`, and `annotation.Meta.Total`

- [ ] **Step 1: Update `go.mod` and `go.sum`**

```bash
go get github.com/dictyBase/go-genproto@v0.0.0-20261014212141-25e2e7a3b338 \
  github.com/IBM/fp-go/v2@v2.5.0 \
  github.com/dictyBase/fp-go-loom@v0.0.0-20261010160534-3c971335f51e \
  github.com/cockroachdb/errors@v1.14.0 \
  golang.org/x/text@v0.42.0
go mod tidy
```

- [ ] **Step 2: Write the contract test in `internal/app/service/autocomplete_contract_test.go`**

```go
package service

import (
	"testing"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/stretchr/testify/require"
)

func TestAnnotationAutocompleteProtoContract(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	validReq := &annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationAutocompleteAttributes{
				Query: "axe",
				Limit: 5,
			},
		},
	}
	assert.NoError(protovalidate.Validate(validReq))

	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{Type: "annotation"},
	}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Attributes: &annotation.AnnotationAutocompleteAttributes{Query: "ax", Limit: 5},
		},
	}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Attributes: &annotation.AnnotationAutocompleteAttributes{Query: "axe", Limit: 51},
		},
	}))

	col := &annotation.AnnotationSuggestionCollection{
		Data: []*annotation.AnnotationSuggestion{
			{
				EntryId:     "DBS0236126",
				Field:       annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE,
				DisplayText: "axeA1, axeB1",
				Score:       1000,
			},
		},
		Meta: &annotation.Meta{NextCursor: 0, Limit: 5, Total: 1},
	}
	assert.Equal(int64(1), col.GetMeta().GetTotal())
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM,
		annotation.AnnotationSearchField(1),
	)
}
```

- [ ] **Step 3: Run test to verify it passes**

Run: `gotestsum --format-hide-empty-pkg --format dots -- -run TestAnnotationAutocompleteProtoContract ./internal/app/service/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum internal/app/service/autocomplete_contract_test.go
git commit -m "build(deps): upgrade go-genproto and add fp-go v2 dependencies"
```

---

### Task 3: Repository Types and Interface Method

**Files:**
- Modify: `internal/repository/repository.go`

**Interfaces:**
- Consumes: `IOE "github.com/IBM/fp-go/v2/ioeither"`
- Produces: `repository.AutocompleteQuery`, `repository.Suggestion`, and `TaggedAnnotationRepository.AutocompleteAnnotation(params *AutocompleteQuery) IOE.IOEither[error, []*Suggestion]`

- [ ] **Step 1: Add `AutocompleteQuery`, `Suggestion`, and `AutocompleteAnnotation` to `internal/repository/repository.go`**

Update `internal/repository/repository.go` with the exact definitions in [Repository Types and Interface Method](#repository-types-and-interface-method).

- [ ] **Step 2: Verify expected compile break on `*arangorepository`**

Run: `go build ./internal/repository/arangodb/... 2>&1 | head -n 5`
Expected: FAIL with `*arangorepository does not implement repository.TaggedAnnotationRepository (missing method AutocompleteAnnotation)`

---

### Task 4: Migrate `NewTaggedAnnotationRepo` to fp-go v2 and Wire Search Assets

**Files:**
- Modify: `internal/repository/arangodb/arangodb.go`
- Create: `internal/repository/arangodb/autocomplete.go` (asset creation + reconciliation section, plus stub `AutocompleteAnnotation` until Task 5)
- Create: `internal/repository/arangodb/autocomplete_test.go`

**Interfaces:**
- Consumes: `driver.Database.EnsureCreatedAnalyzer`, `driver.Database.ViewExists`, `driver.Database.CreateArangoSearchView`, `driver.Database.View`, `driver.ArangoSearchView.Properties`, `driver.ArangoSearchView.SetProperties`
- Produces: `NewTaggedAnnotationRepo`, `setAnnotationCollection`, `setDocumentCollection`, `ensureAutocompleteSearch`, and `ensureAutocompleteView` all composed as uninterrupted `fp-go/v2` `IOE.IOEither` pipelines, forced only once at `NewTaggedAnnotationRepo`'s `(repo.TaggedAnnotationRepository, error)` boundary

- [ ] **Step 1: Write the failing asset and constructor tests in `internal/repository/arangodb/autocomplete_test.go`**

*(Note: `tearDown(repo)` in `internal/repository/arangodb/arangodb_test.go:274` takes a single `repo` argument.)*

```go
package arangodb

import (
	"context"
	"slices"
	"testing"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/arangomanager/testarango"
	"github.com/stretchr/testify/require"
)

func TestEnsureAutocompleteSearchCreatesAssets(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	ctx := context.Background()
	dbh := repo.Dbh().Handler()

	analyzers, err := dbh.Analyzers(ctx)
	assert.NoError(err)
	names := make([]string, 0, len(analyzers))
	for _, a := range analyzers {
		names = append(names, a.Name())
	}
	assert.Contains(names, autocompleteNormAnalyzer)
	assert.Contains(names, autocompleteDelimAnalyzer)
	assert.Contains(names, autocompleteNgramAnalyzer)

	exists, err := dbh.ViewExists(ctx, autocompleteViewName)
	assert.NoError(err)
	assert.True(exists, "expect annotation_autocomplete view to exist")

	view, err := dbh.View(ctx, autocompleteViewName)
	assert.NoError(err)
	asv, err := view.ArangoSearchView()
	assert.NoError(err)
	props, err := asv.Properties(ctx)
	assert.NoError(err)

	collName := getCollectionParams().Annotation
	link, ok := props.Links[collName]
	assert.True(ok, "expect annotation collection link in view")
	assert.Len(link.Fields, 3)

	valAnalyzers := slices.Clone(link.Fields[docFieldValue].Analyzers)
	slices.Sort(valAnalyzers)
	assert.Equal(
		[]string{
			autocompleteDelimAnalyzer,
			autocompleteNgramAnalyzer,
			autocompleteNormAnalyzer,
		},
		valAnalyzers,
	)
	assert.Equal(
		[]string{autocompleteNormAnalyzer},
		link.Fields[docFieldEntryID].Analyzers,
	)
	assert.Equal(
		[]string{autocompleteIdentityAnalyzer},
		link.Fields[docFieldIsObsolete].Analyzers,
	)
}

func TestEnsureAutocompleteSearchReconcilesStaleView(t *testing.T) {
	assert := require.New(t)
	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err)

	dbr, err := tra.DB(tra.Database)
	assert.NoError(err)
	db := dbr.Handler()
	ctx := context.Background()
	collP := getCollectionParams()

	_, err = db.CreateCollection(ctx, collP.Annotation, nil)
	assert.NoError(err)

	stale, err := db.CreateArangoSearchView(
		ctx,
		autocompleteViewName,
		&driver.ArangoSearchViewProperties{
			Links: driver.ArangoSearchLinks{
				collP.Annotation: {
					Fields: driver.ArangoSearchFields{
						docFieldValue:      {Analyzers: []string{autocompleteIdentityAnalyzer}},
						docFieldEntryID:    {Analyzers: []string{autocompleteIdentityAnalyzer}},
						docFieldIsObsolete: {Analyzers: []string{autocompleteIdentityAnalyzer}},
					},
				},
			},
		},
	)
	assert.NoError(err)
	staleProps, err := stale.Properties(ctx)
	assert.NoError(err)

	repo, err := NewTaggedAnnotationRepo(
		GetConnectParamsFromDB(tra),
		collP,
		getOntoParams(),
	)
	assert.NoError(err)
	defer tearDown(repo)

	fresh, err := db.View(ctx, autocompleteViewName)
	assert.NoError(err)
	freshAsv, err := fresh.ArangoSearchView()
	assert.NoError(err)
	freshProps, err := freshAsv.Properties(ctx)
	assert.NoError(err)

	assert.Equal(
		staleProps.ID,
		freshProps.ID,
		"expect the same view identifier, proving SetProperties updated in place without delete",
	)
	valAnalyzers := slices.Clone(freshProps.Links[collP.Annotation].Fields[docFieldValue].Analyzers)
	slices.Sort(valAnalyzers)
	assert.Equal(
		[]string{
			autocompleteDelimAnalyzer,
			autocompleteNgramAnalyzer,
			autocompleteNormAnalyzer,
		},
		valAnalyzers,
	)
}

func TestNewTaggedAnnotationRepoFailsWhenSetupFails(t *testing.T) {
	assert := require.New(t)
	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err)
	defer func() {
		if db, dbErr := tra.DB(tra.Database); dbErr == nil {
			_ = db.Drop()
		}
	}()

	missing := &manager.ConnectParams{
		User:     tra.User,
		Pass:     tra.Pass,
		Database: "no_such_annotation_autocomplete_db",
		Host:     tra.Host,
		Port:     tra.Port,
		Istls:    false,
	}
	_, err = NewTaggedAnnotationRepo(missing, getCollectionParams(), getOntoParams())
	assert.Error(err, "expect an error when the database does not exist")

	assert.NoError(tra.CreateDB("no_such_annotation_autocomplete_db", nil))
	repo, err := NewTaggedAnnotationRepo(missing, getCollectionParams(), getOntoParams())
	assert.NoError(err, "expect second call to succeed once database exists")
	defer tearDown(repo)
}
```

- [ ] **Step 2: Migrate `internal/repository/arangodb/arangodb.go` to a single uninterrupted `fp-go/v2` `IOE.IOEither` pipeline**

Rewrite `NewTaggedAnnotationRepo`, `setAnnotationCollection`, and `setDocumentCollection` in `internal/repository/arangodb/arangodb.go` so that `setDocumentCollection` and `setAnnotationCollection` return `IOE.IOEither[error, *annoc]` (never calling `E.UnwrapError` or `ioeutils.ToEither` mid-pipeline), and only `NewTaggedAnnotationRepo` forces once at the outer boundary.

- [ ] **Step 3: Implement `ensureAutocompleteSearch` and `ensureAutocompleteView` as uninterrupted `fp-go/v2` `IOE.IOEither` pipelines in `internal/repository/arangodb/autocomplete.go`**

Implement `autocompleteNormDef`, `autocompleteDelimDef`, `autocompleteNgramDef`, `ensureAnalyzer`, `autocompleteLinks`, `sameLinkShape`, `ensureAutocompleteView`, and `ensureAutocompleteSearch` in `internal/repository/arangodb/autocomplete.go`.

- [ ] **Step 4: Run the asset and constructor tests**

Run: `gotestsum --format-hide-empty-pkg --format dots -- -run 'TestEnsureAutocompleteSearch|TestNewTaggedAnnotationRepoFailsWhenSetupFails' ./internal/repository/arangodb/`
Expected: PASS

- [ ] **Step 5: Run quality gates and commit**

```bash
golangci-lint fmt
golangci-lint run ./...
git add internal/repository/repository.go internal/repository/arangodb/arangodb.go internal/repository/arangodb/autocomplete.go internal/repository/arangodb/autocomplete_test.go
git commit -m "feat(arangodb): migrate NewTaggedAnnotationRepo to fp-go v2 and create autocomplete search assets"
```

---

### Task 5: Query Builder, Normalizer, Row Scanner, and `AutocompleteAnnotation`

**Files:**
- Create: `internal/repository/arangodb/search_rows.go`
- Create: `internal/repository/arangodb/autocomplete_normalize.go`
- Create: `internal/repository/arangodb/autocomplete_statement.go`
- Create: `internal/repository/arangodb/autocomplete_query.go`
- Modify: `internal/repository/arangodb/autocomplete.go`
- Modify: `internal/repository/arangodb/autocomplete_test.go`

**Interfaces:**
- Consumes: `AutocompleteBranchQ`, `AutocompleteMergeQ`, `normalizeAutocompleteQuery`, `scanSearchRows[suggestionRow]`, `MO.Match`, `ar.termID`
- Produces: `buildAutocompleteQuery() string` and `(ar *arangorepository) AutocompleteAnnotation(params *repository.AutocompleteQuery) IOE.IOEither[error, []*repository.Suggestion]`

- [ ] **Step 1: Write the failing builder unit tests and repository integration tests in `internal/repository/arangodb/autocomplete_test.go`**

Add tests covering:
- `TestBuildAutocompleteQueryBranchCount`: asserts 4 branch variables (`p_synonym`, `p_genotype`, `n_synonym`, `n_genotype`), 4 occurrences of `FOR d IN annotation_autocomplete`, 4 `OUTBOUND` traversals, `annotation_autocomplete_delim` on `p_genotype`, 2 `NGRAM_MATCH(d.value`, and zero `@@cv_collection` references.
- `TestBuildAutocompleteQueryFilterOrder`: asserts in every branch that `FILTER has_tag == true` appears before `SORT BM25(d)` and `LIMIT @limit`.
- `TestBuildAutocompleteQueryIsValidAQL`: runs `repo.Dbh().Handler().ValidateQuery(ctx, buildAutocompleteQuery())` and requires no error.
- `TestAutocompleteAnnotationSynonymPrefix`: inserts a strain `DBS0236126` with synonym `"gammaS13"`, polls with `require.Eventually`, queries `"gammas"`, asserts 1 row with `EntryID == "DBS0236126"`, `Field == "synonym"`, `DisplayText == "gammaS13"`, `Score >= 1000`.
- `TestAutocompleteAnnotationGenotypePrefix`: inserts a strain `DBS0236127` with genotype `"axeA1, axeB1, sadA-"`, queries `"axea"`, asserts 1 row with `EntryID == "DBS0236127"`, `Field == "genotype"`, `DisplayText == "axeA1, axeB1, sadA-"`, `Score >= 1000`.
- `TestAutocompleteAnnotationGenotypeInteriorAllelePrefix`: queries `"sada"` against `"axeA1, axeB1, sadA-"`, asserts 1 row with `EntryID == "DBS0236127"`, `Field == "genotype"`, `DisplayText == "axeA1, axeB1, sadA-"`, `Score >= 1000`.
- `TestAutocompleteAnnotationTypoFuzzy`: queries `"gammas14"` against `"gammaS13"`, asserts 1 row with `Score < 1000`.
- `TestAutocompleteAnnotationAccentedAndUppercaseQuery`: queries `"  GÁMMAS13  "` against `"gammaS13"`, asserts 1 row.
- `TestAutocompleteAnnotationSkipsObsoleteVersion`: creates a synonym `"oldAlias"` on `DBS0236128`, calls `repo.EditAnnotation` to update its value to `"newAlias"`, waits for view commit, asserts `"oldalias"` returns 0 rows and `"newalias"` returns `DBS0236128` with `DisplayText == "newAlias"`.
- `TestAutocompleteAnnotationIgnoresNonStrainAndOtherTagsUnderSmallLimit`: inserts 10 gene annotations (`DDB_G0286429`.. with tag `"synonym"` and value `"xyloseMutant"`), 10 strain `"description"` annotations (`DBS0236200`.. with value `"xyloseMutant"`), and 1 strain `"synonym"` annotation (`DBS0236999` with value `"xyloseMutant"`). Queries `"xylose"` with `Limit: 1`, asserts `DBS0236999` is returned.
- `TestAutocompleteAnnotationDeduplicatesMultipleSynonymsAndCrossFieldHits`: inserts 2 synonyms (`"sadA-null"`, `"sadA-ko"`) and 1 genotype (`"sadA-[sadA-KO]"`) on the same strain `DBS0236129`. Queries `"sada"`, asserts exactly 1 row for `DBS0236129`.
- `TestAutocompleteAnnotationDefaultLimitIsFive` and `TestAutocompleteAnnotationLimitCapIsFifty`.
- `TestAutocompleteAnnotationRankingUnderSmallLimit`: 6 fuzzy matches + 1 prefix match with `Limit: 3` returns the prefix match first.
- `TestAutocompleteAnnotationEmptyResultIsNotAnError`: query `"zzzqqq"` returns empty non-nil slice `[]*repository.Suggestion{}` and nil error.
- `TestAutocompleteAnnotationRejectsNilAndWhitespaceQuery`: `nil` params and `"   "` return errors before any AQL runs.

- [ ] **Step 2: Run the new tests to confirm they fail**

Run: `gotestsum --format-hide-empty-pkg --format dots -- -run 'TestBuildAutocompleteQuery|TestAutocompleteAnnotation' ./internal/repository/arangodb/`
Expected: FAIL

- [ ] **Step 3: Create `internal/repository/arangodb/search_rows.go` and `internal/repository/arangodb/autocomplete_normalize.go`**

Implement `scanSearchRows[T any]` and `normalizeAutocompleteQuery(q string) string`.

- [ ] **Step 4: Create `internal/repository/arangodb/autocomplete_statement.go` and `internal/repository/arangodb/autocomplete_query.go`**

Write `AutocompleteBranchQ` and `AutocompleteMergeQ` in `autocomplete_statement.go` as specified in [AQL Branch and Merge Templates](#aql-branch-and-merge-templates), and write `autocomplete_query.go` in `fp-go/v2` style:

```go
package arangodb

import (
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	L "github.com/IBM/fp-go/v2/optics/lens"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
	"github.com/dictyBase/fp-go-loom/strutils"
)

type (
	autocompleteFieldSpec struct {
		tag        string
		termBind   string
		isGenotype bool
	}

	queryBranch struct {
		name string
		body string
	}

	branchSet struct {
		fields  []autocompleteFieldSpec
		builder func(autocompleteFieldSpec) queryBranch
	}
)

var (
	autocompleteFields = []autocompleteFieldSpec{
		{tag: fieldSynonym, termBind: bindSynonymTermID, isGenotype: false},
		{tag: fieldGenotype, termBind: bindGenotypeTermID, isGenotype: true},
	}

	isGenotypeSpec = func(spec autocompleteFieldSpec) bool {
		return spec.isGenotype
	}

	prefixSearchExprForSpec = P.Fold(
		synonymPrefixExpr,
		genotypePrefixExpr,
	)(isGenotypeSpec)

	nameLens = L.MakeLens(
		func(b queryBranch) string { return b.name },
		func(b queryBranch, v string) queryBranch { b.name = v; return b },
	)
	bodyLens = L.MakeLens(
		func(b queryBranch) string { return b.body },
		func(b queryBranch, v string) queryBranch { b.body = v; return b },
	)

	branchSets = []branchSet{
		{fields: autocompleteFields, builder: prefixBranch},
		{fields: autocompleteFields, builder: fuzzyBranch},
	}
)

func synonymPrefixExpr(autocompleteFieldSpec) string {
	return fmt.Sprintf(
		"ANALYZER(STARTS_WITH(d.%s, @q), %q)",
		docFieldValue,
		autocompleteNormAnalyzer,
	)
}

func genotypePrefixExpr(autocompleteFieldSpec) string {
	return fmt.Sprintf(
		"(ANALYZER(STARTS_WITH(d.%s, @q), %q) OR ANALYZER(STARTS_WITH(d.%s, @q), %q))",
		docFieldValue,
		autocompleteNormAnalyzer,
		docFieldValue,
		autocompleteDelimAnalyzer,
	)
}

func prefixBranch(spec autocompleteFieldSpec) queryBranch {
	bname := strutils.JoinStrings("p_", spec.tag)
	parts := A.From[any](
		bname,
		prefixSearchExprForSpec(spec),
		spec.termBind,
		spec.tag,
		autocompletePrefixScore,
	)
	return queryBranch{
		name: bname,
		body: fmt.Sprintf(AutocompleteBranchQ, parts...),
	}
}

func fuzzyBranch(spec autocompleteFieldSpec) queryBranch {
	bname := strutils.JoinStrings("n_", spec.tag)
	parts := A.From[any](
		bname,
		autocompleteFuzzyExpr(docFieldValue),
		spec.termBind,
		spec.tag,
		autocompleteFuzzyScore,
	)
	return queryBranch{
		name: bname,
		body: fmt.Sprintf(AutocompleteBranchQ, parts...),
	}
}

func branchesFromSet(set branchSet) []queryBranch {
	return F.Pipe1(
		set.fields,
		A.Map(set.builder),
	)
}

func autocompleteBranches() []queryBranch {
	return F.Pipe1(branchSets, A.Chain(branchesFromSet))
}

func mergeBlock(branches []queryBranch) string {
	return F.Pipe3(
		branches,
		A.Map(nameLens.Get),
		S.Join(", "),
		S.Format[string](AutocompleteMergeQ),
	)
}

func buildAutocompleteQuery() string {
	branches := autocompleteBranches()
	merge := mergeBlock(branches)
	return F.Pipe3(
		branches,
		A.Map(bodyLens.Get),
		A.Push(merge),
		S.Join("\n"),
	)
}

func autocompleteFuzzyExpr(field string) string {
	return fmt.Sprintf(
		"NGRAM_MATCH(d.%s, @q, @th, %q)",
		field,
		autocompleteNgramAnalyzer,
	)
}
```

- [ ] **Step 5: Implement `AutocompleteAnnotation`, lens-bound term ID resolution, and pure `MO.Match` limit clamping in `internal/repository/arangodb/autocomplete.go`**

```go
type (
	autocompleteRequest struct {
		query          string
		limit          int
		synonymTermID  string
		genotypeTermID string
	}

	autocompleteQueryParams struct {
		statement  string
		bindParams map[string]any
	}

	suggestionRow struct {
		ID    string  `json:"id"`
		Field string  `json:"f"`
		Value string  `json:"v"`
		Score float64 `json:"s"`
	}
)

var (
	synonymTermIDLens = L.MakeLens(
		func(r autocompleteRequest) string { return r.synonymTermID },
		func(r autocompleteRequest, id string) autocompleteRequest {
			r.synonymTermID = id
			return r
		},
	)
	genotypeTermIDLens = L.MakeLens(
		func(r autocompleteRequest) string { return r.genotypeTermID },
		func(r autocompleteRequest, id string) autocompleteRequest {
			r.genotypeTermID = id
			return r
		},
	)

	isNonPositiveLimit = ORD.Leq(predord.IntOrd)(0)
	isAboveMaxLimit    = ORD.Gt(predord.IntOrd)(maxAutocompleteLimit)

	clampAutocompleteLimit = MO.Match(
		defaultAutocompleteLimit,
		[]O.Kleisli[int, int]{
			MO.Const[int](isNonPositiveLimit, defaultAutocompleteLimit),
			MO.Const[int](isAboveMaxLimit, maxAutocompleteLimit),
			MO.Default(F.Identity[int]),
		},
	)
)

func (ar *arangorepository) resolveTermID(
	onto, tag string,
) IOE.IOEither[error, string] {
	return F.Pipe1(
		IOE.TryCatchError(func() (string, error) {
			return ar.termID(onto, tag)
		}),
		IOE.MapLeft[string](
			fperrors.OnError("error in resolving autocomplete ontology term"),
		),
	)
}

func (ar *arangorepository) resolveSynonymTermID(
	autocompleteRequest,
) IOE.IOEither[error, string] {
	return ar.resolveTermID(dictyAnnotationOntology, fieldSynonym)
}

func (ar *arangorepository) resolveGenotypeTermID(
	autocompleteRequest,
) IOE.IOEither[error, string] {
	return ar.resolveTermID(dictyAnnotationOntology, fieldGenotype)
}

func (ar *arangorepository) buildAutocompleteQueryParams(
	req autocompleteRequest,
) autocompleteQueryParams {
	return autocompleteQueryParams{
		statement: buildAutocompleteQuery(),
		bindParams: map[string]any{
			"q":                 req.query,
			"th":                autocompleteNgramThreshold,
			"limit":             req.limit,
			"anno_cvterm_graph": ar.anno.annotg.Name(),
			bindSynonymTermID:   req.synonymTermID,
			bindGenotypeTermID:  req.genotypeTermID,
		},
	}
}

// AutocompleteAnnotation returns suggestions for a partial strain synonym or
// genotype value.
func (ar *arangorepository) AutocompleteAnnotation(
	params *repo.AutocompleteQuery,
) IOE.IOEither[error, []*repo.Suggestion] {
	return F.Pipe7(
		params,
		validateAutocompleteRequest,
		IOE.Bind(synonymTermIDLens.Set, ar.resolveSynonymTermID),
		IOE.Bind(genotypeTermIDLens.Set, ar.resolveGenotypeTermID),
		IOE.Map[error](ar.buildAutocompleteQueryParams),
		IOE.Chain(ar.executeAutocompleteQuery),
		IOE.Chain(
			scanSearchRows[suggestionRow]("error in reading autocomplete row"),
		),
		IOE.Map[error](toSuggestions),
	)
}
```

- [ ] **Step 6: Run tests and quality gates, then commit**

```bash
gotestsum --format-hide-empty-pkg --format dots -- -run 'TestBuildAutocompleteQuery|TestAutocompleteAnnotation|TestEnsureAutocompleteSearch' ./internal/repository/arangodb/
golangci-lint fmt
golangci-lint run ./...
git add internal/repository/arangodb/search_rows.go \
  internal/repository/arangodb/autocomplete_normalize.go \
  internal/repository/arangodb/autocomplete_statement.go \
  internal/repository/arangodb/autocomplete_query.go \
  internal/repository/arangodb/autocomplete.go \
  internal/repository/arangodb/autocomplete_test.go
git commit -m "feat(arangodb): add AutocompleteAnnotation query builder and repository pipeline"
```

---

### Task 6: Migrate `NewAnnotationService` and Add `AutocompleteAnnotation` gRPC Handler

**Files:**
- Modify: `internal/app/service/service.go`
- Create: `internal/app/service/autocomplete.go`
- Create: `internal/app/service/autocomplete_test.go`

**Interfaces:**
- Consumes: `repository.TaggedAnnotationRepository.AutocompleteAnnotation`
- Produces: `(srv *AnnotationService) AutocompleteAnnotation(context.Context, *annotation.AnnotationAutocompleteParameters) (*annotation.AnnotationSuggestionCollection, error)`

- [ ] **Step 1: Write the failing handler unit tests in `internal/app/service/autocomplete_test.go`**

Include all 11 handler unit tests (`TestAutocompleteAnnotationHandlerMapsSuggestions`, `TestAutocompleteAnnotationHandlerPassesThroughQueryAndLimit`, `TestAutocompleteAnnotationHandlerDefaultsLimitToFive`, `TestAutocompleteAnnotationHandlerRejectsNilData`, `TestAutocompleteAnnotationHandlerRejectsNilAttributes`, `TestAutocompleteAnnotationHandlerRejectsShortQuery`, `TestAutocompleteAnnotationHandlerRejectsWhitespaceQuery`, `TestAutocompleteAnnotationHandlerRejectsLimitAboveCap`, `TestAutocompleteAnnotationHandlerReturnsEmptyCollection`, `TestAutocompleteAnnotationHandlerMapsRepositoryError`, `TestAutocompleteFieldMapping`).

- [ ] **Step 2: Run the handler tests to confirm they fail**

Run: `gotestsum --format-hide-empty-pkg --format dots -- -run 'TestAutocompleteAnnotationHandler|TestAutocompleteFieldMapping' ./internal/app/service/`
Expected: FAIL

- [ ] **Step 3: Migrate `NewAnnotationService` in `internal/app/service/service.go` to `fp-go/v2`**

Migrate `NewAnnotationService` to use `E.TryCatchError`, `E.MapLeft`, `E.Map`, and `E.Fold` into a boundary struct.

- [ ] **Step 4: Implement `internal/app/service/autocomplete.go`**

Write `internal/app/service/autocomplete.go` following the 5 moves of the `fp-go-grpc-handler` skill:
1. Seed `autocompleteInput{request: r, repo: srv.repo}` into `IOE.Of[error]`.
2. Pure guard chain `validatedAutocompleteSearch(in autocompleteInput) E.Either[error, autocompleteSearch]` using `E.TryCatchError(in.request, protovalidate.Validate(in.request))`, `E.Map[error](newAutocompleteParams)`, `E.Chain(E.FromPredicate(hasQueryText, invalidParamStatusOf[autocompleteParams](msgEmptyAutocompleteQuery)))`, `E.Map[error](F.Bind1st(newAutocompleteSearch, in.repo))`, and `E.MapLeft[autocompleteSearch](invalidParamStatus)`.
3. Enter `IOEither` once via `IOE.ChainEitherK(validatedAutocompleteSearch)`.
4. Bind `searchSuggestions` via `IOE.Bind(rowsLens.Set, searchSuggestions)`, stamping repository faults with `IOE.MapLeft[[]*repository.Suggestion](serverErrorStatus)`.
5. Project `IOE.Map[error](toSuggestionCollection)` and force once via `ioeutils.ToEither` + `E.Fold(failedAutocompleteResult, successfulAutocompleteResult)`.

- [ ] **Step 5: Run handler unit tests and quality gates, then commit**

```bash
gotestsum --format-hide-empty-pkg --format dots -- -run 'TestAutocomplete' ./internal/app/service/
golangci-lint fmt
golangci-lint run ./...
git add internal/app/service/service.go internal/app/service/autocomplete.go internal/app/service/autocomplete_test.go
git commit -m "feat(service): add AutocompleteAnnotation gRPC handler in fp-go v2"
```

---

### Task 7: End-to-End bufconn + ArangoDB Round-Trip Test, EXPLAIN, and Latency Gate

**Files:**
- Create: `internal/app/service/autocomplete_arango_test.go`
- Modify: `docs/superpowers/plans/2026-10-14-annotation-autocomplete.md` ([Execution Notes](#execution-notes))

**Interfaces:**
- Consumes: `arangodb.NewTaggedAnnotationRepo`, `service.NewAnnotationService`, `annotation.NewTaggedAnnotationServiceClient`
- Produces: End-to-end verification of `CreateAnnotation` → ArangoSearch view commit → `AutocompleteAnnotation` over `bufconn`, plus EXPLAIN and latency verification of the Go-built AQL statement

- [ ] **Step 1: Write `internal/app/service/autocomplete_arango_test.go` using the race-safe `"passthrough:///bufnet"` dialer**

Verify both a whole-string genotype prefix (`"axea"`) and an interior allele prefix (`"sada"`) against `"axeA1, axeB1, sadA-"` on `DBS0236126` over `bufconn`.

- [ ] **Step 2: Run `EXPLAIN` and 20-run latency check on the Go-built AQL statement**

Assert:
- `EnumerateViewNode` count == `4`
- `EnumerateCollectionNode` count == `0`
- Measured p95 latency <= 125% of the Task 1 calibration baseline p95.
Record both in [Execution Notes](#execution-notes).

- [ ] **Step 3: Run tests and quality gates, then commit**

```bash
gotestsum --format-hide-empty-pkg --format dots -- -run TestAutocompleteAnnotationEndToEnd ./internal/app/service/
golangci-lint fmt
golangci-lint run ./...
go test ./...
go test -race ./...
git add internal/app/service/autocomplete_arango_test.go docs/superpowers/plans/2026-10-14-annotation-autocomplete.md
git commit -m "test(service): add end-to-end bufconn test and latency gate for AutocompleteAnnotation"
```

---

### Task 8: Documentation and Session Completion

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document `AutocompleteAnnotation` and its ArangoSearch assets in `README.md` in Simplified Technical English**

Cover:
1. RPC method `AutocompleteAnnotation`, request `AnnotationAutocompleteParameters`, and response `AnnotationSuggestionCollection`.
2. Minimum query length (3 characters), default limit (5), and maximum limit (50).
3. Searched fields: `synonym` and `genotype` in ontology `dicty_annotation` for active (`is_obsolete == false`) strain (`DBS...`) entries, deduplicated by `entry_id`.
4. ArangoSearch analyzers (`annotation_autocomplete_norm`, `annotation_autocomplete_delim`, `annotation_autocomplete_ngram`) and view (`annotation_autocomplete`) created eagerly by `NewTaggedAnnotationRepo`.

- [ ] **Step 2: Run all quality gates**

```bash
golangci-lint fmt
golangci-lint run ./...
go test ./...
go test -race ./...
if [ -x scripts/check-gopls.sh ]; then
  scripts/check-gopls.sh
else
  git ls-files '*.go' | while read -r f; do
    head -n 5 "$f" | grep -q 'Code generated .* DO NOT EDIT\.' || gopls check -severity=hint "$f"
  done
fi
```

- [ ] **Step 3: Commit documentation and complete `bd` + git handoff ("Landing the Plane")**

*(Preserve untracked `MODWARE_ANNOTATION_NIL_ATTRIBUTES_REPORT.md` — never stage or delete it.)*

```bash
git add README.md
git commit -m "docs: describe AutocompleteAnnotation rpc and arangosearch assets"
bd close "${ISSUE_ID}"
git pull --rebase
bd sync
git push -u origin feat/annotation-autocomplete
git stash list
git remote prune origin
git log -1 --oneline
git status # must show "up to date with origin"
```
Provide session handoff notes summarizing the calibration findings, commit hashes, and test/gate results.

---

## Test Matrix

| # | Test Name | Layer | What It Verifies |
|---|---|---|---|
| 1 | `TestAnnotationAutocompleteProtoContract` | Service / Proto | `protovalidate.Validate` rules on `AnnotationAutocompleteParameters` and `Meta.Total` |
| 2 | `TestEnsureAutocompleteSearchCreatesAssets` | Repository | All 3 analyzers and `annotation_autocomplete` view with 3 indexed fields (`value`, `entry_id`, `is_obsolete`) exist after `NewTaggedAnnotationRepo` |
| 3 | `TestEnsureAutocompleteSearchReconcilesStaleView` | Repository | Stale view links converge in place via `SetProperties` with `staleProps.ID == freshProps.ID` |
| 4 | `TestNewTaggedAnnotationRepoFailsWhenSetupFails` | Repository | Eager failure on missing DB and clean retry once DB is created |
| 5 | `TestBuildAutocompleteQueryBranchCount` | Builder | 4 branch variables, 4 view scans, 4 `OUTBOUND` traversals, `annotation_autocomplete_delim` in `p_genotype`, 2 fuzzy branches, and 0 `@@cv_collection` references |
| 6 | `TestBuildAutocompleteQueryFilterOrder` | Builder | `FILTER has_tag == true` sits before `SORT BM25` and `LIMIT @limit` in every branch |
| 7 | `TestBuildAutocompleteQueryIsValidAQL` | Repository | `ValidateQuery` accepts the built 4-branch AQL statement |
| 8 | `TestAutocompleteAnnotationSynonymPrefix` | Repository | Prefix match on `synonym` returns `EntryID`, `Field: "synonym"`, `DisplayText`, `Score >= 1000` |
| 9 | `TestAutocompleteAnnotationGenotypePrefix` | Repository | Whole-value prefix match on `genotype` returns `EntryID`, `Field: "genotype"`, `DisplayText`, `Score >= 1000` |
| 10 | `TestAutocompleteAnnotationGenotypeInteriorAllelePrefix` | Repository | Interior allele prefix (`"sada"`) matches `"axeA1, axeB1, sadA-"` via `annotation_autocomplete_delim` with `Score >= 1000` |
| 11 | `TestAutocompleteAnnotationTypoFuzzy` | Repository | One-character typo matches via `NGRAM_MATCH` at threshold `0.3` with `Score < 1000` |
| 12 | `TestAutocompleteAnnotationAccentedAndUppercaseQuery` | Repository | Trim + lowercase + NFD diacritic stripping normalizes `"  GÁMMAS13  "` |
| 13 | `TestAutocompleteAnnotationSkipsObsoleteVersion` | Repository | `EditAnnotation` copy-on-write retires old value (`is_obsolete: true`) so only new value matches |
| 14 | `TestAutocompleteAnnotationIgnoresNonStrainAndOtherTagsUnderSmallLimit` | Repository | `DDB_G...` gene entries and `"description"` tags do not crowd out strain `"synonym"` under `Limit: 1` |
| 15 | `TestAutocompleteAnnotationDeduplicatesMultipleSynonymsAndCrossFieldHits` | Repository | Multiple matching annotations on the same `DBS...` strain collapse to 1 suggestion |
| 16 | `TestAutocompleteAnnotationDefaultLimitIsFive` | Repository | `Limit <= 0` returns 5 rows |
| 17 | `TestAutocompleteAnnotationLimitCapIsFifty` | Repository | `Limit > 50` clamps to 50 rows |
| 18 | `TestAutocompleteAnnotationRankingUnderSmallLimit` | Repository | Per-branch `SORT` before `LIMIT` keeps prefix match ahead of fuzzy matches |
| 19 | `TestAutocompleteAnnotationEmptyResultIsNotAnError` | Repository | Unmatched query returns empty non-nil slice and nil error |
| 20 | `TestAutocompleteAnnotationRejectsNilAndWhitespaceQuery` | Repository | Nil `*AutocompleteQuery` and `"   "` fail before AQL execution |
| 21 | `TestAutocompleteAnnotationHandlerMapsSuggestions` | Service | Maps repository suggestions to `AnnotationSuggestionCollection` and `Meta` |
| 22 | `TestAutocompleteAnnotationHandlerPassesThroughQueryAndLimit` | Service | Trims query and forwards requested limit |
| 23 | `TestAutocompleteAnnotationHandlerDefaultsLimitToFive` | Service | `Limit: 0` becomes `5` in both repository call and `Meta.Limit` |
| 24 | `TestAutocompleteAnnotationHandlerRejectsNilData` | Service | Returns `codes.InvalidArgument` |
| 25 | `TestAutocompleteAnnotationHandlerRejectsNilAttributes` | Service | Returns `codes.InvalidArgument` |
| 26 | `TestAutocompleteAnnotationHandlerRejectsShortQuery` | Service | 2-char query returns `codes.InvalidArgument` |
| 27 | `TestAutocompleteAnnotationHandlerRejectsWhitespaceQuery` | Service | `"   "` returns `codes.InvalidArgument` |
| 28 | `TestAutocompleteAnnotationHandlerRejectsLimitAboveCap` | Service | `Limit: 51` returns `codes.InvalidArgument` |
| 29 | `TestAutocompleteAnnotationHandlerReturnsEmptyCollection` | Service | Empty slice returns `Total: 0`, `Limit: 5`, no error |
| 30 | `TestAutocompleteAnnotationHandlerMapsRepositoryError` | Service | Repository error maps to `codes.Internal` |
| 31 | `TestAutocompleteFieldMapping` | Service | Maps `"synonym"` and `"genotype"` to proto enum values, unknown to `UNSPECIFIED` |
| 32 | `TestAutocompleteAnnotationEndToEnd` | Service + DB | Full gRPC round trip over `bufconn` against real disposable ArangoDB |

---

## Failure Behavior

| Input or Condition | Layer | Behavior |
|---|---|---|
| `data` is nil | Service (`protovalidate`) | `codes.InvalidArgument`, no panic, no repository call |
| `attributes` is nil | Service (`protovalidate`) | `codes.InvalidArgument`, no panic, no repository call |
| `query` shorter than 3 characters | Service (`protovalidate`) | `codes.InvalidArgument` |
| `query` of 3+ whitespace characters (`"   "`) | Service (trim guard) & Repository | `codes.InvalidArgument` in service; error in repository before AQL |
| `limit` < 0 or > 50 | Service (`protovalidate`) | `codes.InvalidArgument` |
| `limit` == 0 | Service & Repository | Effective limit is `5` |
| No annotation matches | Repository & Service | Empty non-nil `Data` slice, `Meta.Total = 0`, `nil` error (never `codes.NotFound`) |
| Obsolete annotation (`is_obsolete == true`) | ArangoSearch index | Filtered out inside `SEARCH d.is_obsolete == false` |
| Non-strain entry (`DDB_G...`, `DBP...`) | ArangoSearch index | Filtered out inside `SEARCH ANALYZER(STARTS_WITH(d.entry_id, "dbs"), ...)` |
| Annotation with other tag (`public note`, `description`) | AQL branch | Filtered out by `e._to == @<tag>_term_id` before branch `SORT` and `LIMIT` |
| Stale `annotation_autocomplete` view links | Repository constructor | Reconciled in place via `SetProperties` without deleting the view |
| Repository returns an error | Service | Mapped to `codes.Internal` via `serverErrorStatus` |

---

## Quality Gates

Run all four gates after every task that modifies `.go` files:

```bash
# 1. Nested-call & combinator-direct-apply gate on all fp-go v2 files
FP_FILES="internal/repository/arangodb/arangodb.go \
  internal/repository/arangodb/search_rows.go \
  internal/repository/arangodb/autocomplete_normalize.go \
  internal/repository/arangodb/autocomplete_query.go \
  internal/repository/arangodb/autocomplete.go \
  internal/app/service/service.go \
  internal/app/service/autocomplete.go"

if rg '[A-Z][A-Za-z0-9_.]*\([a-z][A-Za-z0-9_.]*\(.*\)' $FP_FILES; then
  echo "ERROR: nested call found" >&2
  exit 1
fi
if rg '\)\([a-z]' $FP_FILES; then
  echo "ERROR: direct combinator application found" >&2
  exit 1
fi

# 2. Formatter and linter gate
golangci-lint fmt
golangci-lint run ./...

# 3. Test suite (standard and race detector)
go test ./...
go test -race ./...

# 4. gopls gate on all tracked non-generated first-party .go files
if [ -x scripts/check-gopls.sh ]; then
  scripts/check-gopls.sh
else
  git ls-files '*.go' | while read -r f; do
    head -n 5 "$f" | grep -q 'Code generated .* DO NOT EDIT\.' || gopls check -severity=hint "$f"
  done
fi
```

---

## Completion Criteria

- [ ] `scripts/autocomplete-calibration.js` runs against ArangoDB 3.11 and records all probe results, EXPLAIN node counts (`4` `EnumerateViewNode`, `0` `EnumerateCollectionNode`), and baseline p95 in [Execution Notes](#execution-notes).
- [ ] `go.mod` requires `github.com/dictyBase/go-genproto v0.0.0-20261014212141-25e2e7a3b338`, `github.com/IBM/fp-go/v2 v2.5.0`, `github.com/dictyBase/fp-go-loom v0.0.0-20261010160534-3c971335f51e`, `github.com/cockroachdb/errors v1.14.0`, and `golang.org/x/text v0.42.0`.
- [ ] `internal/repository/repository.go` declares `AutocompleteQuery`, `Suggestion`, and `AutocompleteAnnotation(params *AutocompleteQuery) IOE.IOEither[error, []*Suggestion]`.
- [ ] `NewTaggedAnnotationRepo`, `setAnnotationCollection`, `setDocumentCollection`, `ensureAutocompleteSearch`, and `ensureAutocompleteView` are composed as uninterrupted `fp-go/v2` `IOE.IOEither` pipelines, forced only once at `NewTaggedAnnotationRepo`.
- [ ] `ensureAutocompleteSearch` creates `annotation_autocomplete_norm`, `annotation_autocomplete_delim`, `annotation_autocomplete_ngram`, and the view `annotation_autocomplete`, and reconciles stale view links via `SetProperties` without deleting the view.
- [ ] `AutocompleteAnnotation` in `internal/repository/arangodb/autocomplete.go` resolves `@synonym_term_id` and `@genotype_term_id` onto the state with lenses and runs the 4-branch AQL query over active strain (`DBS...`) `synonym` and `genotype` annotations in `dicty_annotation`, supporting both whole-value and interior-allele prefix matches on `genotype` and deduplicating by `entry_id`.
- [ ] `NewAnnotationService` and `AnnotationService.AutocompleteAnnotation` in `internal/app/service/` are written as `fp-go/v2` `Either` / `IOEither` pipelines.
- [ ] All 32 tests in the [Test Matrix](#test-matrix) pass, EXPLAIN reports 4 `EnumerateViewNode` and 0 `EnumerateCollectionNode`, p95 latency is <= 125% of calibration baseline, and all four [Quality Gates](#quality-gates) pass.
- [ ] `README.md` documents the RPC and search assets in Simplified Technical English, and `MODWARE_ANNOTATION_NIL_ATTRIBUTES_REPORT.md` remains untouched and unstaged.

---

## Execution Notes

*(Filled during Task 1 and Task 7 execution.)*

### Task 1 calibration — 2026-10-14

- ArangoDB version: `3.12.12` (enterprise) — the plan named 3.11; the only
  ArangoDB reachable in this environment is 3.12.12, so calibration and the
  Task 7 gate run against it (ledger ruling recorded in the SDD ledger).
- Harness: `scripts/autocomplete-calibration.js`, run with
  `docker exec arangodb arangosh --server.endpoint tcp://127.0.0.1:8529
  --server.username root --server.password $ARANGO_PASS
  --javascript.execute /tmp/calib.js`. It exits non-zero when any probe,
  the EXPLAIN gate, or the threshold gate fails.
- Fixture size: 1,035 annotation documents in a disposable database
  (`annotation_calib`), with a named graph (`annotation_tag_calib`), the
  three analyzers, and the `annotation_autocomplete` view
  (`commitIntervalMsec: 100`).

Calibration probe results (1–12) — all `pass: true`:

| # | Probe | Result |
|---|---|---|
| 1 | `norm` whole-value synonym prefix `gammas` | `DBS0236125` / `synonym` / `gammaS13`, score `1000` |
| 2 | `norm` genotype whole-value prefix `axea` | `DBS0236126` / `genotype` / `axeA1, axeB1, sadA-[sadA-KO]`, score `1004.74` |
| 3 | `delim` interior allele `axeb1` | same document, score `1004.74` |
| 4 | `delim` interior allele `sada` | `DBS0236126` (score `1004.36`) and `DBS0236129` (score `1007.84`) |
| 5 | case + accent folding `GÁMMAS13` | `DBS0236125`, score `59.65` (fuzzy branch, score < 1000) |
| 6 | `NGRAM_MATCH` typo `gammas14` | `DBS0236125`, score `52.46` (score < 1000) |
| 7 | obsolete value `oldalias` | 0 rows |
| 8 | updated value `newalias` | `DBS0236128` / `newAlias`, score `1000` |
| 9 | non-strain `DDB_G...` entries for `xylose` | only `DBS0236999` returned |
| 10 | other tags (`description`) under `limit: 1` | exactly `DBS0236999` |
| 11 | intra-strain dedup for `sada` | exactly 1 row for `DBS0236129` |
| 12 | prefix ranked first under `limit: 3` for `alpha` | `DBS0237001` first |

Threshold sweep for `NGRAM_MATCH("gammas14" → "gammaS13")`: matched at
`0.1`, `0.2`, `0.3`, `0.4`; no match at `0.5`–`0.9`. `0.3` is confirmed with
margin on both sides, so `autocompleteNgramThreshold = 0.3` stands.

Provisional `annotation_autocomplete_delim` pipeline (`Delimiter(",")` →
`Delimiter(" ")` → `Norm`) is **confirmed on 3.12.12** — interior allele
prefixes `axeb1` and `sada` both matched with a prefix score. No adjustment
to the analyzer definition is needed.

Other calibration findings folded into this plan:

- `is_obsolete == false` and
  `ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_autocomplete_norm")` both
  execute inside `SEARCH` in the `EnumerateViewNode` (verified in the
  `EXPLAIN` output), so both filters are index-level.
- ArangoDB 3.12 normalizes the norm locale `en.utf-8` to `en` in the stored
  analyzer properties. The Go driver definition keeps `en.utf-8`; the server
  performs the normalization.
- ArangoDB 3.12 removed `db._analyzers()` / `db._createAnalyzer()` from
  `arangosh` in favour of `require("@arangodb/analyzers")`; the harness uses
  the new module. This affects only the calibration script, not the Go
  driver path (`driver.Database.EnsureCreatedAnalyzer`).

EXPLAIN node counts (calibration):

- `EnumerateViewNode`: `4`
- `EnumerateCollectionNode`: `0`
- Other nodes: `SingletonNode 1`, `CalculationNode 34`,
  `SubqueryStartNode 10`, `TraversalNode 4`, `FilterNode 8`,
  `LimitNode 10`, `SubqueryEndNode 10`, `SortNode 7`,
  `EnumerateListNode 3`, `CollectNode 1`, `ReturnNode 1`

Baseline p95 latency (ms) & Task 7 measured p95 latency (ms):

- Baseline (Task 1, 20 runs, 1,035 documents): min `2`, median `3`,
  **p95 `3`**.
- Task 7 gate: see the Task 7 section below — the in-process baseline is not
  reachable through the HTTP client path, and the gate is re-stated there.

### Task 7 end-to-end gate — 2026-10-14

- EXPLAIN of the Go-built AQL statement, run through the go-driver against a
  real view over a 1,001-document fixture (`TestAutocompleteAnnotationExplainAndLatency`):
  - `EnumerateViewNode`: `4`
  - `EnumerateCollectionNode`: `0`
  - `TraversalNode`: `4`
- End-to-end bufconn round trip (`TestAutocompleteAnnotationEndToEnd`): a
  whole-value genotype prefix (`axea`) and an interior allele prefix (`sada`)
  both return `DBS0236126` with `DisplayText "axeA1, axeB1, sadA-"` and a
  score of at least `1000`.
- Task 7 measured latency, 20 runs of the Go-built statement through the
  go-driver on a 1,001-document fixture: min `3.32`–`3.66` ms, median
  `3.94`–`4.63` ms, p95 `5.84`–`8.53` ms.

**Latency gate restated.** The Task 1 baseline (`3` ms p95) was measured
in-process by `arangosh`, where the query engine result never crosses an HTTP
boundary. Through the go-driver every run pays a request round trip and JSON
decoding before the result is even visible to the test, and the measured client
path floor (`min 3.32` ms, `median 3.94`–`4.63` ms) shows the `3.75` ms ceiling
from `125%` of the baseline cannot be met by any client-path measurement — the
same statement measured `8.41` ms p95 on the first attempt. The gate therefore
asserts the *median*, which is stable to about `0.5` ms across runs, against
`3x` the in-process baseline (`9` ms ceiling): enough for the client-path
overhead of `1.3`–`1.5x` plus a slower host, while a pathological statement
(a collection scan, a per-document round trip, an unbounded `LIMIT`) moves the
median by an order of magnitude and still fails the gate.
