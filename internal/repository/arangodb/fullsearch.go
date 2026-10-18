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
	ORD "github.com/IBM/fp-go/v2/ord"
	P "github.com/IBM/fp-go/v2/predicate"
	driver "github.com/arangodb/go-driver"
	eithererr "github.com/dictyBase/fp-go-loom/eithererr"
	MO "github.com/dictyBase/fp-go-loom/matchopt"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type (
	// fullSearchTargets holds the resolved cvterm _id and ontology cv _id values
	// returned by fullSearchTargetsQ.
	fullSearchTargets struct {
		Tags   map[string]string `json:"tags"`
		Graphs map[string]string `json:"graphs"`
	}

	// fullSearchRequest is the validated state of one full search call.
	fullSearchRequest struct {
		ctx     context.Context
		query   string
		limit   int
		targets fullSearchTargets
	}

	// fullSearchQueryParams holds the finished AQL statement and bind map.
	fullSearchQueryParams struct {
		ctx        context.Context
		statement  string
		bindParams map[string]any
	}

	// fullSearchRow is one raw row of the full search projection.
	fullSearchRow struct {
		ID    string  `json:"id"`
		Field string  `json:"f"`
		Value string  `json:"v"`
		Score float64 `json:"s"`
	}
)

const (
	// fullSearchViewName is the classic arangosearch view of the full
	// search feature.
	fullSearchViewName = "annotation_full_search"
	// fullSearchNormAnalyzer lowercases a value and removes accents.
	fullSearchNormAnalyzer = "annotation_search_norm"
	// fullSearchDelimAnalyzer splits on commas and spaces, then
	// lowercases and removes accents. It serves interior allele prefix
	// matching on comma-separated genotype values.
	fullSearchDelimAnalyzer = "annotation_search_delim"
	// fullSearchNgramAnalyzer chains norm and ngram (2..3, UTF-8,
	// preserveOriginal=true). It serves the fuzzy branches.
	fullSearchNgramAnalyzer = "annotation_search_ngram"
	// fullSearchTextAnalyzer is the built-in English text analyzer for
	// token and phrase branches. Setup must never create it.
	fullSearchTextAnalyzer = "text_en"
	// fullSearchIdentityAnalyzer is the built-in identity analyzer.
	fullSearchIdentityAnalyzer = "identity"

	// Document attributes on the cvterm collection.
	docFieldLabel      = "label"
	docFieldGraphID    = "graph_id"
	docFieldDeprecated = "deprecated"

	// Limits and thresholds.
	fullSearchNgramThreshold = 0.3
	defaultFullSearchLimit   = 50
	maxFullSearchLimit       = 50

	// Score bands (250 apart: prefix > phrase > token > fuzzy).
	fullSearchPrefixBand = "1000 + BM25(d)"
	fullSearchPhraseBand = "500 + BM25(d)"
	//nolint:gosec // G101 false positive on ArangoSearch score band expression
	fullSearchTokenBand = "250 + BM25(d)"
	fullSearchFuzzyBand = "BM25(d)"

	// Canonical field labels reported in FullSearchResult.Field.
	fieldSystematicName      = "systematic_name"
	fieldGeneticModification = "genetic_modification"
	fieldMutagenesisMethod   = "mutagenesis_method"
	fieldPhenotype           = "phenotype"
	fieldEnvironment         = "environment"
	fieldAssay               = "assay"
	fieldCharacteristic      = "characteristic"
	fieldPublicNote          = "public_note"

	// Ontology namespaces and cvterm tag labels in ArangoDB.
	mutagenesisOntology    = "Dd Mutagenesis Method"
	phenotypeOntology      = "Dicty Phenotypes"
	environmentOntology    = "Dicty Environment"
	assayOntology          = "Dictyostelium Assay"
	characteristicOntology = "dicty_strain_characteristics"
	tagSystematicName      = "systematic name"
	tagMutantType          = "mutant type"
	tagMutagenesisMethod   = "mutagenesis method"
	tagPublicNote          = "public note"

	// Bind parameter names for resolved cvterm _id and cv _id targets.
	bindSysnameTermID    = "sysname_term_id"
	bindMuttypeTermID    = "muttype_term_id"
	bindMutmethodTermID  = "mutmethod_term_id"
	bindPubnoteTermID    = "pubnote_term_id"
	bindPhenotypeGraphID = "phenotype_graph_id"
	bindEnvGraphID       = "env_graph_id"
	bindAssayGraphID     = "assay_graph_id"
	bindCharGraphID      = "char_graph_id"

	// Validation error messages.
	msgNilFullSearchQuery   = "expect a non-nil full search query"
	msgEmptyFullSearchQuery = "expect a non-empty full search query"
)

var (
	// fullSearchNormDef is the whole-value normalizing analyzer of the
	// full search view.
	fullSearchNormDef = driver.ArangoSearchAnalyzerDefinition{
		Name: fullSearchNormAnalyzer,
		Type: driver.ArangoSearchAnalyzerTypeNorm,
		Properties: driver.ArangoSearchAnalyzerProperties{
			Locale: analyzerNormLocale,
			Case:   driver.ArangoSearchCaseLower,
			Accent: new(false),
		},
	}
	// fullSearchDelimDef splits a value on commas and spaces before
	// normalizing it, so an interior allele of a multi-allele genotype
	// value is matched by a prefix search.
	fullSearchDelimDef = driver.ArangoSearchAnalyzerDefinition{
		Name: fullSearchDelimAnalyzer,
		Type: driver.ArangoSearchAnalyzerTypePipeline,
		Properties: driver.ArangoSearchAnalyzerProperties{
			Pipeline: []driver.ArangoSearchAnalyzerPipeline{
				{
					Type: driver.ArangoSearchAnalyzerTypeDelimiter,
					Properties: driver.ArangoSearchAnalyzerProperties{
						Delimiter: ",",
					},
				},
				{
					Type: driver.ArangoSearchAnalyzerTypeDelimiter,
					Properties: driver.ArangoSearchAnalyzerProperties{
						Delimiter: " ",
					},
				},
				{
					Type: driver.ArangoSearchAnalyzerTypeNorm,
					Properties: driver.ArangoSearchAnalyzerProperties{
						Locale: analyzerNormLocale,
						Case:   driver.ArangoSearchCaseLower,
						Accent: new(false),
					},
				},
			},
		},
		Features: autocompleteAnalyzerFeatures(),
	}
	// fullSearchNgramDef normalizes a value and then indexes its
	// n-grams, so a misspelled query still matches.
	fullSearchNgramDef = driver.ArangoSearchAnalyzerDefinition{
		Name: fullSearchNgramAnalyzer,
		Type: driver.ArangoSearchAnalyzerTypePipeline,
		Properties: driver.ArangoSearchAnalyzerProperties{
			Pipeline: []driver.ArangoSearchAnalyzerPipeline{
				{
					Type: driver.ArangoSearchAnalyzerTypeNorm,
					Properties: driver.ArangoSearchAnalyzerProperties{
						Locale: analyzerNormLocale,
						Case:   driver.ArangoSearchCaseLower,
						Accent: new(false),
					},
				},
				{
					Type: driver.ArangoSearchAnalyzerTypeNGram,
					Properties: driver.ArangoSearchAnalyzerProperties{
						Min:              new(int64(autocompleteNgramMin)),
						Max:              new(int64(autocompleteNgramMax)),
						PreserveOriginal: new(true),
						StreamType:       new(driver.ArangoSearchNGramStreamUTF8),
					},
				},
			},
		},
		Features: autocompleteAnalyzerFeatures(),
	}
)

// ensureFullSearch creates the three analyzers of the full search feature and
// reconciles the annotation_full_search view. It is a Kleisli arrow over the
// constructor state, so the repository pipeline composes it without forcing
// anything.
func ensureFullSearch(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	return F.Pipe4(
		IOE.Of[error](st),
		IOE.ChainFirst(ensureFullSearchNormAnalyzer),
		IOE.ChainFirst(ensureFullSearchDelimAnalyzer),
		IOE.ChainFirst(ensureFullSearchNgramAnalyzer),
		IOE.Chain(ensureFullSearchView),
	)
}

// ensureFullSearchNormAnalyzer creates the whole-value normalizing analyzer
// when the database does not have it.
func ensureFullSearchNormAnalyzer(
	st annotationRepoState,
) IOE.IOEither[error, string] {
	return ensureAnalyzer(st.db, fullSearchNormDef)
}

// ensureFullSearchDelimAnalyzer creates the delimiter analyzer when the
// database does not have it.
func ensureFullSearchDelimAnalyzer(
	st annotationRepoState,
) IOE.IOEither[error, string] {
	return ensureAnalyzer(st.db, fullSearchDelimDef)
}

// ensureFullSearchNgramAnalyzer creates the n-gram analyzer when the database
// does not have it.
func ensureFullSearchNgramAnalyzer(
	st annotationRepoState,
) IOE.IOEither[error, string] {
	return ensureAnalyzer(st.db, fullSearchNgramDef)
}

// ensureFullSearchView creates the annotation_full_search view when the
// database does not have it, and reconciles its links when it does. An existing
// view converges through SetProperties, so its identifier survives and
// downstream queries never see the view disappear.
func ensureFullSearchView(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	return F.Pipe6(
		IOE.Of[error](st),
		IOE.Bind(viewExistsLens.Set, fullSearchViewExists),
		IOE.Chain(createFullSearchViewIfMissing),
		IOE.Bind(viewLens.Set, findFullSearchView),
		IOE.Bind(searchViewLens.Set, asFullSearchArangoSearchView),
		IOE.Bind(viewPropsLens.Set, readFullSearchViewProperties),
		IOE.Chain(updateFullSearchViewLinks),
	)
}

// fullSearchViewExists reports whether the database already has the
// annotation_full_search view.
func fullSearchViewExists(
	st annotationRepoState,
) IOE.IOEither[error, bool] {
	ctx := context.Background()

	return F.Pipe1(
		IOE.TryCatchError(func() (bool, error) {
			return st.db.ViewExists(ctx, fullSearchViewName)
		}),
		IOE.MapLeft[bool](func(err error) error {
			return fmt.Errorf(
				"error in checking view %s %s",
				fullSearchViewName,
				err,
			)
		}),
	)
}

// createFullSearchViewIfMissing creates the view only when it is absent. The
// branch selects between two lazy IOEither values; it runs no effect itself, so
// nothing is forced and no error is swallowed.
func createFullSearchViewIfMissing(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	if st.viewExists {
		return IOE.Of[error](st)
	}

	return createFullSearchView(st)
}

// createFullSearchView creates the annotation_full_search view with the
// collection links of this feature.
func createFullSearchView(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	return F.Pipe1(
		IOE.Of[error](st),
		IOE.ChainFirst(createFullSearchViewStep),
	)
}

// createFullSearchViewStep lifts the raw view creation into the pipeline. The
// step reports an empty value, so ChainFirst keeps the repository state.
func createFullSearchViewStep(
	st annotationRepoState,
) IOE.IOEither[error, struct{}] {
	return F.Pipe1(
		IOE.TryCatchError(func() (struct{}, error) {
			return createFullSearchViewRecord(st)
		}),
		IOE.MapLeft[struct{}](func(err error) error {
			return fmt.Errorf(
				"error in creating view %s %s",
				fullSearchViewName,
				err,
			)
		}),
	)
}

// createFullSearchViewRecord is the raw view creation call. The driver reports
// two values, so the raw shell lives here and the pipeline step only lifts the
// result.
func createFullSearchViewRecord(st annotationRepoState) (struct{}, error) {
	annotColl := st.anno.annot.Name()
	cvtermColl := st.onto.Term.Name()
	props := fullSearchViewProperties(annotColl, cvtermColl)
	ctx := context.Background()
	_, err := st.db.CreateArangoSearchView(ctx, fullSearchViewName, &props)

	return struct{}{}, err
}

// findFullSearchView reads the view that the previous step ensured exists.
func findFullSearchView(
	st annotationRepoState,
) IOE.IOEither[error, driver.View] {
	ctx := context.Background()

	return F.Pipe1(
		IOE.TryCatchError(func() (driver.View, error) {
			return st.db.View(ctx, fullSearchViewName)
		}),
		IOE.MapLeft[driver.View](func(err error) error {
			return fmt.Errorf(
				"error in finding view %s %s",
				fullSearchViewName,
				err,
			)
		}),
	)
}

// asFullSearchArangoSearchView narrows the generic view handle to its
// ArangoSearch form.
func asFullSearchArangoSearchView(
	st annotationRepoState,
) IOE.IOEither[error, driver.ArangoSearchView] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.ArangoSearchView, error) {
			return st.view.ArangoSearchView()
		}),
		IOE.MapLeft[driver.ArangoSearchView](func(err error) error {
			return fmt.Errorf(
				"error in reading view %s %s",
				fullSearchViewName,
				err,
			)
		}),
	)
}

// readFullSearchViewProperties reads the stored links of the view.
func readFullSearchViewProperties(
	st annotationRepoState,
) IOE.IOEither[error, driver.ArangoSearchViewProperties] {
	ctx := context.Background()

	return F.Pipe1(
		IOE.TryCatchError(func() (driver.ArangoSearchViewProperties, error) {
			return st.searchView.Properties(ctx)
		}),
		IOE.MapLeft[driver.ArangoSearchViewProperties](func(err error) error {
			return fmt.Errorf(
				"error in reading view properties %s %s",
				fullSearchViewName,
				err,
			)
		}),
	)
}

// updateFullSearchViewLinks rewrites the stored links only when they differ
// from the links this feature needs, so a healthy view is left alone and a stale
// one converges in place.
func updateFullSearchViewLinks(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	annotColl := st.anno.annot.Name()
	cvtermColl := st.onto.Term.Name()
	desired := fullSearchViewProperties(annotColl, cvtermColl)
	if sameLinkShape(desired, st.viewProps) {
		return IOE.Of[error](st)
	}

	return setFullSearchViewLinks(desired, st)
}

// setFullSearchViewLinks writes the desired links onto the existing view.
// SetProperties updates the view in place, so the stored view identifier is
// preserved and a stale view is never deleted and recreated.
func setFullSearchViewLinks(
	desired driver.ArangoSearchViewProperties,
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	step := setFullSearchViewLinksStep(desired)

	return F.Pipe1(
		IOE.Of[error](st),
		IOE.ChainFirst(step),
	)
}

// setFullSearchViewLinksStep curries the desired links into the pipeline step
// that writes them. The step reports an empty value, so ChainFirst keeps the
// repository state.
func setFullSearchViewLinksStep(
	desired driver.ArangoSearchViewProperties,
) func(annotationRepoState) IOE.IOEither[error, struct{}] {
	return func(st annotationRepoState) IOE.IOEither[error, struct{}] {
		return F.Pipe1(
			IOE.TryCatchError(func() (struct{}, error) {
				return setFullSearchViewLinksRecord(desired, st)
			}),
			IOE.MapLeft[struct{}](func(err error) error {
				return fmt.Errorf(
					"error in updating view %s %s",
					fullSearchViewName,
					err,
				)
			}),
		)
	}
}

// setFullSearchViewLinksRecord is the raw view update call. The driver reports
// one value, so the raw shell lives here and the pipeline step only lifts the
// result.
func setFullSearchViewLinksRecord(
	desired driver.ArangoSearchViewProperties,
	st annotationRepoState,
) (struct{}, error) {
	ctx := context.Background()
	err := st.searchView.SetProperties(ctx, desired)

	return struct{}{}, err
}

// fullSearchViewProperties is the desired view shape: the annotation collection
// indexed on its value, entry_id, and is_obsolete attributes, and the cvterm
// collection indexed on its label, graph_id, and deprecated attributes.
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
	cvtermFields := driver.ArangoSearchFields{
		docFieldLabel: {
			Analyzers: []string{
				fullSearchNormAnalyzer,
				fullSearchNgramAnalyzer,
				fullSearchTextAnalyzer,
			},
		},
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

var (
	fullSearchCtxLens = L.MakeLens(
		func(r fullSearchRequest) context.Context { return r.ctx },
		func(r fullSearchRequest, ctx context.Context) fullSearchRequest {
			r.ctx = ctx
			return r
		},
	)
	fullSearchParamsCtxLens = L.MakeLens(
		func(p fullSearchQueryParams) context.Context { return p.ctx },
		func(
			p fullSearchQueryParams,
			ctx context.Context,
		) fullSearchQueryParams {
			p.ctx = ctx
			return p
		},
	)
	fullSearchQueryLens = L.MakeLens(
		func(r fullSearchRequest) string { return r.query },
		func(r fullSearchRequest, q string) fullSearchRequest {
			r.query = q
			return r
		},
	)
	fullSearchLimitLens = L.MakeLens(
		func(r fullSearchRequest) int { return r.limit },
		func(r fullSearchRequest, l int) fullSearchRequest {
			r.limit = l
			return r
		},
	)
	fullSearchTargetsLens = L.MakeLens(
		func(r fullSearchRequest) fullSearchTargets { return r.targets },
		func(r fullSearchRequest, t fullSearchTargets) fullSearchRequest {
			r.targets = t
			return r
		},
	)
	fullSearchStmtLens = L.MakeLens(
		func(p fullSearchQueryParams) string { return p.statement },
		func(p fullSearchQueryParams, s string) fullSearchQueryParams {
			p.statement = s
			return p
		},
	)
	fullSearchBindLens = L.MakeLens(
		func(p fullSearchQueryParams) map[string]any { return p.bindParams },
		func(
			p fullSearchQueryParams,
			b map[string]any,
		) fullSearchQueryParams {
			p.bindParams = b
			return p
		},
	)

	isAboveFullSearchMaxLimit = F.Pipe1(
		maxFullSearchLimit,
		ORD.Gt(predord.IntOrd),
	)

	// clampFullSearchLimit turns a requested limit into the effective one:
	// a non-positive request becomes defaultFullSearchLimit (50) and an
	// oversized request is capped at maxFullSearchLimit (50).
	clampFullSearchLimit = MO.Match(
		defaultFullSearchLimit,
		[]O.Kleisli[int, int]{
			MO.Const(isNonPositiveLimit, defaultFullSearchLimit),
			MO.Const(isAboveFullSearchMaxLimit, maxFullSearchLimit),
			MO.Default(F.Identity[int]),
		},
	)

	keepsNonStopword = F.Pipe2(
		repo.IsAllStopwords,
		P.Not,
		P.ContraMap(fullSearchQueryLens.Get),
	)

	isNonEmptyFullSearchRows  = predarrays.IsNonEmpty[fullSearchRow]()
	mapFullSearchRows         = A.Map(toFullSearchResult)
	runFullSearchTargetsFault = fperrors.OnError(
		"error in resolving full search targets",
	)
	readFullSearchTargetsFault = fperrors.OnError(
		"error in reading full search targets",
	)
	runFullSearchQueryFault = fperrors.OnError(
		"error in running full search query",
	)
)

// isPresentFullSearchQuery reports whether the caller sent a query struct.
func isPresentFullSearchQuery(params *repo.FullSearchQuery) bool {
	return params != nil
}

// hasFullSearchQueryText reports whether the normalized query retains at least
// one character.
func hasFullSearchQueryText(req fullSearchRequest) bool {
	pred := predord.MinStrLen(1)

	return F.Pipe1(req.query, pred)
}

// stopwordFullSearchError reports a query whose tokens are all English stopwords.
func stopwordFullSearchError(req fullSearchRequest) error {
	return fmt.Errorf(
		"expect a query with at least one non-stopword, received %q",
		req.query,
	)
}

// newFullSearchRequest normalizes the query and clamps the limit.
func newFullSearchRequest(params *repo.FullSearchQuery) fullSearchRequest {
	normalized := F.Pipe1(params.Query, repo.NormalizeAutocompleteQuery)
	limit := F.Pipe1(params.Limit, clampFullSearchLimit)

	return F.Pipe2(
		fullSearchRequest{},
		fullSearchQueryLens.Set(normalized),
		fullSearchLimitLens.Set(limit),
	)
}

// validateFullSearchRequest rejects a nil request, an empty normalized query,
// and an all-stopword query before any AQL statement runs.
func validateFullSearchRequest(
	params *repo.FullSearchQuery,
) E.Either[error, fullSearchRequest] {
	nilErr := eithererr.ConstErr[*repo.FullSearchQuery](msgNilFullSearchQuery)
	emptyErr := eithererr.ConstErr[fullSearchRequest](msgEmptyFullSearchQuery)
	checkNotEmpty := E.FromPredicate(hasFullSearchQueryText, emptyErr)
	checkNotStopword := E.FromPredicate(keepsNonStopword, stopwordFullSearchError)

	return F.Pipe4(
		params,
		E.FromPredicate(isPresentFullSearchQuery, nilErr),
		E.Map[error](newFullSearchRequest),
		E.Chain(checkNotEmpty),
		E.Chain(checkNotStopword),
	)
}

// fullSearchTargetBindParams builds the bind parameters for fullSearchTargetsQ.
func (ar *arangorepository) fullSearchTargetBindParams() map[string]any {
	return map[string]any{
		cvCollectionBind:     ar.onto.Cv.Name(),
		cvtermCollectionBind: ar.onto.Term.Name(),
		"tag_ontologies": []string{
			dictyAnnotationOntology,
			mutagenesisOntology,
		},
		"tag_labels": []string{
			fieldSynonym,
			fieldGenotype,
			tagSystematicName,
			tagMutantType,
			tagMutagenesisMethod,
			tagPublicNote,
		},
		"term_ontologies": []string{
			phenotypeOntology,
			environmentOntology,
			assayOntology,
			characteristicOntology,
		},
	}
}

// readFullSearchTargetsRow lifts decodeFullSearchTargetsRow into IOEither.
func readFullSearchTargetsRow(
	row *contextResult,
) IOE.IOEither[error, fullSearchTargets] {
	return F.Pipe1(
		IOE.TryCatchError(func() (fullSearchTargets, error) {
			var targets fullSearchTargets
			err := row.Read(&targets)
			return targets, err
		}),
		IOE.MapLeft[fullSearchTargets](readFullSearchTargetsFault),
	)
}

// resolveFullSearchTargets resolves the 6 tag cvterm._id values and the 4
// ontology cv._id values in a single AQL round trip. Unloaded ontologies or
// tags are absent from the returned maps and default to "" when bound.
func (ar *arangorepository) resolveFullSearchTargets(
	req fullSearchRequest,
) IOE.IOEither[error, fullSearchTargets] {
	ctx := fullSearchCtxLens.Get(req)
	return F.Pipe2(
		IOE.TryCatchError(func() (*contextResult, error) {
			bindParams := ar.fullSearchTargetBindParams()
			return ar.getRow(
				ctx,
				fullSearchTargetsQ,
				bindParams,
			)
		}),
		IOE.MapLeft[*contextResult](runFullSearchTargetsFault),
		IOE.Chain(readFullSearchTargetsRow),
	)
}

// fullSearchBindParams builds the 14-entry bind map for the 28-branch query.
func (ar *arangorepository) fullSearchBindParams(
	req fullSearchRequest,
) map[string]any {
	targets := fullSearchTargetsLens.Get(req)
	tags := targets.Tags
	graphs := targets.Graphs

	return map[string]any{
		"q":                  fullSearchQueryLens.Get(req),
		"th":                 fullSearchNgramThreshold,
		"limit":              fullSearchLimitLens.Get(req),
		"anno_cvterm_graph":  ar.anno.annotg.Name(),
		bindSynonymTermID:    tags["dicty_annotation:synonym"],
		bindGenotypeTermID:   tags["dicty_annotation:genotype"],
		bindSysnameTermID:    tags["dicty_annotation:systematic name"],
		bindMuttypeTermID:    tags["dicty_annotation:mutant type"],
		bindMutmethodTermID:  tags["Dd Mutagenesis Method:mutagenesis method"],
		bindPubnoteTermID:    tags["dicty_annotation:public note"],
		bindPhenotypeGraphID: graphs[phenotypeOntology],
		bindEnvGraphID:       graphs[environmentOntology],
		bindAssayGraphID:     graphs[assayOntology],
		bindCharGraphID:      graphs[characteristicOntology],
	}
}

// buildFullSearchQueryParams assembles the 28-branch statement and bind map.
func (ar *arangorepository) buildFullSearchQueryParams(
	req fullSearchRequest,
) fullSearchQueryParams {
	ctx := fullSearchCtxLens.Get(req)
	statement := buildFullSearchQuery()
	bindParams := ar.fullSearchBindParams(req)

	return F.Pipe3(
		fullSearchQueryParams{},
		fullSearchParamsCtxLens.Set(ctx),
		fullSearchStmtLens.Set(statement),
		fullSearchBindLens.Set(bindParams),
	)
}

// runFullSearchRows is the raw SearchRows adapter for executeFullSearchQuery.
func (ar *arangorepository) runFullSearchRows(
	params fullSearchQueryParams,
) (dbRows, error) {
	ctx := fullSearchParamsCtxLens.Get(params)
	stmt := fullSearchStmtLens.Get(params)
	binds := fullSearchBindLens.Get(params)

	return ar.searchRows(ctx, stmt, binds)
}

// executeFullSearchQuery runs the 28-branch full search statement.
func (ar *arangorepository) executeFullSearchQuery(
	params fullSearchQueryParams,
) IOE.IOEither[error, dbRows] {
	return F.Pipe1(
		IOE.TryCatchError(func() (dbRows, error) {
			return ar.runFullSearchRows(params)
		}),
		IOE.MapLeft[dbRows](runFullSearchQueryFault),
	)
}

// emptyFullSearchResults returns a non-nil empty slice when no rows match.
func emptyFullSearchResults() []*repo.FullSearchResult {
	return []*repo.FullSearchResult{}
}

// toFullSearchResult maps one projection row to a repository FullSearchResult.
func toFullSearchResult(row fullSearchRow) *repo.FullSearchResult {
	return &repo.FullSearchResult{
		EntryID:     row.ID,
		Field:       row.Field,
		DisplayText: row.Value,
		Score:       row.Score,
	}
}

// toFullSearchResults projects scanned rows into FullSearchResult pointers
// without imperative loops, preserving a non-nil empty slice for zero hits.
func toFullSearchResults(rows []fullSearchRow) []*repo.FullSearchResult {
	return F.Pipe2(
		rows,
		O.FromPredicate(isNonEmptyFullSearchRows),
		O.Fold(emptyFullSearchResults, mapFullSearchRows),
	)
}

// SearchAnnotation returns at most Limit ranked results for the normalized
// query of params as a lazy IOEither pipeline.
func (ar *arangorepository) SearchAnnotation(
	ctx context.Context,
	params *repo.FullSearchQuery,
) IOE.IOEither[error, []*repo.FullSearchResult] {
	baseCtx := normalizeQueryContext(ctx)
	withCtx := fullSearchCtxLens.Set(baseCtx)
	rowFault := scanSearchRows[fullSearchRow](
		"error in reading full search row",
	)

	return F.Pipe7(
		IOE.Of[error](params),
		IOE.ChainEitherK(validateFullSearchRequest),
		IOE.Map[error](withCtx),
		IOE.Bind(fullSearchTargetsLens.Set, ar.resolveFullSearchTargets),
		IOE.Map[error](ar.buildFullSearchQueryParams),
		IOE.Chain(ar.executeFullSearchQuery),
		IOE.Chain(rowFault),
		IOE.Map[error](toFullSearchResults),
	)
}
