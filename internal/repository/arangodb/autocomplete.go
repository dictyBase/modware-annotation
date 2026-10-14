package arangodb

import (
	"context"
	"fmt"
	"slices"

	E "github.com/IBM/fp-go/v2/either"
	fperrors "github.com/IBM/fp-go/v2/errors"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	O "github.com/IBM/fp-go/v2/option"
	ORD "github.com/IBM/fp-go/v2/ord"
	driver "github.com/arangodb/go-driver"
	eithererr "github.com/dictyBase/fp-go-loom/eithererr"
	MO "github.com/dictyBase/fp-go-loom/matchopt"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type (
	// autocompleteRequest is the state of one autocomplete search once the
	// query is normalized and the ontology term identifiers are resolved.
	autocompleteRequest struct {
		query          string
		limit          int
		synonymTermID  string
		genotypeTermID string
	}

	// autocompleteQueryParams is the finished AQL statement together with
	// the bind parameters it needs.
	autocompleteQueryParams struct {
		statement  string
		bindParams map[string]any
	}

	// suggestionRow is one raw row of the autocomplete statement. The field
	// names are the short aliases the statement returns.
	suggestionRow struct {
		ID    string  `json:"id"`
		Field string  `json:"f"`
		Value string  `json:"v"`
		Score float64 `json:"s"`
	}
)

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
	// autocompleteNgramMin is the shortest indexed n-gram.
	autocompleteNgramMin = 2
	// autocompleteNgramMax is the longest indexed n-gram.
	autocompleteNgramMax = 3
	// docFieldValue is the indexed value attribute on the annotation document.
	docFieldValue = "value"
	// docFieldEntryID is the indexed entry_id attribute on the annotation document.
	docFieldEntryID = "entry_id"
	// docFieldIsObsolete is the indexed is_obsolete attribute on the annotation document.
	docFieldIsObsolete = "is_obsolete"
	// autocompleteNgramThreshold is the minimum n-gram similarity of a
	// fuzzy match, confirmed in Task 1 calibration on ArangoDB.
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
	// msgNilAutocompleteQuery is the error message for a nil query struct.
	msgNilAutocompleteQuery = "expect a non-nil autocomplete query"
	// msgEmptyAutocompleteQuery is the error message for an empty query string.
	msgEmptyAutocompleteQuery = "expect a non-empty autocomplete query"
)

var (
	// autocompleteNormDef is the whole-value normalizing analyzer of the
	// autocomplete view.
	autocompleteNormDef = driver.ArangoSearchAnalyzerDefinition{
		Name: autocompleteNormAnalyzer,
		Type: driver.ArangoSearchAnalyzerTypeNorm,
		Properties: driver.ArangoSearchAnalyzerProperties{
			Locale: analyzerNormLocale,
			Case:   driver.ArangoSearchCaseLower,
			Accent: new(false),
		},
	}
	// autocompleteDelimDef splits a value on commas and spaces before
	// normalizing it, so an interior allele of a multi-allele genotype
	// value is matched by a prefix search.
	autocompleteDelimDef = driver.ArangoSearchAnalyzerDefinition{
		Name: autocompleteDelimAnalyzer,
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
	// autocompleteNgramDef normalizes a value and then indexes its
	// n-grams, so a misspelled query still matches.
	autocompleteNgramDef = driver.ArangoSearchAnalyzerDefinition{
		Name: autocompleteNgramAnalyzer,
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
	// synonymTermIDLens and genotypeTermIDLens focus the resolved ontology
	// term identifiers of a request, so IOE.Bind can fill them in.
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

	// isNonPositiveLimit and isAboveMaxLimit guard the two limit clamps.
	isNonPositiveLimit = F.Pipe1(0, ORD.Leq(predord.IntOrd))
	isAboveMaxLimit    = F.Pipe1(
		maxAutocompleteLimit,
		ORD.Gt(predord.IntOrd),
	)

	// clampAutocompleteLimit turns a requested limit into the effective one:
	// a non-positive request becomes the default and an oversized request is
	// capped. A value inside the range is used unchanged.
	clampAutocompleteLimit = MO.Match(
		defaultAutocompleteLimit,
		[]O.Kleisli[int, int]{
			MO.Const(isNonPositiveLimit, defaultAutocompleteLimit),
			MO.Const(isAboveMaxLimit, maxAutocompleteLimit),
			MO.Default(F.Identity[int]),
		},
	)
)

// ensureAutocompleteSearch creates the three analyzers of the autocomplete
// feature and reconciles the annotation_autocomplete view. It is a Kleisli
// arrow over the constructor state, so the repository pipeline composes it
// without forcing anything.
func ensureAutocompleteSearch(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	return F.Pipe4(
		IOE.Of[error](st),
		IOE.ChainFirst(ensureAutocompleteNormAnalyzer),
		IOE.ChainFirst(ensureAutocompleteDelimAnalyzer),
		IOE.ChainFirst(ensureAutocompleteNgramAnalyzer),
		IOE.Chain(ensureAutocompleteView),
	)
}

// ensureAutocompleteNormAnalyzer creates the whole-value normalizing analyzer
// when the database does not have it.
func ensureAutocompleteNormAnalyzer(
	st annotationRepoState,
) IOE.IOEither[error, string] {
	return ensureAnalyzer(st.db, autocompleteNormDef)
}

// ensureAutocompleteDelimAnalyzer creates the delimiter analyzer when the
// database does not have it.
func ensureAutocompleteDelimAnalyzer(
	st annotationRepoState,
) IOE.IOEither[error, string] {
	return ensureAnalyzer(st.db, autocompleteDelimDef)
}

// ensureAutocompleteNgramAnalyzer creates the n-gram analyzer when the
// database does not have it.
func ensureAutocompleteNgramAnalyzer(
	st annotationRepoState,
) IOE.IOEither[error, string] {
	return ensureAnalyzer(st.db, autocompleteNgramDef)
}

// ensureAnalyzer creates def on the database when it is missing. The analyzer
// definition is idempotent, so an existing analyzer is left untouched.
func ensureAnalyzer(
	dbh driver.Database,
	def driver.ArangoSearchAnalyzerDefinition,
) IOE.IOEither[error, string] {
	name := def.Name

	return F.Pipe1(
		IOE.TryCatchError(func() (string, error) {
			return createArangoSearchAnalyzer(dbh, def)
		}),
		IOE.MapLeft[string](func(err error) error {
			return fmt.Errorf("error in creating analyzer %s %s", name, err)
		}),
	)
}

// createArangoSearchAnalyzer is the raw analyzer call. The driver reports three
// values, so the raw shell lives here and the pipeline step only lifts it.
func createArangoSearchAnalyzer(
	dbh driver.Database,
	def driver.ArangoSearchAnalyzerDefinition,
) (string, error) {
	ctx := context.Background()
	_, _, err := dbh.EnsureCreatedAnalyzer(ctx, &def)

	return def.Name, err
}

// ensureAutocompleteView creates the annotation_autocomplete view when the
// database does not have it, and reconciles its links when it does. An
// existing view converges through SetProperties, so its identifier survives
// and downstream queries never see the view disappear.
func ensureAutocompleteView(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	return F.Pipe6(
		IOE.Of[error](st),
		IOE.Bind(viewExistsLens.Set, autocompleteViewExists),
		IOE.Chain(createAutocompleteViewIfMissing),
		IOE.Bind(viewLens.Set, findAutocompleteView),
		IOE.Bind(searchViewLens.Set, asArangoSearchView),
		IOE.Bind(viewPropsLens.Set, readAutocompleteViewProperties),
		IOE.Chain(updateAutocompleteViewLinks),
	)
}

// autocompleteViewExists reports whether the database already has the
// annotation_autocomplete view.
func autocompleteViewExists(
	st annotationRepoState,
) IOE.IOEither[error, bool] {
	ctx := context.Background()

	return F.Pipe1(
		IOE.TryCatchError(func() (bool, error) {
			return st.db.ViewExists(ctx, autocompleteViewName)
		}),
		IOE.MapLeft[bool](func(err error) error {
			return fmt.Errorf(
				"error in checking view %s %s",
				autocompleteViewName,
				err,
			)
		}),
	)
}

// createAutocompleteViewIfMissing creates the view only when it is absent.
// The branch selects between two lazy IOEither values; it runs no effect
// itself, so nothing is forced and no error is swallowed.
func createAutocompleteViewIfMissing(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	if st.viewExists {
		return IOE.Of[error](st)
	}

	return createAutocompleteView(st)
}

// createAutocompleteView creates the annotation_autocomplete view with the
// collection links of this feature.
func createAutocompleteView(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	return F.Pipe1(
		IOE.Of[error](st),
		IOE.ChainFirst(createAutocompleteViewStep),
	)
}

// createAutocompleteViewStep lifts the raw view creation into the pipeline. The
// step reports an empty value, so ChainFirst keeps the repository state.
func createAutocompleteViewStep(
	st annotationRepoState,
) IOE.IOEither[error, struct{}] {
	return F.Pipe1(
		IOE.TryCatchError(func() (struct{}, error) {
			return createAutocompleteViewRecord(st)
		}),
		IOE.MapLeft[struct{}](func(err error) error {
			return fmt.Errorf(
				"error in creating view %s %s",
				autocompleteViewName,
				err,
			)
		}),
	)
}

// createAutocompleteViewRecord is the raw view creation call. The driver
// reports two values, so the raw shell lives here and the pipeline step only
// lifts the result.
func createAutocompleteViewRecord(st annotationRepoState) (struct{}, error) {
	collName := st.anno.annot.Name()
	props := autocompleteViewProperties(collName)
	ctx := context.Background()
	_, err := st.db.CreateArangoSearchView(ctx, autocompleteViewName, &props)

	return struct{}{}, err
}

// findAutocompleteView reads the view that the previous step ensured exists.
func findAutocompleteView(
	st annotationRepoState,
) IOE.IOEither[error, driver.View] {
	ctx := context.Background()

	return F.Pipe1(
		IOE.TryCatchError(func() (driver.View, error) {
			return st.db.View(ctx, autocompleteViewName)
		}),
		IOE.MapLeft[driver.View](func(err error) error {
			return fmt.Errorf(
				"error in finding view %s %s",
				autocompleteViewName,
				err,
			)
		}),
	)
}

// asArangoSearchView narrows the generic view handle to its ArangoSearch form.
func asArangoSearchView(
	st annotationRepoState,
) IOE.IOEither[error, driver.ArangoSearchView] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.ArangoSearchView, error) {
			return st.view.ArangoSearchView()
		}),
		IOE.MapLeft[driver.ArangoSearchView](func(err error) error {
			return fmt.Errorf(
				"error in reading view %s %s",
				autocompleteViewName,
				err,
			)
		}),
	)
}

// readAutocompleteViewProperties reads the stored links of the view.
func readAutocompleteViewProperties(
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
				autocompleteViewName,
				err,
			)
		}),
	)
}

// updateAutocompleteViewLinks rewrites the stored links only when they differ
// from the links this feature needs, so a healthy view is left alone and a
// stale one converges in place.
func updateAutocompleteViewLinks(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	collName := st.anno.annot.Name()
	desired := autocompleteViewProperties(collName)
	if sameLinkShape(desired, st.viewProps) {
		return IOE.Of[error](st)
	}

	return setAutocompleteViewLinks(desired, st)
}

// setAutocompleteViewLinks writes the desired links onto the existing view.
// SetProperties updates the view in place, so the stored view identifier is
// preserved and a stale view is never deleted and recreated.
func setAutocompleteViewLinks(
	desired driver.ArangoSearchViewProperties,
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	step := setViewLinksStep(desired)

	return F.Pipe1(
		IOE.Of[error](st),
		IOE.ChainFirst(step),
	)
}

// setViewLinksStep curries the desired links into the pipeline step that
// writes them. The step reports an empty value, so ChainFirst keeps the
// repository state.
func setViewLinksStep(
	desired driver.ArangoSearchViewProperties,
) func(annotationRepoState) IOE.IOEither[error, struct{}] {
	return func(st annotationRepoState) IOE.IOEither[error, struct{}] {
		return F.Pipe1(
			IOE.TryCatchError(func() (struct{}, error) {
				return setViewLinksRecord(desired, st)
			}),
			IOE.MapLeft[struct{}](func(err error) error {
				return fmt.Errorf(
					"error in updating view %s %s",
					autocompleteViewName,
					err,
				)
			}),
		)
	}
}

// setViewLinksRecord is the raw view update call. The driver reports one value,
// so the raw shell lives here and the pipeline step only lifts the result.
func setViewLinksRecord(
	desired driver.ArangoSearchViewProperties,
	st annotationRepoState,
) (struct{}, error) {
	ctx := context.Background()
	err := st.searchView.SetProperties(ctx, desired)

	return struct{}{}, err
}

// autocompleteViewProperties is the desired view shape: the annotation
// collection indexed on its value, entry_id, and is_obsolete attributes.
// The is_obsolete entry carries no analyzer list on purpose: ArangoDB strips
// an analyzer that equals the inherited link default, with identity being
// that default, so the stored field would come back empty and every
// reconciliation would rewrite a view that is already correct.
func autocompleteViewProperties(
	collName string,
) driver.ArangoSearchViewProperties {
	fields := driver.ArangoSearchFields{
		docFieldValue: {
			Analyzers: []string{
				autocompleteNormAnalyzer,
				autocompleteDelimAnalyzer,
				autocompleteNgramAnalyzer,
			},
		},
		docFieldEntryID: {Analyzers: []string{autocompleteNormAnalyzer}},
		// ArangoDB strips an analyzer list that equals the inherited link
		// default, and identity is that default, so an explicit identity
		// list on is_obsolete would come back empty and force a rewrite on
		// every reconciliation of a view that is already correct.
		docFieldIsObsolete: {},
	}

	return driver.ArangoSearchViewProperties{
		Links: driver.ArangoSearchLinks{
			collName: {
				Analyzers: []string{autocompleteIdentityAnalyzer},
				Fields:    fields,
			},
		},
	}
}

// sameLinkShape reports whether two view property sets index the same
// collections and fields with the same analyzers. Unrelated view settings are
// ignored, so they never force a rewrite.
func sameLinkShape(
	a, b driver.ArangoSearchViewProperties,
) bool {
	if len(a.Links) != len(b.Links) {
		return false
	}
	for coll, left := range a.Links {
		right, ok := b.Links[coll]
		if !ok || !sameFieldShape(left.Fields, right.Fields) {
			return false
		}
	}

	return true
}

// sameFieldShape reports whether two field maps carry the same analyzers.
func sameFieldShape(
	a, b driver.ArangoSearchFields,
) bool {
	if len(a) != len(b) {
		return false
	}
	for field, left := range a {
		right, ok := b[field]
		if !ok || !sameAnalyzers(left.Analyzers, right.Analyzers) {
			return false
		}
	}

	return true
}

// sameAnalyzers reports whether two analyzer name lists hold the same names.
// ArangoDB may return them in any order, so the comparison sorts clones.
func sameAnalyzers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left := slices.Clone(a)
	right := slices.Clone(b)
	slices.Sort(left)
	slices.Sort(right)

	return slices.Equal(left, right)
}

// autocompleteAnalyzerFeatures are the features a pipeline analyzer of this
// feature needs. Norm is required by the normalization step, and frequency
// with position are required by the n-gram and BM25 scoring.
func autocompleteAnalyzerFeatures() []driver.ArangoSearchAnalyzerFeature {
	return []driver.ArangoSearchAnalyzerFeature{
		driver.ArangoSearchAnalyzerFeatureFrequency,
		driver.ArangoSearchAnalyzerFeatureNorm,
		driver.ArangoSearchAnalyzerFeaturePosition,
	}
}

// AutocompleteAnnotation returns suggestions for a partial strain synonym or
// genotype value. The pipeline normalizes and validates the query, resolves
// the two ontology term identifiers onto the request state with lenses, and
// runs the 4-branch statement. It is forced only once, by the caller.
func (ar *arangorepository) AutocompleteAnnotation(
	params *repo.AutocompleteQuery,
) IOE.IOEither[error, []*repo.Suggestion] {
	rowFault := scanSearchRows[suggestionRow](
		"error in reading autocomplete row",
	)

	return F.Pipe7(
		params,
		validateAutocompleteRequest,
		IOE.Bind(synonymTermIDLens.Set, ar.resolveSynonymTermID),
		IOE.Bind(genotypeTermIDLens.Set, ar.resolveGenotypeTermID),
		IOE.Map[error](ar.buildAutocompleteQueryParams),
		IOE.Chain(ar.executeAutocompleteQuery),
		IOE.Chain(rowFault),
		IOE.Map[error](toSuggestions),
	)
}

// validateAutocompleteRequest rejects a nil query and a query that holds no
// searchable text once it is normalized, before any ontology lookup or AQL
// statement runs.
func validateAutocompleteRequest(
	params *repo.AutocompleteQuery,
) IOE.IOEither[error, autocompleteRequest] {
	present := F.Pipe1(
		params,
		E.FromPredicate(
			isPresentAutocompleteQuery,
			eithererr.ConstErr[*repo.AutocompleteQuery](
				msgNilAutocompleteQuery,
			),
		),
	)
	withText := F.Pipe1(
		present,
		E.Chain(E.FromPredicate(
			hasAutocompleteQueryText,
			eithererr.ConstErr[*repo.AutocompleteQuery](
				msgEmptyAutocompleteQuery,
			),
		)),
	)
	request := F.Pipe1(withText, E.Map[error](newAutocompleteRequest))

	return IOE.FromEither(request)
}

// isPresentAutocompleteQuery reports whether the caller sent a query struct.
func isPresentAutocompleteQuery(params *repo.AutocompleteQuery) bool {
	return params != nil
}

// hasAutocompleteQueryText reports whether the query still holds searchable
// text once whitespace, case, and combining marks are gone.
func hasAutocompleteQueryText(params *repo.AutocompleteQuery) bool {
	normalized := repo.NormalizeAutocompleteQuery(params.Query)
	isSearchable := predord.MinStrLen(1)

	return isSearchable(normalized)
}

// newAutocompleteRequest normalizes the query and clamps the limit.
func newAutocompleteRequest(params *repo.AutocompleteQuery) autocompleteRequest {
	return autocompleteRequest{
		query: repo.NormalizeAutocompleteQuery(params.Query),
		limit: clampAutocompleteLimit(params.Limit),
	}
}

// resolveTermID looks up the cvterm identifier of one ontology tag.
func (ar *arangorepository) resolveTermID(
	onto, tag string,
) IOE.IOEither[error, string] {
	termFault := fperrors.OnError(
		"error in resolving autocomplete ontology term",
	)

	return F.Pipe1(
		IOE.TryCatchError(func() (string, error) {
			return ar.termID(onto, tag)
		}),
		IOE.MapLeft[string](termFault),
	)
}

// resolveSynonymTermID resolves the synonym cvterm identifier of the request.
func (ar *arangorepository) resolveSynonymTermID(
	autocompleteRequest,
) IOE.IOEither[error, string] {
	return ar.resolveTermID(dictyAnnotationOntology, fieldSynonym)
}

// resolveGenotypeTermID resolves the genotype cvterm identifier of the request.
func (ar *arangorepository) resolveGenotypeTermID(
	autocompleteRequest,
) IOE.IOEither[error, string] {
	return ar.resolveTermID(dictyAnnotationOntology, fieldGenotype)
}

// buildAutocompleteQueryParams finishes the statement and its bind parameters.
// The graph name and the analyzer and view names come from the repository, so
// a renamed collection never leaks a stale literal into a query.
func (ar *arangorepository) buildAutocompleteQueryParams(
	req autocompleteRequest,
) autocompleteQueryParams {
	graphName := ar.anno.annotg.Name()

	return autocompleteQueryParams{
		statement: buildAutocompleteQuery(),
		bindParams: map[string]any{
			"q":                 req.query,
			"th":                autocompleteNgramThreshold,
			"limit":             req.limit,
			"anno_cvterm_graph": graphName,
			bindSynonymTermID:   req.synonymTermID,
			bindGenotypeTermID:  req.genotypeTermID,
		},
	}
}

// executeAutocompleteQuery runs the statement and hands back the row reader.
func (ar *arangorepository) executeAutocompleteQuery(
	params autocompleteQueryParams,
) IOE.IOEither[error, dbRows] {
	queryFault := fperrors.OnError("error in running autocomplete query")

	return F.Pipe1(
		IOE.TryCatchError(func() (dbRows, error) {
			return ar.database.SearchRows(params.statement, params.bindParams)
		}),
		IOE.MapLeft[dbRows](queryFault),
	)
}

// toSuggestions projects the scanned statement rows onto the repository result.
func toSuggestions(rows []suggestionRow) []*repo.Suggestion {
	out := make([]*repo.Suggestion, 0, len(rows))
	for _, row := range rows {
		out = append(out, &repo.Suggestion{
			EntryID:     row.ID,
			Field:       row.Field,
			DisplayText: row.Value,
			Score:       row.Score,
		})
	}

	return out
}
