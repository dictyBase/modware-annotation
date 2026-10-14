package arangodb

import (
	"context"
	"errors"
	"fmt"
	"slices"

	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	driver "github.com/arangodb/go-driver"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
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
			_, _, err := dbh.EnsureCreatedAnalyzer(
				context.Background(),
				&def,
			)
			return name, err
		}),
		IOE.MapLeft[string](func(err error) error {
			return fmt.Errorf("error in creating analyzer %s %s", name, err)
		}),
	)
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
	return F.Pipe1(
		IOE.TryCatchError(func() (bool, error) {
			return st.db.ViewExists(
				context.Background(),
				autocompleteViewName,
			)
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
	collName := st.anno.annot.Name()
	props := autocompleteViewProperties(collName)

	return F.Pipe1(
		IOE.TryCatchError(func() (annotationRepoState, error) {
			_, err := st.db.CreateArangoSearchView(
				context.Background(),
				autocompleteViewName,
				&props,
			)
			return st, err
		}),
		IOE.MapLeft[annotationRepoState](func(err error) error {
			return fmt.Errorf(
				"error in creating view %s %s",
				autocompleteViewName,
				err,
			)
		}),
	)
}

// findAutocompleteView reads the view that the previous step ensured exists.
func findAutocompleteView(
	st annotationRepoState,
) IOE.IOEither[error, driver.View] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.View, error) {
			return st.db.View(
				context.Background(),
				autocompleteViewName,
			)
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
	return F.Pipe1(
		IOE.TryCatchError(func() (annotationRepoState, error) {
			return st, st.searchView.SetProperties(
				context.Background(),
				desired,
			)
		}),
		IOE.MapLeft[annotationRepoState](func(err error) error {
			return fmt.Errorf(
				"error in updating view %s %s",
				autocompleteViewName,
				err,
			)
		}),
	)
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
// genotype value.
//
// TODO(task 5): replace this stub with the search statement, term-id
// resolution, and row mapping.
func (ar *arangorepository) AutocompleteAnnotation(
	params *repo.AutocompleteQuery,
) IOE.IOEither[error, []*repo.Suggestion] {
	_ = params

	return IOE.Left[[]*repo.Suggestion](errors.New("not implemented"))
}
