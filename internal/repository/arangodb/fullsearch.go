package arangodb

import (
	"context"
	"fmt"

	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	driver "github.com/arangodb/go-driver"
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
