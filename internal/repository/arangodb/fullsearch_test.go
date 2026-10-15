package arangodb

import (
	"context"
	"slices"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/arangomanager/testarango"
	"github.com/stretchr/testify/require"
)

func TestEnsureFullSearchCreatesAssets(t *testing.T) {
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
	assert.Contains(names, fullSearchNormAnalyzer)
	assert.Contains(names, fullSearchDelimAnalyzer)
	assert.Contains(names, fullSearchNgramAnalyzer)

	exists, err := dbh.ViewExists(ctx, fullSearchViewName)
	assert.NoError(err)
	assert.True(exists, "expect annotation_full_search view to exist")

	view, err := dbh.View(ctx, fullSearchViewName)
	assert.NoError(err)
	asv, err := view.ArangoSearchView()
	assert.NoError(err)
	props, err := asv.Properties(ctx)
	assert.NoError(err)

	collP := getCollectionParams()
	ontoP := getOntoParams()

	annotLink, ok := props.Links[collP.Annotation]
	assert.True(ok, "expect annotation collection link in view")
	assert.Len(annotLink.Fields, 3)

	valAnalyzers := slices.Clone(annotLink.Fields[docFieldValue].Analyzers)
	slices.Sort(valAnalyzers)
	assert.Equal(
		[]string{
			fullSearchDelimAnalyzer,
			fullSearchNgramAnalyzer,
			fullSearchNormAnalyzer,
			fullSearchTextAnalyzer,
		},
		valAnalyzers,
	)
	assert.Equal(
		[]string{fullSearchNormAnalyzer},
		annotLink.Fields[docFieldEntryID].Analyzers,
	)
	assert.Empty(
		annotLink.Fields[docFieldIsObsolete].Analyzers,
		"expect is_obsolete to inherit identity analyzer of link",
	)

	termLink, ok := props.Links[ontoP.Term]
	assert.True(ok, "expect cvterm collection link in view")
	assert.Len(termLink.Fields, 3)

	labelAnalyzers := slices.Clone(termLink.Fields[docFieldLabel].Analyzers)
	slices.Sort(labelAnalyzers)
	assert.Equal(
		[]string{
			fullSearchNgramAnalyzer,
			fullSearchNormAnalyzer,
			fullSearchTextAnalyzer,
		},
		labelAnalyzers,
	)
	assert.Empty(
		termLink.Fields[docFieldGraphID].Analyzers,
		"expect graph_id to inherit identity analyzer of link",
	)
	assert.Empty(
		termLink.Fields[docFieldDeprecated].Analyzers,
		"expect deprecated to inherit identity analyzer of link",
	)
}

func TestEnsureFullSearchReconcilesStaleView(t *testing.T) {
	assert := require.New(t)
	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err)

	dbr, err := tra.DB(tra.Database)
	assert.NoError(err)
	db := dbr.Handler()
	ctx := context.Background()
	collP := getCollectionParams()
	ontoP := getOntoParams()

	_, err = db.CreateCollection(ctx, collP.Annotation, nil)
	assert.NoError(err)
	_, err = db.CreateCollection(ctx, ontoP.Term, nil)
	assert.NoError(err)

	_, _, err = db.EnsureCreatedAnalyzer(ctx, &fullSearchNormDef)
	assert.NoError(err)

	// Pre-create annotation_full_search with stale field analyzers on annotation,
	// a stale link-level analyzer, and missing cvterm link.
	stale, err := db.CreateArangoSearchView(
		ctx,
		fullSearchViewName,
		&driver.ArangoSearchViewProperties{
			Links: driver.ArangoSearchLinks{
				collP.Annotation: {
					Analyzers: []string{fullSearchNormAnalyzer},
					Fields: driver.ArangoSearchFields{
						docFieldValue:      {Analyzers: []string{fullSearchIdentityAnalyzer}},
						docFieldEntryID:    {Analyzers: []string{fullSearchIdentityAnalyzer}},
						docFieldIsObsolete: {Analyzers: []string{fullSearchIdentityAnalyzer}},
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
		ontoP,
	)
	assert.NoError(err)
	defer tearDown(repo)

	fresh, err := db.View(ctx, fullSearchViewName)
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

	freshAnnotLink := freshProps.Links[collP.Annotation]
	assert.True(
		slices.Equal(freshAnnotLink.Analyzers, []string{fullSearchIdentityAnalyzer}) ||
			len(freshAnnotLink.Analyzers) == 0,
		"expect annotation link analyzer reconciled to identity default",
	)
	valAnalyzers := slices.Clone(freshAnnotLink.Fields[docFieldValue].Analyzers)
	slices.Sort(valAnalyzers)
	assert.Equal(
		[]string{
			fullSearchDelimAnalyzer,
			fullSearchNgramAnalyzer,
			fullSearchNormAnalyzer,
			fullSearchTextAnalyzer,
		},
		valAnalyzers,
	)

	freshTermLink, ok := freshProps.Links[ontoP.Term]
	assert.True(ok, "expect cvterm collection link to be added")
	assert.True(
		slices.Equal(freshTermLink.Analyzers, []string{fullSearchIdentityAnalyzer}) ||
			len(freshTermLink.Analyzers) == 0,
		"expect cvterm link analyzer reconciled to identity default",
	)
	labelAnalyzers := slices.Clone(freshTermLink.Fields[docFieldLabel].Analyzers)
	slices.Sort(labelAnalyzers)
	assert.Equal(
		[]string{
			fullSearchNgramAnalyzer,
			fullSearchNormAnalyzer,
			fullSearchTextAnalyzer,
		},
		labelAnalyzers,
	)
}

func TestEnsureFullSearchDoesNotCreateCustomTextEnAnalyzer(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	ctx := context.Background()
	dbh := repo.Dbh().Handler()

	anl, err := dbh.Analyzer(ctx, fullSearchTextAnalyzer)
	assert.NoError(err)
	assert.Equal(driver.ArangoSearchAnalyzerTypeText, anl.Type())
}
