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
	// ArangoDB omits an analyzer list equal to the inherited link default,
	// so is_obsolete inherits the identity analyzer set on the link itself.
	assert.Empty(
		link.Fields[docFieldIsObsolete].Analyzers,
		"expect is_obsolete to inherit the identity analyzer of the link",
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
