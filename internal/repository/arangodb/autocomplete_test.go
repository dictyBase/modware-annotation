package arangodb

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	E "github.com/IBM/fp-go/v2/either"
	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/arangomanager/testarango"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	repository "github.com/dictyBase/modware-annotation/internal/repository"
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

const (
	// autocompleteWait bounds how long a test waits for an ArangoSearch view
	// commit to expose the documents it just wrote.
	autocompleteWait = 30 * time.Second
	// autocompletePoll is the interval between two view-commit polls.
	autocompletePoll = 250 * time.Millisecond
	// fieldDescription is a dicty_annotation tag that this feature must
	// never search. It proves the tag filter keeps other tags out of the
	// result even when their values match.
	fieldDescription = "description"
)

// addStrainAnnotation stores one annotation through the repository, so the
// ArangoSearch view indexes it exactly the way production writes do.
func addStrainAnnotation(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	entryID, tag, value string,
	rank int64,
) *model.AnnoDoc {
	t.Helper()
	doc, err := repo.AddAnnotation(&annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:         value,
				EditableValue: value,
				CreatedBy:     "siddbasu@gmail.com",
				Tag:           tag,
				Ontology:      dictyAnnotationOntology,
				EntryId:       entryID,
				Rank:          rank,
			},
		},
	})
	require.NoErrorf(t, err, "expect annotation %s/%s to be stored", entryID, tag)

	return doc
}

// autocompleteOnce runs the repository pipeline once and returns its rows and
// error without failing the test, so a poller can retry until the view commits.
func autocompleteOnce(
	repo repository.TaggedAnnotationRepository,
	params *repository.AutocompleteQuery,
) ([]*repository.Suggestion, error) {
	either := ioeutils.ToEither(repo.AutocompleteAnnotation(params))

	return E.UnwrapError(either)
}

// waitForSuggestions polls the search until expect accepts a result. An
// ArangoSearch view serves a document only after its next commit, so a write
// that must be visible is polled rather than asserted immediately.
func waitForSuggestions(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	query string,
	limit int,
	expect func([]*repository.Suggestion) bool,
) []*repository.Suggestion {
	t.Helper()
	params := &repository.AutocompleteQuery{Query: query, Limit: limit}
	var rows []*repository.Suggestion
	var lastErr error

	require.Eventually(t, func() bool {
		rows, lastErr = autocompleteOnce(repo, params)
		return lastErr == nil && expect(rows)
	}, autocompleteWait, autocompletePoll,
		"expect autocomplete %q to return the expected rows", query)
	require.NoError(t, lastErr, "expect the autocomplete pipeline to succeed")

	return rows
}

// waitForNoSuggestions polls the search until the query matches nothing.
func waitForNoSuggestions(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	query string,
) {
	t.Helper()
	matched := func(rows []*repository.Suggestion) bool { return len(rows) == 0 }
	waitForSuggestions(t, repo, query, 5, matched)
}

func TestBuildAutocompleteQueryBranchCount(t *testing.T) {
	t.Parallel()
	statement := buildAutocompleteQuery()

	for _, name := range []string{
		"p_synonym", "p_genotype", "n_synonym", "n_genotype",
	} {
		require.Containsf(
			t,
			statement,
			"LET "+name+" = (",
			"expect branch %s in the statement",
			name,
		)
	}
	require.Equal(
		t,
		4,
		strings.Count(statement, "FOR d IN "+autocompleteViewName),
		"expect one view scan per branch",
	)
	require.Equal(
		t,
		4,
		strings.Count(statement, "OUTBOUND d GRAPH @anno_cvterm_graph"),
		"expect one tag traversal per branch",
	)
	require.Equal(
		t,
		1,
		strings.Count(statement, autocompleteDelimAnalyzer),
		"expect the delimiter analyzer on the genotype prefix branch only",
	)
	require.Equal(
		t,
		2,
		strings.Count(statement, "NGRAM_MATCH(d.value"),
		"expect two fuzzy branches",
	)
	require.NotContains(
		t,
		statement,
		"@@cv_collection",
		"expect no cvterm collection scan",
	)
}

func TestBuildAutocompleteQueryFilterOrder(t *testing.T) {
	t.Parallel()
	statement := buildAutocompleteQuery()

	// Each branch ends where the next statement block begins, so the five
	// block markers slice the statement into the four branch bodies.
	markers := []string{
		"LET p_synonym = (",
		"LET p_genotype = (",
		"LET n_synonym = (",
		"LET n_genotype = (",
		"LET hits = FLATTEN(",
	}

	checked := 0
	for i := range 4 {
		start := strings.Index(statement, markers[i])
		end := strings.Index(statement, markers[i+1])
		require.NotEqual(t, -1, start, "expect %s", markers[i])
		require.NotEqual(t, -1, end, "expect %s", markers[i+1])
		branch := statement[start:end]
		checked++

		tagFilter := strings.Index(branch, "FILTER has_tag == true")
		sortNode := strings.Index(branch, "SORT BM25(d)")
		limitNode := strings.Index(branch, "LIMIT @limit")
		require.NotEqual(t, -1, tagFilter, "expect the tag filter in %s", markers[i])
		require.NotEqual(t, -1, sortNode, "expect the sort in %s", markers[i])
		require.NotEqual(t, -1, limitNode, "expect the limit in %s", markers[i])
		require.Less(
			t,
			tagFilter,
			sortNode,
			"expect the tag filter before the sort in %s",
			markers[i],
		)
		require.Less(
			t,
			sortNode,
			limitNode,
			"expect the sort before the limit in %s",
			markers[i],
		)
	}
	require.Equal(t, 4, checked, "expect four branches to be checked")
}

func TestBuildAutocompleteQueryIsValidAQL(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	ctx := context.Background()
	assert.NoError(
		repo.Dbh().Handler().ValidateQuery(ctx, buildAutocompleteQuery()),
	)
}

func TestAutocompleteAnnotationSynonymPrefix(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(t, repo, "DBS0236126", fieldSynonym, "gammaS13", 0)

	rows := waitForSuggestions(t, repo, "gammas", 5, oneSuggestion)
	assert.Equal("DBS0236126", rows[0].EntryID)
	assert.Equal(fieldSynonym, rows[0].Field)
	assert.Equal("gammaS13", rows[0].DisplayText)
	assert.GreaterOrEqual(rows[0].Score, float64(1000))
}

func TestAutocompleteAnnotationGenotypePrefix(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(
		t,
		repo,
		"DBS0236127",
		fieldGenotype,
		"axeA1, axeB1, sadA-",
		0,
	)

	rows := waitForSuggestions(t, repo, "axea", 5, oneSuggestion)
	assert.Equal("DBS0236127", rows[0].EntryID)
	assert.Equal(fieldGenotype, rows[0].Field)
	assert.Equal("axeA1, axeB1, sadA-", rows[0].DisplayText)
	assert.GreaterOrEqual(rows[0].Score, float64(1000))
}

func TestAutocompleteAnnotationGenotypeInteriorAllelePrefix(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(
		t,
		repo,
		"DBS0236127",
		fieldGenotype,
		"axeA1, axeB1, sadA-",
		0,
	)

	rows := waitForSuggestions(t, repo, "sada", 5, oneSuggestion)
	assert.Equal("DBS0236127", rows[0].EntryID)
	assert.Equal(fieldGenotype, rows[0].Field)
	assert.Equal("axeA1, axeB1, sadA-", rows[0].DisplayText)
	assert.GreaterOrEqual(rows[0].Score, float64(1000))
}

func TestAutocompleteAnnotationTypoFuzzy(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(t, repo, "DBS0236126", fieldSynonym, "gammaS13", 0)

	rows := waitForSuggestions(t, repo, "gammas14", 5, oneSuggestion)
	assert.Equal("DBS0236126", rows[0].EntryID)
	assert.Less(
		rows[0].Score,
		float64(1000),
		"expect a fuzzy match to score below a prefix match",
	)
}

func TestAutocompleteAnnotationAccentedAndUppercaseQuery(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(t, repo, "DBS0236126", fieldSynonym, "gammaS13", 0)

	rows := waitForSuggestions(t, repo, "  G\u00c1MMAS13  ", 5, oneSuggestion)
	assert.Equal("DBS0236126", rows[0].EntryID)
	assert.Equal("gammaS13", rows[0].DisplayText)
}

func TestAutocompleteAnnotationSkipsObsoleteVersion(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	doc := addStrainAnnotation(
		t,
		repo,
		"DBS0236128",
		fieldSynonym,
		"oldAlias",
		0,
	)
	waitForSuggestions(t, repo, "oldalias", 5, oneSuggestion)

	_, err := repo.EditAnnotation(&annotation.TaggedAnnotationUpdate{
		Data: &annotation.TaggedAnnotationUpdate_Data{
			Id: doc.Key,
			Attributes: &annotation.TaggedAnnotationUpdateAttributes{
				Value:         "newAlias",
				EditableValue: "newAlias",
				CreatedBy:     "siddbasu@gmail.com",
			},
		},
	})
	assert.NoError(err, "expect the annotation edit to succeed")

	rows := waitForSuggestions(t, repo, "newalias", 5, oneSuggestion)
	assert.Equal("DBS0236128", rows[0].EntryID)
	assert.Equal("newAlias", rows[0].DisplayText)
	waitForNoSuggestions(t, repo, "oldalias")
}

func TestAutocompleteAnnotationIgnoresNonStrainAndOtherTagsUnderSmallLimit(
	t *testing.T,
) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	for i := range 10 {
		entryID := fmt.Sprintf("DDB_G028642%d", i)
		addStrainAnnotation(
			t,
			repo,
			entryID,
			fieldSynonym,
			"xyloseMutant",
			0,
		)
	}
	for i := range 10 {
		entryID := fmt.Sprintf("DBS023620%d", i)
		addStrainAnnotation(
			t,
			repo,
			entryID,
			fieldDescription,
			"xyloseMutant",
			0,
		)
	}
	addStrainAnnotation(t, repo, "DBS0236999", fieldSynonym, "xyloseMutant", 0)

	rows := waitForSuggestions(t, repo, "xylose", 1, oneSuggestion)
	assert.Equal("DBS0236999", rows[0].EntryID)
	assert.Equal(fieldSynonym, rows[0].Field)
}

func TestAutocompleteAnnotationDeduplicatesMultipleSynonymsAndCrossFieldHits(
	t *testing.T,
) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(t, repo, "DBS0236129", fieldSynonym, "sadA-null", 0)
	addStrainAnnotation(t, repo, "DBS0236129", fieldSynonym, "sadA-ko", 1)
	addStrainAnnotation(
		t,
		repo,
		"DBS0236129",
		fieldGenotype,
		"sadA-[sadA-KO]",
		2,
	)

	rows := waitForSuggestions(t, repo, "sada", 5, oneSuggestion)
	assert.Equal("DBS0236129", rows[0].EntryID)
}

func TestAutocompleteAnnotationDefaultLimitIsFive(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	for i := range 8 {
		entryID := fmt.Sprintf("DBS02361%02d", i)
		addStrainAnnotation(
			t,
			repo,
			entryID,
			fieldSynonym,
			fmt.Sprintf("limitDefault%d", i),
			0,
		)
	}

	rows := waitForSuggestions(t, repo, "limitdefault", 0, func(
		rows []*repository.Suggestion,
	) bool {
		return len(rows) == defaultAutocompleteLimit
	})
	assert.Len(rows, defaultAutocompleteLimit)
}

func TestAutocompleteAnnotationLimitCapIsFifty(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	for i := range 55 {
		entryID := fmt.Sprintf("DBS02362%03d", i)
		addStrainAnnotation(
			t,
			repo,
			entryID,
			fieldSynonym,
			fmt.Sprintf("limitCap%d", i),
			0,
		)
	}

	rows := waitForSuggestions(t, repo, "limitcap", 500, func(
		rows []*repository.Suggestion,
	) bool {
		return len(rows) == maxAutocompleteLimit
	})
	assert.Len(rows, maxAutocompleteLimit)
}

func TestAutocompleteAnnotationRankingUnderSmallLimit(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	addStrainAnnotation(t, repo, "DBS0237001", fieldSynonym, "rankAlphaOne", 0)
	for i, value := range []string{
		"rankAlphbOne",
		"rankAlphcOne",
		"rankAlphdOne",
		"rankAlpheOne",
		"rankAlphfOne",
		"rankAlphgOne",
	} {
		entryID := fmt.Sprintf("DBS023700%d", i+2)
		addStrainAnnotation(t, repo, entryID, fieldSynonym, value, 0)
	}

	rows := waitForSuggestions(t, repo, "rankalpha", 3, func(
		rows []*repository.Suggestion,
	) bool {
		return len(rows) == 3
	})
	assert.Equal(
		"DBS0237001",
		rows[0].EntryID,
		"expect the prefix match ahead of the fuzzy matches",
	)
}

func TestAutocompleteAnnotationEmptyResultIsNotAnError(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	rows, err := autocompleteOnce(
		repo,
		&repository.AutocompleteQuery{Query: "zzzqqq", Limit: 5},
	)
	assert.NoError(err, "expect an unmatched query to succeed")
	assert.NotNil(rows, "expect an empty slice rather than a nil slice")
	assert.Empty(rows)
}

func TestAutocompleteAnnotationRejectsNilAndWhitespaceQuery(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	_, err := autocompleteOnce(repo, nil)
	assert.Error(err, "expect a nil query to be rejected")

	_, err = autocompleteOnce(
		repo,
		&repository.AutocompleteQuery{Query: "   ", Limit: 5},
	)
	assert.Error(err, "expect a whitespace-only query to be rejected")
}

// oneSuggestion is the common expectation of the single-hit tests.
func oneSuggestion(rows []*repository.Suggestion) bool {
	return len(rows) == 1
}
