package arangodb

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	E "github.com/IBM/fp-go/v2/either"
	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/arangomanager/testarango"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
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

type seedTermSpec struct {
	id         string
	label      string
	deprecated bool
}

// seedFullSearchOntologies loads dicty_phenotypes.json and inserts the test
// ontologies and cvterm documents needed to exercise all 28 full search branches.
func seedFullSearchOntologies(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	assert := require.New(t)

	fh, err := oboReader()
	assert.NoError(err)
	defer func() { _ = fh.Close() }()
	_, err = repo.LoadOboJSON(bufio.NewReader(fh))
	assert.NoError(err)

	ar, ok := repo.(*arangorepository)
	assert.True(ok, "expect concrete arangorepository")
	ctx := context.Background()

	// Find the loaded Dicty Phenotypes cv._id so additional test phenotype
	// terms can be attached to the same ontology graph_id.
	var phenoGraphID string
	row, err := ar.database.GetRow(
		`FOR cv IN @@cv_collection FILTER cv.metadata.namespace == @onto LIMIT 1 RETURN cv._id`,
		map[string]any{
			cvCollectionBind: ar.onto.Cv.Name(),
			"onto":           phenotypeOntology,
		},
	)
	assert.NoError(err)
	assert.False(row.IsEmpty(), "expect Dicty Phenotypes cv document")
	assert.NoError(row.Read(&phenoGraphID))

	extraPhenoTerms := []seedTermSpec{
		{
			id:         "DDPHENO:9990001",
			label:      "aberrant fruiting body morphogenesis",
			deprecated: false,
		},
		{
			id:         "DDPHENO:9990002",
			label:      "popularpheno phenotype",
			deprecated: false,
		},
		{
			id:         "DDPHENO:9990003",
			label:      "aberrant cordax development",
			deprecated: false,
		},
	}
	for _, term := range extraPhenoTerms {
		_, createErr := ar.onto.Term.CreateDocument(ctx, map[string]any{
			"graph_id":   phenoGraphID,
			"id":         term.id,
			"label":      term.label,
			"deprecated": term.deprecated,
		})
		assert.NoError(createErr)
	}

	customOntologies := []struct {
		namespace string
		terms     []seedTermSpec
	}{
		{
			namespace: mutagenesisOntology,
			terms: []seedTermSpec{
				{
					id:         "DDMUT:0000001",
					label:      tagMutagenesisMethod,
					deprecated: false,
				},
			},
		},
		{
			namespace: environmentOntology,
			terms: []seedTermSpec{
				{
					id:         "DDENV:0000001",
					label:      "in suspension",
					deprecated: false,
				},
			},
		},
		{
			namespace: assayOntology,
			terms: []seedTermSpec{
				{
					id:         "DDASSAY:0000001",
					label:      "confocal microscopy",
					deprecated: false,
				},
			},
		},
		{
			namespace: characteristicOntology,
			terms: []seedTermSpec{
				{
					id:         "DDCHAR:0000001",
					label:      "axenic",
					deprecated: false,
				},
			},
		},
	}

	for _, onto := range customOntologies {
		cvMeta, cvErr := ar.onto.Cv.CreateDocument(ctx, map[string]any{
			"metadata": map[string]any{
				"namespace": onto.namespace,
			},
		})
		assert.NoError(cvErr)
		for _, term := range onto.terms {
			_, termErr := ar.onto.Term.CreateDocument(ctx, map[string]any{
				"graph_id":   cvMeta.ID.String(),
				"id":         term.id,
				"label":      term.label,
				"deprecated": term.deprecated,
			})
			assert.NoError(termErr)
		}
	}
}

func addFullSearchAnnotation(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	entryID, ontology, tag, value string,
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
				Ontology:      ontology,
				EntryId:       entryID,
				Rank:          rank,
			},
		},
	})
	require.NoErrorf(
		t,
		err,
		"expect annotation %s/%s/%s to be stored",
		entryID,
		ontology,
		tag,
	)

	return doc
}

func fullSearchOnce(
	repo repository.TaggedAnnotationRepository,
	params *repository.FullSearchQuery,
) ([]*repository.FullSearchResult, error) {
	either := ioeutils.ToEither(repo.SearchAnnotation(params))

	return E.UnwrapError(either)
}

func waitForFullSearchResults(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	query string,
	limit int,
	expect func([]*repository.FullSearchResult) bool,
) []*repository.FullSearchResult {
	t.Helper()
	params := &repository.FullSearchQuery{Query: query, Limit: limit}
	var rows []*repository.FullSearchResult
	var lastErr error

	require.Eventually(t, func() bool {
		rows, lastErr = fullSearchOnce(repo, params)
		return lastErr == nil && expect(rows)
	}, autocompleteWait, autocompletePoll,
		"expect full search %q to return the expected rows", query)
	require.NoError(t, lastErr, "expect the full search pipeline to succeed")

	return rows
}

func waitForNoFullSearchResults(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	query string,
) {
	t.Helper()
	matched := func(rows []*repository.FullSearchResult) bool {
		return len(rows) == 0
	}
	waitForFullSearchResults(t, repo, query, defaultFullSearchLimit, matched)
}

func oneFullSearchResult(rows []*repository.FullSearchResult) bool {
	return len(rows) == 1
}

func TestBuildFullSearchQueryBranchCount(t *testing.T) {
	t.Parallel()
	statement := buildFullSearchQuery()

	for i := range 9 {
		pName := fmt.Sprintf("p%d", i)
		nName := fmt.Sprintf("n%d", i)
		require.Containsf(t, statement, "LET "+pName+" = (", "expect branch %s", pName)
		require.Containsf(t, statement, "LET "+nName+" = (", "expect branch %s", nName)
	}
	for i := range 5 {
		tName := fmt.Sprintf("t%d", i)
		hName := fmt.Sprintf("h%d", i)
		require.Containsf(t, statement, "LET "+tName+" = (", "expect branch %s", tName)
		require.Containsf(t, statement, "LET "+hName+" = (", "expect branch %s", hName)
	}

	require.Equal(
		t,
		28,
		strings.Count(statement, "FOR d IN "+fullSearchViewName),
		"expect 28 view scans",
	)
	require.Equal(
		t,
		12,
		strings.Count(statement, "OUTBOUND d GRAPH @anno_cvterm_graph"),
		"expect 12 OUTBOUND traversals from annotation to cvterm",
	)
	require.Equal(
		t,
		16,
		strings.Count(statement, "INBOUND d GRAPH @anno_cvterm_graph"),
		"expect 16 INBOUND traversals from cvterm to annotation",
	)
	require.Equal(
		t,
		1,
		strings.Count(statement, fullSearchDelimAnalyzer),
		"expect annotation_search_delim on p1 genotype prefix branch only",
	)
	require.Equal(
		t,
		9,
		strings.Count(statement, "NGRAM_MATCH("),
		"expect 9 fuzzy NGRAM_MATCH branches",
	)
	require.Equal(
		t,
		5,
		strings.Count(statement, "IN TOKENS(@q"),
		"expect 5 token branches",
	)
	require.Equal(
		t,
		5,
		strings.Count(statement, "PHRASE("),
		"expect 5 phrase branches",
	)
	require.NotContains(
		t,
		statement,
		"@@cv_collection",
		"expect 0 @@cv_collection scans in the main search statement",
	)
}

func TestBuildFullSearchQueryBandCounts(t *testing.T) {
	t.Parallel()
	statement := buildFullSearchQuery()

	require.Equal(
		t,
		9,
		strings.Count(statement, "s: "+fullSearchPrefixBand),
		"expect 9 prefix band branches",
	)
	require.Equal(
		t,
		5,
		strings.Count(statement, "s: "+fullSearchPhraseBand),
		"expect 5 phrase band branches",
	)
	require.Equal(
		t,
		5,
		strings.Count(statement, "s: "+fullSearchTokenBand),
		"expect 5 token band branches",
	)
	require.Equal(
		t,
		9,
		strings.Count(statement, "s: "+fullSearchFuzzyBand),
		"expect 9 fuzzy band branches",
	)
}

func TestBuildFullSearchQueryFilterOrder(t *testing.T) {
	t.Parallel()
	statement := buildFullSearchQuery()

	markers := []string{
		"LET p0 = (", "LET p1 = (", "LET p2 = (", "LET p3 = (", "LET p4 = (",
		"LET p5 = (", "LET p6 = (", "LET p7 = (", "LET p8 = (",
		"LET n0 = (", "LET n1 = (", "LET n2 = (", "LET n3 = (", "LET n4 = (",
		"LET n5 = (", "LET n6 = (", "LET n7 = (", "LET n8 = (",
		"LET t0 = (", "LET t1 = (", "LET t2 = (", "LET t3 = (", "LET t4 = (",
		"LET h0 = (", "LET h1 = (", "LET h2 = (", "LET h3 = (", "LET h4 = (",
		"LET hits = FLATTEN(",
	}

	annoBranchSet := map[string]bool{
		"LET p0 = (": true, "LET p1 = (": true, "LET p2 = (": true,
		"LET p3 = (": true, "LET p4 = (": true,
		"LET n0 = (": true, "LET n1 = (": true, "LET n2 = (": true,
		"LET n3 = (": true, "LET n4 = (": true,
		"LET t0 = (": true, "LET h0 = (": true,
	}

	for i := range 28 {
		start := strings.Index(statement, markers[i])
		end := strings.Index(statement, markers[i+1])
		require.NotEqual(t, -1, start, "expect marker %s", markers[i])
		require.NotEqual(t, -1, end, "expect marker %s", markers[i+1])
		branch := statement[start:end]

		sortNode := strings.Index(branch, "SORT BM25(d)")
		limitNode := strings.Index(branch, "LIMIT @limit")
		require.NotEqual(t, -1, sortNode, "expect SORT BM25(d) in %s", markers[i])
		require.NotEqual(t, -1, limitNode, "expect LIMIT @limit in %s", markers[i])
		require.Less(t, sortNode, limitNode, "expect SORT before LIMIT in %s", markers[i])

		if annoBranchSet[markers[i]] {
			tagFilter := strings.Index(branch, "FILTER has_tag == true")
			require.NotEqual(t, -1, tagFilter, "expect tag filter in %s", markers[i])
			require.Less(t, tagFilter, sortNode, "expect tag filter before SORT in %s", markers[i])
		} else {
			activeFilter := strings.Index(branch, "FILTER ann.is_obsolete == false")
			require.NotEqual(t, -1, activeFilter, "expect active strain filter in %s", markers[i])
			require.Less(t, activeFilter, sortNode, "expect active filter before SORT in %s", markers[i])
		}
	}
}

func TestBuildFullSearchQueryIsValidAQL(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	ctx := context.Background()
	assert.NoError(
		repo.Dbh().Handler().ValidateQuery(ctx, buildFullSearchQuery()),
	)
}

func TestSearchAnnotationValuePrefixFields(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236125", dictyAnnotationOntology, fieldSynonym, "gammaS13", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236126", dictyAnnotationOntology, fieldGenotype, "axeA1, axeB1, sadA-[sadA-KO]", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236130", dictyAnnotationOntology, tagSystematicName, "AX4-sys", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236131", dictyAnnotationOntology, tagMutantType, "endogenous insertion", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236132", mutagenesisOntology, tagMutagenesisMethod, "Restriction Enzyme-Mediated Integration", 0,
	)

	cases := []struct {
		query   string
		entryID string
		field   string
		display string
	}{
		{query: "gammas", entryID: "DBS0236125", field: fieldSynonym, display: "gammaS13"},
		{query: "axea", entryID: "DBS0236126", field: fieldGenotype, display: "axeA1, axeB1, sadA-[sadA-KO]"},
		{query: "sada", entryID: "DBS0236126", field: fieldGenotype, display: "axeA1, axeB1, sadA-[sadA-KO]"},
		{query: "ax4", entryID: "DBS0236130", field: fieldSystematicName, display: "AX4-sys"},
		{query: "endogenous", entryID: "DBS0236131", field: fieldGeneticModification, display: "endogenous insertion"},
		{
			query:   "restriction",
			entryID: "DBS0236132",
			field:   fieldMutagenesisMethod,
			display: "Restriction Enzyme-Mediated Integration",
		},
	}

	for _, tc := range cases {
		rows := waitForFullSearchResults(t, repo, tc.query, 10, func(rows []*repository.FullSearchResult) bool {
			return len(rows) >= 1 && rows[0].EntryID == tc.entryID
		})
		assert.Equal(tc.entryID, rows[0].EntryID)
		assert.Equal(tc.field, rows[0].Field)
		assert.Equal(tc.display, rows[0].DisplayText)
		assert.GreaterOrEqual(rows[0].Score, float64(1000))
	}
}

func TestSearchAnnotationOntologyTermPrefixFields(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236140", phenotypeOntology, "aberrant fruiting body morphogenesis", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236141", environmentOntology, "in suspension", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236142", assayOntology, "confocal microscopy", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236143", characteristicOntology, "axenic", "novalue", 0,
	)

	cases := []struct {
		query   string
		entryID string
		field   string
		display string
	}{
		{query: "aberrant", entryID: "DBS0236140", field: fieldPhenotype, display: "aberrant fruiting body morphogenesis"},
		{query: "in suspension", entryID: "DBS0236141", field: fieldEnvironment, display: "in suspension"},
		{query: "confocal", entryID: "DBS0236142", field: fieldAssay, display: "confocal microscopy"},
		{query: "axenic", entryID: "DBS0236143", field: fieldCharacteristic, display: "axenic"},
	}

	for _, tc := range cases {
		rows := waitForFullSearchResults(t, repo, tc.query, 10, func(rows []*repository.FullSearchResult) bool {
			return len(rows) >= 1 && rows[0].EntryID == tc.entryID
		})
		assert.Equal(tc.entryID, rows[0].EntryID)
		assert.Equal(tc.field, rows[0].Field)
		assert.Equal(tc.display, rows[0].DisplayText)
		assert.GreaterOrEqual(rows[0].Score, float64(1000))
	}
}

func TestSearchAnnotationPublicNotePhraseAndToken(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236150", dictyAnnotationOntology, tagPublicNote,
		"mutant forms culminants under starvation", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236151", dictyAnnotationOntology, tagPublicNote,
		"culminants appear in mutant forms", 0,
	)
	_, err := repo.AddAnnotation(&annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:         "normal note",
				EditableValue: "slateonlymarker",
				CreatedBy:     "siddbasu@gmail.com",
				Tag:           tagPublicNote,
				Ontology:      dictyAnnotationOntology,
				EntryId:       "DBS0236160",
				Rank:          0,
			},
		},
	})
	assert.NoError(err)

	rows := waitForFullSearchResults(
		t, repo, "forms culminants", 10,
		func(rows []*repository.FullSearchResult) bool {
			return len(rows) == 2
		},
	)
	assert.Equal("DBS0236150", rows[0].EntryID)
	assert.Equal(fieldPublicNote, rows[0].Field)
	assert.GreaterOrEqual(rows[0].Score, float64(500))

	assert.Equal("DBS0236151", rows[1].EntryID)
	assert.Equal(fieldPublicNote, rows[1].Field)
	assert.GreaterOrEqual(rows[1].Score, float64(250))
	assert.Less(rows[1].Score, float64(500))

	waitForNoFullSearchResults(t, repo, "slateonlymarker")
}

func TestSearchAnnotationOntologyTermPhraseAndToken(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236140", phenotypeOntology, "aberrant fruiting body morphogenesis", "novalue", 0,
	)

	phraseRows := waitForFullSearchResults(
		t, repo, "fruiting body", 10, oneFullSearchResult,
	)
	assert.Equal("DBS0236140", phraseRows[0].EntryID)
	assert.Equal(fieldPhenotype, phraseRows[0].Field)
	assert.Equal("aberrant fruiting body morphogenesis", phraseRows[0].DisplayText)
	assert.GreaterOrEqual(phraseRows[0].Score, float64(500))

	tokenRows := waitForFullSearchResults(
		t, repo, "morphogenesis", 10, oneFullSearchResult,
	)
	assert.Equal("DBS0236140", tokenRows[0].EntryID)
	assert.Equal(fieldPhenotype, tokenRows[0].Field)
	assert.Equal("aberrant fruiting body morphogenesis", tokenRows[0].DisplayText)
	assert.GreaterOrEqual(tokenRows[0].Score, float64(250))
}

func TestSearchAnnotationBandOrdering(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236181", dictyAnnotationOntology, fieldSynonym,
		"slug migration-A", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236182", dictyAnnotationOntology, tagPublicNote,
		"aberrant slug migration under light", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236183", dictyAnnotationOntology, tagPublicNote,
		"migration impaired during slug development", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236184", dictyAnnotationOntology, fieldSynonym,
		"slug migratn", 0,
	)

	rows := waitForFullSearchResults(
		t, repo, "slug migration", 10,
		func(rows []*repository.FullSearchResult) bool {
			return len(rows) == 4
		},
	)
	assert.Equal("DBS0236181", rows[0].EntryID)
	assert.GreaterOrEqual(rows[0].Score, float64(1000))

	assert.Equal("DBS0236182", rows[1].EntryID)
	assert.GreaterOrEqual(rows[1].Score, float64(500))
	assert.Less(rows[1].Score, float64(1000))

	assert.Equal("DBS0236183", rows[2].EntryID)
	assert.GreaterOrEqual(rows[2].Score, float64(250))
	assert.Less(rows[2].Score, float64(500))

	assert.Equal("DBS0236184", rows[3].EntryID)
	assert.Less(rows[3].Score, float64(250))
}

func TestSearchAnnotationSkipsObsoleteAndDeprecated(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	doc := addFullSearchAnnotation(
		t, repo, "DBS0236128", dictyAnnotationOntology, fieldSynonym, "oldAlias", 0,
	)
	waitForFullSearchResults(t, repo, "oldalias", 10, oneFullSearchResult)

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
	assert.NoError(err)

	rows := waitForFullSearchResults(t, repo, "newalias", 10, oneFullSearchResult)
	assert.Equal("DBS0236128", rows[0].EntryID)
	waitForNoFullSearchResults(t, repo, "oldalias")

	ar, ok := repo.(*arangorepository)
	assert.True(ok)
	ctx := context.Background()

	var phenoGraphID string
	row, err := ar.database.GetRow(
		`FOR cv IN @@cv_collection FILTER cv.metadata.namespace == @onto LIMIT 1 RETURN cv._id`,
		map[string]any{
			cvCollectionBind: ar.onto.Cv.Name(),
			"onto":           phenotypeOntology,
		},
	)
	assert.NoError(err)
	assert.NoError(row.Read(&phenoGraphID))

	termMeta, err := ar.onto.Term.CreateDocument(ctx, map[string]any{
		"graph_id":   phenoGraphID,
		"id":         "DDPHENO:9990099",
		"label":      "obsoletephenoterm",
		"deprecated": false,
	})
	assert.NoError(err)

	addFullSearchAnnotation(
		t, repo, "DBS0236161", phenotypeOntology, "obsoletephenoterm", "novalue", 0,
	)
	waitForFullSearchResults(t, repo, "obsoletephenoterm", 10, oneFullSearchResult)

	_, err = ar.onto.Term.UpdateDocument(ctx, termMeta.Key, map[string]any{
		"deprecated": true,
	})
	assert.NoError(err)
	waitForNoFullSearchResults(t, repo, "obsoletephenoterm")
}

func TestSearchAnnotationIgnoresNonStrainAndOtherTagsUnderSmallLimit(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	for i := range 10 {
		addFullSearchAnnotation(
			t, repo, fmt.Sprintf("DDB_G028642%d", i),
			dictyAnnotationOntology, fieldSynonym, "xyloseMutant", 0,
		)
		addFullSearchAnnotation(
			t, repo, fmt.Sprintf("DBP028642%d", i),
			dictyAnnotationOntology, fieldSynonym, "xyloseMutant", 0,
		)
		addFullSearchAnnotation(
			t, repo, fmt.Sprintf("DBS023620%d", i),
			dictyAnnotationOntology, fieldDescription, "xyloseMutant", 0,
		)
		addFullSearchAnnotation(
			t, repo, fmt.Sprintf("DBS023630%d", i),
			dictyAnnotationOntology, "private note", "xyloseMutant", 0,
		)
	}
	addFullSearchAnnotation(
		t, repo, "DBS0236999", dictyAnnotationOntology, fieldSynonym, "xyloseMutant", 0,
	)

	rows := waitForFullSearchResults(t, repo, "xylose", 1, oneFullSearchResult)
	assert.Equal("DBS0236999", rows[0].EntryID)
	assert.Equal(fieldSynonym, rows[0].Field)
}

func TestSearchAnnotationHighFanOutTermRespectsLimitAndOrder(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	for i := range 55 {
		entryID := fmt.Sprintf("DBS0239%03d", i)
		addFullSearchAnnotation(
			t, repo, entryID, phenotypeOntology, "popularpheno phenotype", "novalue", 0,
		)
	}

	rows := waitForFullSearchResults(
		t, repo, "popularpheno", defaultFullSearchLimit,
		func(rows []*repository.FullSearchResult) bool {
			return len(rows) == defaultFullSearchLimit
		},
	)
	assert.Len(rows, defaultFullSearchLimit)
	for i := range defaultFullSearchLimit {
		expectedID := fmt.Sprintf("DBS0239%03d", i)
		assert.Equal(expectedID, rows[i].EntryID)
		assert.Equal(fieldPhenotype, rows[i].Field)
		assert.GreaterOrEqual(rows[i].Score, float64(1000))
	}
}

func TestSearchAnnotationCrossCollectionDeduplicationKeepsBestField(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236170", dictyAnnotationOntology, fieldSynonym, "cordaxStrain", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236170", phenotypeOntology, "aberrant cordax development", "novalue", 1,
	)

	rows := waitForFullSearchResults(t, repo, "cordax", 10, oneFullSearchResult)
	assert.Equal("DBS0236170", rows[0].EntryID)
	assert.Equal(fieldSynonym, rows[0].Field)
	assert.Equal("cordaxStrain", rows[0].DisplayText)
	assert.GreaterOrEqual(rows[0].Score, float64(1000))
}

func TestSearchAnnotationSucceedsWhenOptionalOntologiesAreUnloaded(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236125", dictyAnnotationOntology, fieldSynonym, "gammaS13", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236150", dictyAnnotationOntology, tagPublicNote,
		"mutant forms culminants under starvation", 0,
	)

	synRows := waitForFullSearchResults(t, repo, "gammas", 10, oneFullSearchResult)
	assert.Equal("DBS0236125", synRows[0].EntryID)
	assert.Equal(fieldSynonym, synRows[0].Field)

	noteRows := waitForFullSearchResults(
		t, repo, "forms culminants", 10, oneFullSearchResult,
	)
	assert.Equal("DBS0236150", noteRows[0].EntryID)
	assert.Equal(fieldPublicNote, noteRows[0].Field)
}

func TestSearchAnnotationEmptyWhenNoOntologiesAreLoaded(t *testing.T) {
	assert := require.New(t)
	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err)

	repo, err := NewTaggedAnnotationRepo(
		GetConnectParamsFromDB(tra),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoError(err)
	defer tearDown(repo)

	rows, err := fullSearchOnce(
		repo,
		&repository.FullSearchQuery{Query: "gammas", Limit: 10},
	)
	assert.NoError(err)
	assert.NotNil(rows)
	assert.Empty(rows)
}

func TestSearchAnnotationDefaultAndClampedLimit(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	for i := range 55 {
		entryID := fmt.Sprintf("DBS0238%03d", i)
		addFullSearchAnnotation(
			t, repo, entryID, dictyAnnotationOntology, fieldSynonym,
			fmt.Sprintf("limitProbe%d", i), 0,
		)
	}

	defaultRows := waitForFullSearchResults(
		t, repo, "limitprobe", 0,
		func(rows []*repository.FullSearchResult) bool {
			return len(rows) == defaultFullSearchLimit
		},
	)
	assert.Len(defaultRows, defaultFullSearchLimit)

	clampedRows := waitForFullSearchResults(
		t, repo, "limitprobe", 500,
		func(rows []*repository.FullSearchResult) bool {
			return len(rows) == maxFullSearchLimit
		},
	)
	assert.Len(clampedRows, maxFullSearchLimit)
}

func TestSearchAnnotationEmptyAndPunctuationResultIsNotAnError(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	seedFullSearchOntologies(t, repo)

	rows, err := fullSearchOnce(
		repo,
		&repository.FullSearchQuery{Query: "zzzqqq", Limit: 10},
	)
	assert.NoError(err)
	assert.NotNil(rows)
	assert.Empty(rows)

	punctRows, err := fullSearchOnce(
		repo,
		&repository.FullSearchQuery{Query: "---", Limit: 10},
	)
	assert.NoError(err)
	assert.NotNil(punctRows)
	assert.Empty(punctRows)
}

func TestSearchAnnotationRejectsNilWhitespaceAndStopwordQuery(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	_, err := fullSearchOnce(repo, nil)
	assert.Error(err, "expect a nil query to be rejected")

	_, err = fullSearchOnce(
		repo,
		&repository.FullSearchQuery{Query: "   ", Limit: 10},
	)
	assert.Error(err, "expect a whitespace-only query to be rejected")

	_, err = fullSearchOnce(
		repo,
		&repository.FullSearchQuery{Query: "the and", Limit: 10},
	)
	assert.Error(err, "expect a stopword-only query to be rejected")
}

const (
	fullSearchCalibrationP95Ms    = 17.0
	fullSearchClientBaselineP95Ms = 20.0
)

func addFullSearchLatencyFixture(
	t *testing.T,
	ar *arangorepository,
	synonymID, genotypeID, noteID string,
) {
	t.Helper()
	ctx := context.Background()
	dbh := ar.database.Handler()

	annotColl, err := dbh.Collection(ctx, ar.anno.annot.Name())
	require.NoError(t, err)
	edgeColl, err := dbh.Collection(ctx, ar.anno.term.Name())
	require.NoError(t, err)

	docs := make([]map[string]any, 0, latencyFixtureDocuments+1)
	targets := make([]string, 0, latencyFixtureDocuments+1)
	for i := range latencyFixtureDocuments {
		target := synonymID
		value := fmt.Sprintf("zzfill%d allele", i)
		switch i % 3 {
		case 0:
			target = genotypeID
			value = fmt.Sprintf("zzfill%dA1, zzfill%dB2, zzfill%dC3", i, i, i)
		case 1:
			target = noteID
			value = fmt.Sprintf("filler note %d description text", i)
		}
		docs = append(docs, map[string]any{
			"value":          value,
			"editable_value": value,
			"created_by":     "siddbasu@gmail.com",
			"entry_id":       fmt.Sprintf("DBS02%05d", i),
			"rank":           0,
			"is_obsolete":    false,
			"version":        1,
			"created_at":     time.Now().UTC().Format(time.RFC3339),
		})
		targets = append(targets, target)
	}
	// One searchable probe document, so the measured statement returns a row.
	docs = append(docs, map[string]any{
		"value":          "gammaS13",
		"editable_value": "gammaS13",
		"created_by":     "siddbasu@gmail.com",
		"entry_id":       "DBS0236125",
		"rank":           0,
		"is_obsolete":    false,
		"version":        1,
		"created_at":     time.Now().UTC().Format(time.RFC3339),
	})
	targets = append(targets, synonymID)

	metas, docErrs, err := annotColl.CreateDocuments(ctx, docs)
	require.NoError(t, err)
	for _, docErr := range docErrs {
		require.NoError(t, docErr)
	}

	edges := make([]map[string]any, 0, len(metas))
	for i, meta := range metas {
		edges = append(edges, map[string]any{
			"_from": meta.ID.String(),
			"_to":   targets[i],
		})
	}
	_, edgeErrs, err := edgeColl.CreateDocuments(ctx, edges)
	require.NoError(t, err)
	for _, edgeErr := range edgeErrs {
		require.NoError(t, edgeErr)
	}
}

func TestSearchAnnotationExplainAndLatency(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)

	ar, isArangoRepo := repo.(*arangorepository)
	require.True(t, isArangoRepo, "expect the concrete arangodb repository")

	seedFullSearchOntologies(t, repo)

	synonymID, err := ar.termID(dictyAnnotationOntology, fieldSynonym)
	assert.NoError(err)
	genotypeID, err := ar.termID(dictyAnnotationOntology, fieldGenotype)
	assert.NoError(err)
	noteID, err := ar.termID(dictyAnnotationOntology, tagPublicNote)
	assert.NoError(err)

	addFullSearchLatencyFixture(t, ar, synonymID, genotypeID, noteID)
	waitForFullSearchResults(
		t,
		repo,
		"gammas",
		defaultFullSearchLimit,
		oneFullSearchResult,
	)

	ctx := context.Background()
	either := ioeutils.ToEither(ar.resolveFullSearchTargets(fullSearchRequest{}))
	targets, err := E.UnwrapError(either)
	assert.NoError(err)

	req := fullSearchRequest{
		query:   "gammas",
		limit:   defaultFullSearchLimit,
		targets: targets,
	}
	statement := buildFullSearchQuery()
	bindParams := ar.fullSearchBindParams(req)

	dbh := repo.Dbh().Handler()
	explained, err := dbh.ExplainQuery(ctx, statement, bindParams, nil)
	assert.NoError(err)

	counts := map[string]int{}
	for _, node := range explained.Plan.NodesRaw {
		nodeType, isString := node["type"].(string)
		if !isString {
			continue
		}
		counts[nodeType]++
	}
	assert.Equal(
		28,
		counts["EnumerateViewNode"],
		"expect 28 view scans (one per branch) in the explain plan",
	)
	assert.Equal(
		0,
		counts["EnumerateCollectionNode"],
		"expect no collection scan in the explain plan",
	)
	assert.Equal(
		28,
		counts["TraversalNode"],
		"expect 28 traversals (12 outbound + 16 inbound) in the explain plan",
	)

	timings := make([]float64, 0, latencyRuns)
	for range latencyRuns {
		started := time.Now()
		cursor, queryErr := dbh.Query(ctx, statement, bindParams)
		assert.NoError(queryErr)
		for cursor.HasMore() {
			var row fullSearchRow
			_, readErr := cursor.ReadDocument(ctx, &row)
			assert.NoError(readErr)
		}
		assert.NoError(cursor.Close())
		elapsed := time.Since(started)
		timings = append(timings, float64(elapsed.Microseconds())/1000)
	}
	slices.Sort(timings)
	p95Index := int(math.Ceil(float64(len(timings))*0.95)) - 1
	p95 := timings[p95Index]
	median := timings[len(timings)/2]
	ceiling := fullSearchClientBaselineP95Ms * latencyCeilingRatio
	t.Logf(
		"full search statement min %.2f ms median %.2f ms p95 %.2f ms"+
			" over %d runs (calibration baseline %.0f ms, client-path baseline"+
			" %.2f ms, ceiling %.2f ms)",
		timings[0],
		median,
		p95,
		len(timings),
		fullSearchCalibrationP95Ms,
		fullSearchClientBaselineP95Ms,
		ceiling,
	)

	assert.LessOrEqual(
		p95,
		ceiling,
		"expect the statement p95 within 125% of the client-path baseline",
	)
}
