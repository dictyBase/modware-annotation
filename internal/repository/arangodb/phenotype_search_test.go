package arangodb

import (
	"context"
	"testing"

	E "github.com/IBM/fp-go/v2/either"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
)

type seedPhenotypeTermSpec struct {
	id         string
	label      string
	deprecated bool
	metadata   map[string]any
}

// seedPhenotypeSearchOntologies creates the 4 ontology graphs and the cvterm
// documents the phenotype search branches traverse: label, synonyms,
// definition, and comments fields plus the deprecated and non-phenotype cases.
func seedPhenotypeSearchOntologies(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
) map[string]string {
	t.Helper()
	assert := require.New(t)
	ar, ok := repo.(*arangorepository)
	assert.True(ok, "expect concrete arangorepository")
	ctx := context.Background()
	termKeys := make(map[string]string)

	graphIDs := make(map[string]string)
	for _, namespace := range []string{
		phenotypeOntology,
		environmentOntology,
		assayOntology,
		dictyAnnotationOntology,
	} {
		cvMeta, cvErr := ar.onto.Cv.CreateDocument(ctx, map[string]any{
			"metadata": map[string]any{"namespace": namespace},
		})
		assert.NoError(cvErr)
		graphIDs[namespace] = cvMeta.ID.String()
	}

	terms := []struct {
		namespace string
		term      seedPhenotypeTermSpec
	}{
		{
			namespace: phenotypeOntology,
			term: seedPhenotypeTermSpec{
				id:    "DDPHENO:9991001",
				label: "culminant mound formation",
				metadata: map[string]any{
					"synonyms": []map[string]any{
						{"value": "fruiting body morphogenesis"},
					},
					"definition": map[string]any{
						"value": "The developmental process that forms the culminant mound.",
					},
					"comments": []string{"observed under starvation conditions"},
				},
			},
		},
		{
			namespace: environmentOntology,
			term: seedPhenotypeTermSpec{
				id:    "DDENV:9991001",
				label: "in suspension",
			},
		},
		{
			namespace: assayOntology,
			term: seedPhenotypeTermSpec{
				id:    "DDASSAY:9991001",
				label: "confocal microscopy",
			},
		},
		{
			namespace: phenotypeOntology,
			term: seedPhenotypeTermSpec{
				id:    "DDPHENO:9991002",
				label: "deprecated fruiting term",
			},
		},
		{
			namespace: dictyAnnotationOntology,
			term: seedPhenotypeTermSpec{
				id:    "DDANNO:9991001",
				label: "culminant annotation tag",
			},
		},
	}
	for _, entry := range terms {
		doc := map[string]any{
			"graph_id":   graphIDs[entry.namespace],
			"id":         entry.term.id,
			"label":      entry.term.label,
			"deprecated": entry.term.deprecated,
		}
		if entry.term.metadata != nil {
			doc["metadata"] = entry.term.metadata
		}
		termMeta, termErr := ar.onto.Term.CreateDocument(ctx, doc)
		assert.NoError(termErr)
		termKeys[entry.term.id] = termMeta.Key
	}

	return termKeys
}

func phenotypeSearchOnce(
	repo repository.TaggedAnnotationRepository,
	params *repository.PhenotypeSearchQuery,
) ([]*repository.PhenotypeSearchResult, error) {
	either := ioeutils.ToEither(repo.SearchPhenotypes(context.Background(), params))

	return E.UnwrapError(either)
}

func waitForPhenotypeSearchResults(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
	query string,
	limit int,
	expect func([]*repository.PhenotypeSearchResult) bool,
) []*repository.PhenotypeSearchResult {
	t.Helper()
	params := &repository.PhenotypeSearchQuery{Query: query, Limit: limit}
	var rows []*repository.PhenotypeSearchResult
	var lastErr error

	require.Eventually(t, func() bool {
		rows, lastErr = phenotypeSearchOnce(repo, params)
		return lastErr == nil && expect(rows)
	}, autocompleteWait, autocompletePoll,
		"expect phenotype search %q to return the expected rows", query)
	require.NoError(t, lastErr, "expect the phenotype search pipeline to succeed")

	return rows
}

func findPhenotypeRow(
	rows []*repository.PhenotypeSearchResult,
	entryID string,
) *repository.PhenotypeSearchResult {
	for _, row := range rows {
		if row.EntryID == entryID {
			return row
		}
	}

	return nil
}

func TestBuildPhenotypeSearchQueryBranchCount(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	stmt := buildPhenotypeSearchQuery()

	for _, name := range []string{
		"p0", "p1", "n0", "n1",
		"t0", "t1", "t2", "t3",
		"h0", "h1", "h2", "h3",
	} {
		assert.Contains(stmt, "LET "+name+" = (", "expect branch %s in statement", name)
	}
	assert.Contains(
		stmt,
		"LET hits = FLATTEN([p0, p1, n0, n1, t0, t1, t2, t3, h0, h1, h2, h3])",
	)
}

func TestValidatePhenotypeSearchRequest(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	_, nilErr := E.UnwrapError(validatePhenotypeSearchRequest(nil))
	assert.ErrorContains(nilErr, msgNilPhenotypeSearchQuery)

	for _, query := range []string{"", "   ", "\u0301\u0301"} {
		_, emptyErr := E.UnwrapError(validatePhenotypeSearchRequest(
			&repository.PhenotypeSearchQuery{Query: query},
		))
		assert.ErrorContains(emptyErr, msgEmptyPhenotypeSearchQuery)
	}

	_, stopwordErr := E.UnwrapError(validatePhenotypeSearchRequest(
		&repository.PhenotypeSearchQuery{Query: "the and"},
	))
	assert.ErrorContains(stopwordErr, "expect a query with at least one non-stopword")

	normalized, normErr := E.UnwrapError(validatePhenotypeSearchRequest(
		&repository.PhenotypeSearchQuery{Query: "  AX4  ", Limit: 15},
	))
	assert.NoError(normErr)
	assert.Equal("ax4", normalized.query)
	assert.Equal(15, normalized.limit)

	clamped, clampErr := E.UnwrapError(validatePhenotypeSearchRequest(
		&repository.PhenotypeSearchQuery{Query: "ax4", Limit: 75},
	))
	assert.NoError(clampErr)
	assert.Equal(50, clamped.limit)

	defaulted, defaultErr := E.UnwrapError(validatePhenotypeSearchRequest(
		&repository.PhenotypeSearchQuery{Query: "ax4", Limit: 0},
	))
	assert.NoError(defaultErr)
	assert.Equal(50, defaulted.limit)
}

func TestSearchPhenotypesAcrossThreeOntologiesAndFourFields(t *testing.T) {
	assert, repo := setUp(t)
	defer tearDown(repo)
	termKeys := seedPhenotypeSearchOntologies(t, repo)

	addFullSearchAnnotation(
		t, repo, "DBS0236201", phenotypeOntology, "culminant mound formation", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236202", environmentOntology, "in suspension", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236203", assayOntology, "confocal microscopy", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236204", phenotypeOntology, "deprecated fruiting term", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236205", environmentOntology, "in suspension", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBS0236206", dictyAnnotationOntology, "culminant annotation tag", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DDB_G0123456", phenotypeOntology, "culminant mound formation", "novalue", 0,
	)
	addFullSearchAnnotation(
		t, repo, "DBP0123456", phenotypeOntology, "culminant mound formation", "novalue", 0,
	)

	ar, ok := repo.(*arangorepository)
	assert.True(ok)

	_, err := ar.onto.Term.UpdateDocument(
		context.Background(),
		termKeys["DDPHENO:9991002"],
		map[string]any{"deprecated": true},
	)
	assert.NoError(err)

	obsoleteDoc := addFullSearchAnnotation(
		t, repo, "DBS0236207", assayOntology, "confocal microscopy", "novalue", 0,
	)
	_, err = ar.anno.annot.UpdateDocument(
		context.Background(),
		obsoleteDoc.Key,
		map[string]any{"is_obsolete": true},
	)
	assert.NoError(err)

	t.Run("prefix label and synonym bands", func(t *testing.T) {
		assert := require.New(t)
		labelRows := waitForPhenotypeSearchResults(
			t, repo, "culminant", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return len(rows) >= 1 && rows[0].EntryID == "DBS0236201"
			},
		)
		labelRow := findPhenotypeRow(labelRows, "DBS0236201")
		assert.NotNil(labelRow)
		assert.Equal(fieldPhenotypeLabel, labelRow.Field)
		assert.GreaterOrEqual(labelRow.Score, float64(1000))

		synonymRows := waitForPhenotypeSearchResults(
			t, repo, "fruiting", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				row := findPhenotypeRow(rows, "DBS0236201")
				return row != nil && row.Field == fieldSynonym
			},
		)
		synonymRow := findPhenotypeRow(synonymRows, "DBS0236201")
		assert.NotNil(synonymRow)
		assert.Equal(fieldSynonym, synonymRow.Field)
		assert.GreaterOrEqual(synonymRow.Score, float64(1000))
	})

	t.Run("phrase token and fuzzy bands rank below prefix", func(t *testing.T) {
		assert := require.New(t)

		phraseRows := waitForPhenotypeSearchResults(
			t, repo, "mound formation", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236201") != nil
			},
		)
		phraseRow := findPhenotypeRow(phraseRows, "DBS0236201")
		assert.NotNil(phraseRow)
		assert.GreaterOrEqual(phraseRow.Score, float64(500))
		assert.Less(phraseRow.Score, float64(1000))

		tokenRows := waitForPhenotypeSearchResults(
			t, repo, "mound process", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236201") != nil
			},
		)
		tokenRow := findPhenotypeRow(tokenRows, "DBS0236201")
		assert.NotNil(tokenRow)
		assert.GreaterOrEqual(tokenRow.Score, float64(250))
		assert.Less(tokenRow.Score, float64(500))

		fuzzyRows := waitForPhenotypeSearchResults(
			t, repo, "culmnant", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236201") != nil
			},
		)
		fuzzyRow := findPhenotypeRow(fuzzyRows, "DBS0236201")
		assert.NotNil(fuzzyRow)
		assert.Less(fuzzyRow.Score, float64(250))
	})

	t.Run("definition and comment projections", func(t *testing.T) {
		assert := require.New(t)

		defRows := waitForPhenotypeSearchResults(
			t, repo, "developmental process", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				row := findPhenotypeRow(rows, "DBS0236201")
				return row != nil && row.Field == fieldPhenotypeDefinition
			},
		)
		defRow := findPhenotypeRow(defRows, "DBS0236201")
		assert.NotNil(defRow)
		assert.Equal("DBS0236201", defRow.EntryID)
		assert.Equal(fieldPhenotypeDefinition, defRow.Field)
		assert.Equal("culminant mound formation", defRow.Name)
		assert.Equal(
			"The developmental process that forms the culminant mound.",
			defRow.Definition,
		)
		assert.GreaterOrEqual(defRow.Score, float64(500))

		commentRows := waitForPhenotypeSearchResults(
			t, repo, "starvation conditions", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				row := findPhenotypeRow(rows, "DBS0236201")
				return row != nil && row.Field == fieldPhenotypeComment
			},
		)
		commentRow := findPhenotypeRow(commentRows, "DBS0236201")
		assert.NotNil(commentRow)
		assert.Equal("DBS0236201", commentRow.EntryID)
		assert.Equal(fieldPhenotypeComment, commentRow.Field)
		assert.Equal("culminant mound formation", commentRow.Name)
		assert.GreaterOrEqual(commentRow.Score, float64(500))
	})

	t.Run("environment and assay ontologies match", func(t *testing.T) {
		assert := require.New(t)

		envRows := waitForPhenotypeSearchResults(
			t, repo, "in suspension", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236202") != nil
			},
		)
		assert.NotNil(findPhenotypeRow(envRows, "DBS0236202"))

		assayRows := waitForPhenotypeSearchResults(
			t, repo, "confocal microscopy", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236203") != nil
			},
		)
		assert.NotNil(findPhenotypeRow(assayRows, "DBS0236203"))
	})

	t.Run("excluded rows never match", func(t *testing.T) {
		assert := require.New(t)

		deprecatedRows := waitForPhenotypeSearchResults(
			t, repo, "deprecated fruiting", 10,
			func(rows []*repository.PhenotypeSearchResult) bool { return true },
		)
		assert.Nil(findPhenotypeRow(deprecatedRows, "DBS0236204"))

		obsoleteRows := waitForPhenotypeSearchResults(
			t, repo, "confocal microscopy", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236203") != nil
			},
		)
		assert.Nil(findPhenotypeRow(obsoleteRows, "DBS0236207"))

		nonStrainRows := waitForPhenotypeSearchResults(
			t, repo, "culminant", 10,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236201") != nil
			},
		)
		assert.Nil(findPhenotypeRow(nonStrainRows, "DDB_G0123456"))
		assert.Nil(findPhenotypeRow(nonStrainRows, "DBP0123456"))

		otherOntologyRows := waitForPhenotypeSearchResults(
			t, repo, "annotation tag", 10,
			func(rows []*repository.PhenotypeSearchResult) bool { return true },
		)
		assert.Nil(findPhenotypeRow(otherOntologyRows, "DBS0236206"))
	})

	t.Run("duplicate hits deduplicate to the best row", func(t *testing.T) {
		assert := require.New(t)
		rows := waitForPhenotypeSearchResults(
			t, repo, "fruiting body morphogenesis", defaultFullSearchLimit,
			func(rows []*repository.PhenotypeSearchResult) bool {
				return findPhenotypeRow(rows, "DBS0236201") != nil
			},
		)
		seen := make(map[string]int)
		for _, row := range rows {
			seen[row.EntryID]++
		}
		assert.Equal(1, seen["DBS0236201"], "expect one deduplicated row per strain")
	})
}
