package service

import (
	"context"
	"testing"
	"time"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
)

// seedPhenotypeSearchTerms creates the environment and assay cvterm documents
// plus a phenotype term carrying synonyms, definition, and comments metadata
// for the phenotype search projection.
func seedPhenotypeSearchTerms(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	assert := require.New(t)
	ctx := context.Background()
	handler := repo.Dbh().Handler()

	cvColl, err := handler.Collection(ctx, "cv")
	assert.NoError(err)
	termColl, err := handler.Collection(ctx, "cvterm")
	assert.NoError(err)

	graphIDs := make(map[string]string)
	for _, namespace := range []string{
		"Dicty Phenotypes",
		"Dicty Environment",
		"Dictyostelium Assay",
	} {
		row, queryErr := handler.Query(
			ctx,
			`FOR cv IN cv FILTER cv.metadata.namespace == @onto LIMIT 1 RETURN cv._id`,
			map[string]any{"onto": namespace},
		)
		assert.NoError(queryErr)
		var graphID string
		if row.HasMore() {
			_, readErr := row.ReadDocument(ctx, &graphID)
			assert.NoError(readErr)
		}
		assert.NoError(row.Close())
		if graphID != "" {
			graphIDs[namespace] = graphID
			continue
		}
		cvMeta, createErr := cvColl.CreateDocument(ctx, map[string]any{
			"metadata": map[string]any{"namespace": namespace},
		})
		assert.NoError(createErr)
		graphIDs[namespace] = cvMeta.ID.String()
	}

	_, err = termColl.CreateDocument(ctx, map[string]any{
		"graph_id":   graphIDs["Dicty Environment"],
		"id":         "DDENV:9992001",
		"label":      "in suspension",
		"deprecated": false,
	})
	assert.NoError(err)
	_, err = termColl.CreateDocument(ctx, map[string]any{
		"graph_id":   graphIDs["Dictyostelium Assay"],
		"id":         "DDASSAY:9992001",
		"label":      "confocal microscopy",
		"deprecated": false,
	})
	assert.NoError(err)
	_, err = termColl.CreateDocument(ctx, map[string]any{
		"graph_id":   graphIDs["Dicty Phenotypes"],
		"id":         "DDPHENO:9992001",
		"label":      "culminant mound formation",
		"deprecated": false,
		"metadata": map[string]any{
			"synonyms": []map[string]any{
				{"value": "fruiting body morphogenesis"},
			},
			"definition": map[string]any{
				"value": "The developmental process that forms the culminant mound.",
			},
			"comments": []string{"observed under starvation conditions"},
		},
	})
	assert.NoError(err)
}

// phenotypeSearchEndToEndSetup reuses the full search bufconn world, seeds
// phenotype search terms and strain annotations, and returns the client.
func phenotypeSearchEndToEndSetup(
	t *testing.T,
) annotation.TaggedAnnotationServiceClient {
	t.Helper()
	assert := require.New(t)
	client, repo := fullSearchEndToEndSetup(t)
	seedPhenotypeSearchTerms(t, repo)

	annotations := []struct {
		entryID  string
		ontology string
		tag      string
	}{
		{"DBS0236301", "Dicty Phenotypes", "culminant mound formation"},
		{"DBS0236302", "Dicty Environment", "in suspension"},
		{"DBS0236303", "Dictyostelium Assay", "confocal microscopy"},
	}
	ctx := context.Background()
	for _, anno := range annotations {
		_, err := client.CreateAnnotation(
			ctx,
			&annotation.NewTaggedAnnotation{
				Data: &annotation.NewTaggedAnnotation_Data{
					Type: "annotations",
					Attributes: &annotation.NewTaggedAnnotationAttributes{
						Value:         "novalue",
						EditableValue: "novalue",
						CreatedBy:     "siddbasu@gmail.com",
						Tag:           anno.tag,
						Ontology:      anno.ontology,
						EntryId:       anno.entryID,
						Rank:          0,
					},
				},
			},
		)
		assert.NoError(err, "expect annotation %s to be stored", anno.entryID)
	}

	return client
}

// waitForPhenotypeSearchRPC polls the SearchPhenotypes RPC until the view
// serves the expected hit.
func waitForPhenotypeSearchRPC(
	t *testing.T,
	client annotation.TaggedAnnotationServiceClient,
	query string,
	expectedID string,
	expectedField annotation.PhenotypeSearchField,
	minScore float64,
) *annotation.PhenotypeSearchResult {
	t.Helper()
	params := &annotation.PhenotypeSearchParameters{
		Data: &annotation.PhenotypeSearchParameters_Data{
			Type: "phenotype",
			Attributes: &annotation.PhenotypeSearchAttributes{
				Query: query,
				Limit: 10,
			},
		},
	}
	var match *annotation.PhenotypeSearchResult
	var lastErr error

	require.Eventually(t, func() bool {
		ctx := context.Background()
		collection, err := client.SearchPhenotypes(ctx, params)
		if err != nil {
			lastErr = err
			return false
		}
		for _, row := range collection.GetData() {
			if row.GetEntryId() == expectedID &&
				row.GetField() == expectedField &&
				row.GetScore() >= minScore {
				match = row
				return true
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"expect search %q to return entry %s with field %v and score >= %f",
		query, expectedID, expectedField, minScore)
	require.NoError(t, lastErr, "expect search rpc to succeed")

	return match
}

func TestSearchPhenotypesEndToEnd(t *testing.T) {
	client := phenotypeSearchEndToEndSetup(t)

	tests := []struct {
		name          string
		query         string
		expectedID    string
		expectedField annotation.PhenotypeSearchField
		minScore      float64
	}{
		{
			name:          "phenotype_label_prefix",
			query:         "aberrant",
			expectedID:    "DBS0236128",
			expectedField: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
			minScore:      1000.0,
		},
		{
			name:          "phenotype_synonym_phrase",
			query:         "fruiting body morphogenesis",
			expectedID:    "DBS0236301",
			expectedField: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_SYNONYM,
			minScore:      500.0,
		},
		{
			name:          "phenotype_definition_phrase",
			query:         "developmental process",
			expectedID:    "DBS0236301",
			expectedField: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_DEFINITION,
			minScore:      500.0,
		},
		{
			name:          "phenotype_comment_phrase",
			query:         "starvation conditions",
			expectedID:    "DBS0236301",
			expectedField: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_COMMENT,
			minScore:      500.0,
		},
		{
			name:          "environment_label_phrase",
			query:         "in suspension",
			expectedID:    "DBS0236302",
			expectedField: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
			minScore:      500.0,
		},
		{
			name:          "assay_label_prefix",
			query:         "confocal",
			expectedID:    "DBS0236303",
			expectedField: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
			minScore:      1000.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := require.New(t)
			row := waitForPhenotypeSearchRPC(
				t,
				client,
				tc.query,
				tc.expectedID,
				tc.expectedField,
				tc.minScore,
			)

			assert.Equal(tc.expectedID, row.GetEntryId())
			assert.Equal(tc.expectedField, row.GetField())
			assert.GreaterOrEqual(row.GetScore(), tc.minScore)
			if tc.expectedID == "DBS0236301" {
				assert.Equal("culminant mound formation", row.GetName())
				assert.Equal(
					"The developmental process that forms the culminant mound.",
					row.GetDefinition(),
				)
			}
		})
	}
}

func TestSearchPhenotypesRankingIsDescending(t *testing.T) {
	assert := require.New(t)
	client := phenotypeSearchEndToEndSetup(t)
	params := &annotation.PhenotypeSearchParameters{
		Data: &annotation.PhenotypeSearchParameters_Data{
			Type: "phenotype",
			Attributes: &annotation.PhenotypeSearchAttributes{
				Query: "mound formation",
				Limit: 10,
			},
		},
	}

	var collection *annotation.PhenotypeSearchResultCollection
	require.Eventually(t, func() bool {
		var err error
		collection, err = client.SearchPhenotypes(context.Background(), params)
		return err == nil && len(collection.GetData()) >= 1
	}, 30*time.Second, 250*time.Millisecond,
		"expect search %q to return at least one row", "mound formation")

	rows := collection.GetData()
	for i := 1; i < len(rows); i++ {
		assert.GreaterOrEqual(
			rows[i-1].GetScore(),
			rows[i].GetScore(),
			"expect rows sorted by descending score",
		)
	}
	assert.Equal(int64(len(rows)), collection.GetMeta().GetTotal())
}
