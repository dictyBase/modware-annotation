package service

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/arangomanager/testarango"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-obograph/graph"
	araobo "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fullSearchOntologyParams describe the ontology collections that the
// annotation tag graph points into.
func fullSearchOntologyParams() *araobo.CollectionParams {
	return &araobo.CollectionParams{
		GraphInfo:    "cv",
		OboGraph:     "obograph",
		Relationship: "cvterm_relationship",
		Term:         "cvterm",
	}
}

// fullSearchArangoParams describe the annotation collections of the
// end-to-end test database.
func fullSearchArangoParams() *arangodb.CollectionParams {
	return &arangodb.CollectionParams{
		Annotation:   "annotation",
		AnnoTerm:     "annotation_cvterm",
		AnnoVersion:  "annotation_version",
		AnnoTagGraph: "annotation_tag",
		AnnoVerGraph: "annotation_history",
		AnnoGroup:    "annotation_group",
		AnnoIndexes:  []string{"entry_id"},
	}
}

// loadFullSearchObo saves an obograph into the disposable database.
func loadFullSearchObo(
	t *testing.T,
	tra *testarango.TestArango,
	filename string,
) {
	t.Helper()
	assert := require.New(t)

	dir, err := os.Getwd()
	assert.NoError(err)
	reader, err := os.Open(filepath.Clean(filepath.Join(
		filepath.Dir(filepath.Dir(dir)),
		"repository",
		"testdata",
		filename,
	)))
	assert.NoError(err)
	defer func() { _ = reader.Close() }()

	oboGraph, err := graph.BuildGraph(reader)
	assert.NoError(err)

	datasource, err := araobo.NewDataSource(&araobo.ConnectParams{
		User:     tra.User,
		Pass:     tra.Pass,
		Host:     tra.Host,
		Database: tra.Database,
		Port:     tra.Port,
		Istls:    tra.Istls,
	}, fullSearchOntologyParams())
	assert.NoError(err)

	assert.NoError(datasource.SaveOboGraphInfo(oboGraph))
	_, err = datasource.SaveTerms(oboGraph)
	assert.NoError(err)
	_, err = datasource.SaveRelationships(oboGraph)
	assert.NoError(err)
}

// ensureFullSearchPhenotypeTerm ensures the test phenotype cvterm exists
// under the loaded Dicty Phenotypes cv._id.
func ensureFullSearchPhenotypeTerm(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	assert := require.New(t)
	ctx := context.Background()

	var phenoGraphID string
	cursor, err := repo.Dbh().Handler().Query(
		ctx,
		`FOR cv IN cv FILTER cv.metadata.namespace == @onto LIMIT 1 RETURN cv._id`,
		map[string]any{"onto": "Dicty Phenotypes"},
	)
	assert.NoError(err)
	defer func() { _ = cursor.Close() }()
	assert.True(cursor.HasMore(), "expect Dicty Phenotypes cv document")
	_, err = cursor.ReadDocument(ctx, &phenoGraphID)
	assert.NoError(err)

	termColl, err := repo.Dbh().Handler().Collection(ctx, "cvterm")
	assert.NoError(err)
	_, err = termColl.CreateDocument(ctx, map[string]any{
		"graph_id":   phenoGraphID,
		"id":         "DDPHENO:9990001",
		"label":      "aberrant fruiting body morphogenesis",
		"deprecated": false,
	})
	assert.NoError(err)
}

// fullSearchEndToEndSetup builds a real repository over a disposable
// database, seeds ontologies, creates 3 annotations over CreateAnnotation,
// and serves the annotation service over bufconn.
func fullSearchEndToEndSetup(
	t *testing.T,
) (annotation.TaggedAnnotationServiceClient, repository.TaggedAnnotationRepository) {
	t.Helper()
	assert := require.New(t)

	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err, "expect a disposable arangodb instance")

	repo, err := arangodb.NewTaggedAnnotationRepo(
		arangodb.GetConnectParamsFromDB(tra),
		fullSearchArangoParams(),
		fullSearchOntologyParams(),
	)
	assert.NoErrorf(
		err,
		"expect no error connecting to annotation repository, received %s",
		err,
	)

	loadFullSearchObo(t, tra, "dicty_annotation.json")
	loadFullSearchObo(t, tra, "dicty_phenotypes.json")
	ensureFullSearchPhenotypeTerm(t, repo)

	svc, err := NewAnnotationService(&Params{
		Repository: repo,
		Publisher:  &MockTaggedMessage{},
		Options: []aphgrpc.Option{
			aphgrpc.TopicsOption(map[string]string{
				"annotationCreate": "annotation_create_test",
				"annotationUpdate": "annotation_update_test",
			}),
		},
		Group: "annotation_group_test",
	})
	assert.NoError(err)

	server := grpc.NewServer()
	annotation.RegisterTaggedAnnotationServiceServer(server, svc)
	listener := bufconn.Listen(1024 * 1024)
	go func() {
		// Do not log from this goroutine: it would race with testing bookkeeping.
		_ = server.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	assert.NoError(err)
	t.Cleanup(func() {
		_ = repo.Dbh().Drop()
		_ = conn.Close()
		_ = listener.Close()
		server.Stop()
	})

	client := annotation.NewTaggedAnnotationServiceClient(conn)
	ctx := context.Background()

	// 1. Seed genotype annotation (DBS0236126)
	_, err = client.CreateAnnotation(
		ctx,
		&annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         "axeA1, axeB1, sadA-",
					EditableValue: "axeA1, axeB1, sadA-",
					CreatedBy:     "siddbasu@gmail.com",
					Tag:           "genotype",
					Ontology:      "dicty_annotation",
					EntryId:       "DBS0236126",
					Rank:          0,
				},
			},
		},
	)
	assert.NoError(err, "expect genotype annotation to be stored")

	// 2. Seed public note annotation (DBS0236127)
	_, err = client.CreateAnnotation(
		ctx,
		&annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         "mutant forms culminants under starvation",
					EditableValue: "mutant forms culminants under starvation",
					CreatedBy:     "siddbasu@gmail.com",
					Tag:           "public note",
					Ontology:      "dicty_annotation",
					EntryId:       "DBS0236127",
					Rank:          0,
				},
			},
		},
	)
	assert.NoError(err, "expect public note annotation to be stored")

	// 3. Seed Dicty Phenotypes annotation (DBS0236128)
	_, err = client.CreateAnnotation(
		ctx,
		&annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         "novalue",
					EditableValue: "novalue",
					CreatedBy:     "siddbasu@gmail.com",
					Tag:           "aberrant fruiting body morphogenesis",
					Ontology:      "Dicty Phenotypes",
					EntryId:       "DBS0236128",
					Rank:          0,
				},
			},
		},
	)
	assert.NoError(err, "expect phenotype annotation to be stored")

	return client, repo
}

// waitForFullSearchRPC polls the SearchAnnotation RPC until the view serves the hit.
func waitForFullSearchRPC(
	t *testing.T,
	client annotation.TaggedAnnotationServiceClient,
	query string,
	expectedID string,
	expectedField annotation.AnnotationSearchField,
	minScore float64,
) *annotation.AnnotationSearchResult {
	t.Helper()
	params := &annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationSearchAttributes{
				Query: query,
				Limit: 10,
			},
		},
	}
	var match *annotation.AnnotationSearchResult
	var lastErr error

	require.Eventually(t, func() bool {
		ctx := context.Background()
		collection, err := client.SearchAnnotation(ctx, params)
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

func TestSearchAnnotationEndToEnd(t *testing.T) {
	client, _ := fullSearchEndToEndSetup(t)

	tests := []struct {
		name          string
		query         string
		expectedID    string
		expectedField annotation.AnnotationSearchField
		minScore      float64
	}{
		{
			name:          "interior_allele_genotype_prefix",
			query:         "sada",
			expectedID:    "DBS0236126",
			expectedField: annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE,
			minScore:      1000.0,
		},
		{
			name:          "public_note_phrase",
			query:         "forms culminants",
			expectedID:    "DBS0236127",
			expectedField: annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PUBLIC_NOTE,
			minScore:      500.0,
		},
		{
			name:          "phenotype_term_phrase",
			query:         "fruiting body",
			expectedID:    "DBS0236128",
			expectedField: annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PHENOTYPE,
			minScore:      500.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := require.New(t)
			row := waitForFullSearchRPC(
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
		})
	}
}
