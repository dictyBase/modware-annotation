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

// autocompleteOntologyParams describe the ontology collections that the
// annotation tag graph points into.
func autocompleteOntologyParams() *araobo.CollectionParams {
	return &araobo.CollectionParams{
		GraphInfo:    "cv",
		OboGraph:     "obograph",
		Relationship: "cvterm_relationship",
		Term:         "cvterm",
	}
}

// autocompleteArangoParams describe the annotation collections of the
// end-to-end test database.
func autocompleteArangoParams() *arangodb.CollectionParams {
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

// loadAutocompleteOntology saves the dicty_annotation obograph into the
// disposable database, so the synonym and genotype cvterm identifiers resolve.
func loadAutocompleteOntology(t *testing.T, tra *testarango.TestArango) {
	t.Helper()
	assert := require.New(t)

	dir, err := os.Getwd()
	assert.NoError(err)
	reader, err := os.Open(filepath.Join(
		filepath.Dir(filepath.Dir(dir)),
		"repository",
		"testdata",
		"dicty_annotation.json",
	))
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
	}, autocompleteOntologyParams())
	assert.NoError(err)

	assert.NoError(datasource.SaveOboGraphInfo(oboGraph))
	_, err = datasource.SaveTerms(oboGraph)
	assert.NoError(err)
	_, err = datasource.SaveRelationships(oboGraph)
	assert.NoError(err)
}

// autocompleteEndToEndSetup builds a real repository over a disposable
// database, seeds one multi-allele genotype annotation, and serves the
// annotation service over bufconn.
func autocompleteEndToEndSetup(
	t *testing.T,
) (annotation.TaggedAnnotationServiceClient, repository.TaggedAnnotationRepository) {
	t.Helper()
	assert := require.New(t)

	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err, "expect a disposable arangodb instance")

	repo, err := arangodb.NewTaggedAnnotationRepo(
		arangodb.GetConnectParamsFromDB(tra),
		autocompleteArangoParams(),
		autocompleteOntologyParams(),
	)
	assert.NoErrorf(
		err,
		"expect no error connecting to annotation repository, received %s",
		err,
	)
	loadAutocompleteOntology(t, tra)

	_, err = repo.AddAnnotation(&annotation.NewTaggedAnnotation{
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
	})
	assert.NoError(err, "expect the genotype annotation to be stored")

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
		// Do not log from this goroutine: it would race with the testing
		// package's own bookkeeping.
		_ = server.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	// Dial the listener through the passthrough scheme explicitly. Mutating
	// the process-wide default scheme races between parallel tests.
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

	return annotation.NewTaggedAnnotationServiceClient(conn), repo
}

// waitForAutocomplete polls the RPC until the search view serves the seeded
// annotation, because an ArangoSearch view only exposes a document after its
// next commit.
func waitForAutocomplete(
	t *testing.T,
	client annotation.TaggedAnnotationServiceClient,
	query string,
) []*annotation.AnnotationSuggestion {
	t.Helper()
	params := &annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationAutocompleteAttributes{
				Query: query,
				Limit: 5,
			},
		},
	}
	var rows []*annotation.AnnotationSuggestion
	var lastErr error

	require.Eventually(t, func() bool {
		ctx := context.Background()
		collection, err := client.AutocompleteAnnotation(ctx, params)
		if err != nil {
			lastErr = err
			return false
		}
		rows = collection.GetData()
		return len(rows) == 1
	}, 30*time.Second, 250*time.Millisecond,
		"expect autocomplete %q to return one suggestion", query)
	require.NoError(t, lastErr, "expect the autocomplete rpc to succeed")

	return rows
}

func TestAutocompleteAnnotationEndToEnd(t *testing.T) {
	client, _ := autocompleteEndToEndSetup(t)

	tests := []struct {
		name  string
		query string
	}{
		{name: "whole_value_genotype_prefix", query: "axea"},
		{name: "interior_allele_prefix", query: "sada"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := require.New(t)
			rows := waitForAutocomplete(t, client, tc.query)

			assert.Len(rows, 1)
			assert.Equal("DBS0236126", rows[0].GetEntryId())
			assert.Equal(
				annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE,
				rows[0].GetField(),
			)
			assert.Equal("axeA1, axeB1, sadA-", rows[0].GetDisplayText())
			assert.GreaterOrEqual(rows[0].GetScore(), float64(1000))
		})
	}
}
