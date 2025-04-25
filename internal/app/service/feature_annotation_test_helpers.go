package service

import (
	"context"
	"net"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testParams struct {
	t      *testing.T
	ctx    context.Context
	client feature.FeatureAnnotationServiceClient
	assert *require.Assertions
}

type MockMessage struct{}

func (msn *MockMessage) Publish(
	subject string,
	feat *feature.FeatureAnnotation,
) error {
	return nil
}

func (msn *MockMessage) Close() error {
	return nil
}

// sortTagPropertiesByTag sorts TagProperty objects by their tag name
// (case-insensitive).
func sortTagPropertiesByTag(a, b *feature.TagProperty) int {
	return strings.Compare(
		strings.ToLower(a.Tag),
		strings.ToLower(b.Tag),
	)
}

// extractTagAndValue returns a new TagProperty with only Tag and Value fields.
func extractTagAndValue(prop *feature.TagProperty) *feature.TagProperty {
	return &feature.TagProperty{
		Tag:   prop.Tag,
		Value: prop.Value,
	}
}

func setup(
	t *testing.T,
) (feature.FeatureAnnotationServiceClient, *require.Assertions) {
	t.Helper()
	assert := require.New(t)
	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err, "expect no error from creating an arangodb instance")
	repo, err := arangodb.NewFeatureAnnoRepo(
		arangodb.GetConnectParamsFromDB(tra),
		&arangodb.FeatureCollectionParams{
			Feature: "feature_test",
			Pub:     "pub_test",
			Edge:    "feature_pub_test",
			Graph:   "feature_pub_graph_test",
		},
	)
	assert.NoErrorf(
		err,
		"expect no error connecting to annotation repository, received %s",
		err,
	)
	// Create service with mock dependencies
	svc, err := NewFeatureAnnotationService(&FeatureParams{
		Repository: repo,
		Publisher:  &MockMessage{},
	})
	assert.NoError(err)

	// GRPC server setup
	server := grpc.NewServer()
	feature.RegisterFeatureAnnotationServiceServer(server, svc)
	lis := bufconn.Listen(1024 * 1024)
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
			os.Exit(1)
		}
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		conn, err := lis.Dial()
		assert.NoError(err, "expect no error from creating listener")

		return conn, nil
	}
	resolver.SetDefaultScheme("passthrough")
	// GRPC client setup
	conn, err := grpc.NewClient(
		"bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	assert.NoError(err)
	t.Cleanup(func() {
		_ = repo.Dbh().Drop()
		conn.Close()
		lis.Close()
		server.Stop()
	})

	return feature.NewFeatureAnnotationServiceClient(conn), assert
}

func testCreateValidFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("CreateValidFeatureAnnotation", func(t *testing.T) {
		t.Parallel()
		req := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285425",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:     "Test Feature",
				Synonyms: []string{"test1", "test2"},
				// Add properties
				Properties: []*feature.TagProperty{
					{
						Tag:       "description",
						Value:     "Test description",
						CreatedBy: "testuser@dictybase.org",
					},
					{
						Tag:       "note",
						Value:     "Test note",
						CreatedBy: "testuser@dictybase.org",
					},
				},
			},
		}
		resp, err := params.client.CreateFeatureAnnotation(params.ctx, req)
		params.assert.NoError(err)
		params.assert.Equal(req.Id, resp.Id)
		params.assert.Equal(req.CreatedBy, resp.CreatedBy)
		params.assert.Equal(req.Attributes.Name, resp.Attributes.Name)
		params.assert.Equal(req.Attributes.Synonyms, resp.Attributes.Synonyms)

		// Validate properties
		params.assert.Len(resp.Attributes.Properties, 2)
		slices.SortFunc(req.Attributes.Properties, sortTagPropertiesByTag)
		slices.SortFunc(resp.Attributes.Properties, sortTagPropertiesByTag)
		params.assert.ElementsMatch(
			collection.Map(req.Attributes.Properties, extractTagAndValue),
			collection.Map(resp.Attributes.Properties, extractTagAndValue),
			"should have matching properties",
		)
	})
}

func testListByDOIValid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByDOIValid", func(t *testing.T) {
		t.Parallel()
		doi := "10.1234/j.abcd.2023.01.001"
		// Create features associated with the DOI
		feat1 := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285430",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:         "Feature DOI 1",
				Publications: []string{doi},
			},
		}
		feat2 := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285431",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:         "Feature DOI 2",
				Publications: []string{doi},
			},
		}
		_, err := params.client.CreateFeatureAnnotation(params.ctx, feat1)
		params.assert.NoError(err)
		_, err = params.client.CreateFeatureAnnotation(params.ctx, feat2)
		params.assert.NoError(err)

		// List features by DOI
		req := &feature.DOI{Id: doi}
		resp, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
		params.assert.NoError(err)
		params.assert.Len(resp.Data, 2)
		foundIds := []string{resp.Data[0].Id, resp.Data[1].Id}
		params.assert.Contains(foundIds, feat1.Id)
		params.assert.Contains(foundIds, feat2.Id)
	})
}

func testListByDOINotFound(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByDOINotFound", func(t *testing.T) {
		t.Parallel()
		req := &feature.DOI{Id: "10.9999/non.existent.doi"} // Non-existent DOI
		_, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.NotFound, st.Code())
	})
}

func testListByDOIInvalid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByDOIInvalid", func(t *testing.T) {
		t.Parallel()
		req := &feature.DOI{Id: ""} // Invalid (empty) DOI
		_, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, st.Code())
	})
}

func testListByPubmedIdValid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByPubmedIdValid", func(t *testing.T) {
		t.Parallel()
		pubmedId := "12345678"
		// Create features associated with the pubmed ID
		feat1 := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285428",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:   "Feature 1",
				Pubmed: []string{pubmedId},
			},
		}
		feat2 := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285429",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:   "Feature 2",
				Pubmed: []string{pubmedId},
			},
		}
		_, err := params.client.CreateFeatureAnnotation(params.ctx, feat1)
		params.assert.NoError(err)
		_, err = params.client.CreateFeatureAnnotation(params.ctx, feat2)
		params.assert.NoError(err)

		// List features by pubmed ID
		req := &feature.PubmedId{Id: pubmedId}
		resp, err := params.client.ListFeatureAnnotationsByPubmedId(
			params.ctx,
			req,
		)
		params.assert.NoError(err)
		params.assert.Len(resp.Data, 2)
		// Check if the returned features match the created ones (order might vary)
		foundIds := []string{resp.Data[0].Id, resp.Data[1].Id}
		params.assert.Contains(foundIds, feat1.Id)
		params.assert.Contains(foundIds, feat2.Id)
	})
}

func testListByPubmedIdNotFound(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByPubmedIdNotFound", func(t *testing.T) {
		t.Parallel()
		req := &feature.PubmedId{Id: "99999999"} // Non-existent pubmed ID
		_, err := params.client.ListFeatureAnnotationsByPubmedId(
			params.ctx,
			req,
		)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.NotFound, st.Code())
	})
}

func testListByPubmedIdInvalid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByPubmedIdInvalid", func(t *testing.T) {
		t.Parallel()
		req := &feature.PubmedId{Id: ""} // Invalid (empty) pubmed ID
		_, err := params.client.ListFeatureAnnotationsByPubmedId(
			params.ctx,
			req,
		)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, st.Code())
	})
}

func testCreateMissingFields(params *testParams) {
	params.t.Helper()
	params.t.Run("CreateFailsMissingRequiredFields", func(t *testing.T) {
		t.Parallel()
		req := &feature.NewFeatureAnnotation{
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Invalid Feature",
			},
		}
		_, err := params.client.CreateFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, st.Code())
	})
}

func testCreateDuplicateFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("CreateFailsDuplicateFeatureId", func(t *testing.T) {
		t.Parallel()
		req := &feature.NewFeatureAnnotation{
			Id:        "DDB_G02854297",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Duplicate Feature",
			},
		}
		_, firstErr := params.client.CreateFeatureAnnotation(params.ctx, req)
		params.assert.NoError(firstErr)
		_, dupErr := params.client.CreateFeatureAnnotation(params.ctx, req)
		params.assert.Error(dupErr)
		st, ok := status.FromError(dupErr)
		params.assert.True(ok)
		params.assert.Equal(codes.AlreadyExists, st.Code())
	})
}

func testGetExistingFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("GetExistingFeatureAnnotation", func(t *testing.T) {
		t.Parallel()
		// First create a feature
		createReq := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285426",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:     "Test Feature",
				Synonyms: []string{"test1", "test2"},
			},
		}
		_, err := params.client.CreateFeatureAnnotation(params.ctx, createReq)
		params.assert.NoError(err)

		// Then retrieve it
		getReq := &feature.FeatureAnnotationId{
			Id: "DDB_G0285426",
		}
		resp, err := params.client.GetFeatureAnnotation(params.ctx, getReq)
		params.assert.NoError(err)
		params.assert.Equal(createReq.Id, resp.Id)
		params.assert.Equal(createReq.CreatedBy, resp.CreatedBy)
		params.assert.Equal(createReq.Attributes.Name, resp.Attributes.Name)
		params.assert.Equal(
			createReq.Attributes.Synonyms,
			resp.Attributes.Synonyms,
		)
	})
}

func testGetNonExistentFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("GetNonExistentFeatureAnnotation", func(t *testing.T) {
		t.Parallel()
		req := &feature.FeatureAnnotationId{
			Id: "DDB_G0000000",
		}
		_, err := params.client.GetFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.NotFound, st.Code())
	})
}

func testGetFeatureWithInvalidID(params *testParams) {
	params.t.Helper()
	params.t.Run("GetFeatureAnnotationWithInvalidID", func(t *testing.T) {
		t.Parallel()
		req := &feature.FeatureAnnotationId{
			Id: "", // Empty ID
		}
		_, err := params.client.GetFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, st.Code())
	})
}

func testUpdateExistingFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("UpdateExistingFeatureAnnotation", func(t *testing.T) {
		t.Parallel()
		// First create a feature
		createReq := &feature.NewFeatureAnnotation{
			Id:        "DDB_G0285427",
			CreatedBy: "testuser@dictybase.org",
			CreatedAt: timestamppb.Now(),
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:     "Original Feature",
				Synonyms: []string{"orig1", "orig2"},
			},
		}
		_, err := params.client.CreateFeatureAnnotation(params.ctx, createReq)
		params.assert.NoError(err)

		// Then update it
		updateReq := &feature.FeatureAnnotationUpdate{
			Id:        "DDB_G0285427",
			UpdatedBy: "anotheruser@dictybase.org",
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:     "Updated Feature",
				Synonyms: []string{"new1", "new2"},
			},
		}
		resp, err := params.client.UpdateFeatureAnnotation(
			params.ctx,
			updateReq,
		)
		params.assert.NoError(err)
		params.assert.Equal(updateReq.Id, resp.Id)
		params.assert.Equal(updateReq.UpdatedBy, resp.UpdatedBy)
		params.assert.Equal(updateReq.Attributes.Name, resp.Attributes.Name)
		params.assert.ElementsMatch(
			slices.Concat(
				createReq.Attributes.Synonyms,
				updateReq.Attributes.Synonyms,
			),
			resp.Attributes.Synonyms,
		)
		params.assert.Equal(createReq.CreatedBy, resp.CreatedBy)
	})
}

func testUpdateNonExistentFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("UpdateNonExistentFeatureAnnotation", func(t *testing.T) {
		t.Parallel()
		req := &feature.FeatureAnnotationUpdate{
			Id:        "DDB_G0000000",
			UpdatedBy: "testuser@dictybase.org",
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Non-existent Feature",
			},
		}
		_, err := params.client.UpdateFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		t.Log(st.Code().String())
		params.assert.Equal(codes.Internal, st.Code())
	})
}

func testUpdateWithInvalidData(params *testParams) {
	params.t.Helper()
	params.t.Run("UpdateFeatureAnnotationWithInvalidData", func(t *testing.T) {
		t.Parallel()
		req := &feature.FeatureAnnotationUpdate{
			Id: "", // Empty ID
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Invalid Feature",
			},
		}
		_, err := params.client.UpdateFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		st, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, st.Code())
	})
}
