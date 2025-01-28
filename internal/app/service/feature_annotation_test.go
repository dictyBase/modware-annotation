package service

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
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

func TestCreateFeatureAnnotation(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testCreateValidFeature(params)
	testCreateMissingFields(params)
	testCreateDuplicateFeature(params)
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
			},
		}
		resp, err := params.client.CreateFeatureAnnotation(params.ctx, req)
		params.assert.NoError(err)
		params.assert.Equal(req.Id, resp.Id)
		params.assert.Equal(req.CreatedBy, resp.CreatedBy)
		params.assert.Equal(req.Attributes.Name, resp.Attributes.Name)
		params.assert.Equal(req.Attributes.Synonyms, resp.Attributes.Synonyms)
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

func TestGetFeatureAnnotation(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testGetExistingFeature(params)
	testGetNonExistentFeature(params)
	testGetFeatureWithInvalidID(params)
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
