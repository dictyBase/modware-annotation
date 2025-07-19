package service

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

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

// tagPropertyCreateParams holds the parameters for the
// createServiceTagPropertyCreate function.
type tagPropertyCreateParams struct {
	tag       string
	value     string
	createdBy string
	timestamp *time.Time
}

// testListByPublicationHelperParams holds the parameters for the
// testListByPublicationHelper function.
type testListByPublicationHelperParams struct {
	params            *testParams
	publicationType   string // "doi" or "pubmed"
	publicationID     string
	featureID1        string
	featureID2        string
	featureNamePrefix string
}

// assertGrpcErrorParams holds the parameters for the assertGrpcError function.
type assertGrpcErrorParams struct {
	assert               *require.Assertions
	err                  error
	expectedCode         codes.Code
	expectedMsgSubstring string
}

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
		if err = server.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
			os.Exit(1)
		}
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		conn, errd := lis.Dial()
		assert.NoError(errd, "expect no error from creating listener")

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

// newTestFeature provides a consistent *feature.NewFeatureAnnotation for testing.
func newTestFeature() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425", // Use the ID from testCreateValidFeature
		CreatedBy: "testuser@dictybase.org",
		CreatedAt: timestamppb.Now(), // Note: Timestamp will differ slightly on each call
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:     "Test Feature", // Use the name from testCreateValidFeature
			Synonyms: []string{"test1", "test2"},
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
			// Add other fields if necessary to match testCreateValidFeature's intent
		},
	}
}

// assertGrpcError checks if the given error is a gRPC error with the expected code
// and optionally contains the expected message substring.
func assertGrpcError(params assertGrpcErrorParams) {
	params.assert.Error(params.err, "expected a gRPC error")
	sts, ok := status.FromError(params.err)
	params.assert.True(ok, "error should be a gRPC status error")
	params.assert.Equal(
		params.expectedCode,
		sts.Code(),
		"expected gRPC code %s, but got %s",
		params.expectedCode,
		sts.Code(),
	)
	if params.expectedMsgSubstring != "" {
		params.assert.Contains(
			strings.ToLower(sts.Message()), // Case-insensitive check
			strings.ToLower(params.expectedMsgSubstring),
			"expected gRPC error message to contain '%s', but got '%s'",
			params.expectedMsgSubstring,
			sts.Message(),
		)
	}
}
