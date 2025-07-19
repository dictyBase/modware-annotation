package service

import (
	"context"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

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

func testCreateValidFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("CreateValidFeatureAnnotation", func(t *testing.T) {
		// Removed t.Parallel() to ensure creation completes before subsequent steps
		// t.Parallel()
		// Use the helper function to get the test data
		req := newTestFeature()
		// Adjust CreatedAt if precise matching is needed later, otherwise Now() is fine for creation
		req.CreatedAt = timestamppb.Now()

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

// testListByPublicationHelper is a helper function to test listing features by publication ID (DOI or Pubmed).
func testListByPublicationHelper(args *testListByPublicationHelperParams) {
	args.params.t.Helper()
	feat1 := &feature.NewFeatureAnnotation{
		Id:        args.featureID1,
		CreatedBy: "testuser@dictybase.org",
		CreatedAt: timestamppb.Now(),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: args.featureNamePrefix,
		},
	}
	feat2 := &feature.NewFeatureAnnotation{
		Id:        args.featureID2,
		CreatedBy: "testuser@dictybase.org",
		CreatedAt: timestamppb.Now(),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: args.featureNamePrefix,
		},
	}

	switch args.publicationType {
	case "doi":
		feat1.Attributes.Publications = []string{args.publicationID}
		feat2.Attributes.Publications = []string{args.publicationID}
	case "pubmed":
		feat1.Attributes.Pubmed = []string{args.publicationID}
		feat2.Attributes.Pubmed = []string{args.publicationID}
	default:
		args.params.t.Fatalf(
			"invalid publication type: %s",
			args.publicationType,
		)
	}

	_, err := args.params.client.CreateFeatureAnnotation(args.params.ctx, feat1)
	args.params.assert.NoError(err)
	_, err = args.params.client.CreateFeatureAnnotation(args.params.ctx, feat2)
	args.params.assert.NoError(err)

	var resp *feature.FeatureAnnotationCollection
	// List features by publication ID
	switch args.publicationType {
	case "doi":
		req := &feature.DOI{Id: args.publicationID}
		resp, err = args.params.client.ListFeatureAnnotationsByDOI(
			args.params.ctx,
			req,
		)
	case "pubmed":
		req := &feature.PubmedId{Id: args.publicationID}
		resp, err = args.params.client.ListFeatureAnnotationsByPubmedId(
			args.params.ctx,
			req,
		)
	}

	args.params.assert.NoError(err)
	args.params.assert.Len(resp.Data, 2)
	// Check if the returned features match the created ones (order might vary)
	foundIDs := []string{
		resp.Data[0].Id,
		resp.Data[1].Id,
	} // Fix var-naming here
	args.params.assert.Contains(foundIDs, feat1.Id)
	args.params.assert.Contains(foundIDs, feat2.Id)
}

func testListByDOIValid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByDOIValid", func(t *testing.T) {
		testListByPublicationHelper(&testListByPublicationHelperParams{
			params:            params,
			publicationType:   "doi",
			publicationID:     "10.1234/j.abcd.2023.01.001",
			featureID1:        "DDB_G0285430",
			featureID2:        "DDB_G0285431",
			featureNamePrefix: "Feature DOI",
		})
	})
}

func testListByDOINotFound(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByDOINotFound", func(t *testing.T) {
		req := &feature.DOI{Id: "10.9999/non.existent.doi"} // Non-existent DOI
		_, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.NotFound, sts.Code())
	})
}

func testListByDOIInvalid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByDOIInvalid", func(t *testing.T) {
		req := &feature.DOI{Id: ""} // Invalid (empty) DOI
		_, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, sts.Code())
	})
}

func testListByPubmedIdValid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByPubmedIdValid", func(t *testing.T) {
		testListByPublicationHelper(&testListByPublicationHelperParams{
			params:            params,
			publicationType:   "pubmed",
			publicationID:     "12345678",
			featureID1:        "DDB_G0285428",
			featureID2:        "DDB_G0285429",
			featureNamePrefix: "Feature Pubmed",
		})
	})
}

func testListByPubmedIdNotFound(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByPubmedIdNotFound", func(t *testing.T) {
		req := &feature.PubmedId{Id: "99999999"} // Non-existent pubmed ID
		_, err := params.client.ListFeatureAnnotationsByPubmedId(
			params.ctx,
			req,
		)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.NotFound, sts.Code())
	})
}

func testListByPubmedIdInvalid(params *testParams) {
	params.t.Helper()
	params.t.Run("ListByPubmedIdInvalid", func(t *testing.T) {
		req := &feature.PubmedId{Id: ""} // Invalid (empty) pubmed ID
		_, err := params.client.ListFeatureAnnotationsByPubmedId(
			params.ctx,
			req,
		)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, sts.Code())
	})
}

func testCreateMissingFields(params *testParams) {
	params.t.Helper()
	params.t.Run("CreateFailsMissingRequiredFields", func(t *testing.T) {
		req := &feature.NewFeatureAnnotation{
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Invalid Feature",
			},
		}
		_, err := params.client.CreateFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, sts.Code())
	})
}

func testCreateDuplicateFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("CreateFailsDuplicateFeatureId", func(t *testing.T) {
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
		sts, ok := status.FromError(dupErr)
		params.assert.True(ok)
		params.assert.Equal(codes.AlreadyExists, sts.Code())
	})
}

func testGetExistingFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("GetExistingFeatureAnnotation", func(t *testing.T) {
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
		req := &feature.FeatureAnnotationId{
			Id: "DDB_G0000000",
		}
		_, err := params.client.GetFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.NotFound, sts.Code())
	})
}

func testGetFeatureWithInvalidID(params *testParams) {
	params.t.Helper()
	params.t.Run("GetFeatureAnnotationWithInvalidID", func(t *testing.T) {
		req := &feature.FeatureAnnotationId{
			Id: "", // Empty ID
		}
		_, err := params.client.GetFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, sts.Code())
	})
}

func testUpdateExistingFeature(params *testParams) {
	params.t.Helper()
	params.t.Run("UpdateExistingFeatureAnnotation", func(t *testing.T) {
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
		req := &feature.FeatureAnnotationUpdate{
			Id:        "DDB_G0000000",
			UpdatedBy: "testuser@dictybase.org",
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Non-existent Feature",
			},
		}
		_, err := params.client.UpdateFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		t.Log(sts.Code().String())
		params.assert.Equal(codes.Internal, sts.Code())
	})
}

func testUpdateWithInvalidData(params *testParams) {
	params.t.Helper()
	params.t.Run("UpdateFeatureAnnotationWithInvalidData", func(t *testing.T) {
		req := &feature.FeatureAnnotationUpdate{
			Id: "", // Empty ID
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "Invalid Feature",
			},
		}
		_, err := params.client.UpdateFeatureAnnotation(params.ctx, req)
		params.assert.Error(err)
		sts, ok := status.FromError(err)
		params.assert.True(ok)
		params.assert.Equal(codes.InvalidArgument, sts.Code())
	})
}

func testGetExistingFeatureByName(params *testParams) {
	testFeatureData := newTestFeature() // Assuming this helper exists and provides the data used in testCreateValidFeature
	testCreateValidFeature(
		params,
	) // Call this to ensure the feature is in the DB, ignore return

	// 2. Retrieve the feature by its known name.
	featureName := testFeatureData.Attributes.Name
	req := &feature.FeatureName{Name: featureName}
	gotFeat, err := params.client.GetFeatureAnnotationByName(params.ctx, req)

	params.assert.NoError(
		err,
		"should retrieve existing feature by name without error",
	)
	params.assert.Equal(
		featureName,
		gotFeat.Attributes.Name,
		"retrieved feature name should match the known name",
	)
	params.assert.Equal(
		testFeatureData.Id,
		gotFeat.Id,
		"retrieved feature entry_id should match",
	)
	slices.SortFunc(
		testFeatureData.Attributes.Properties,
		sortTagPropertiesByTag,
	)
	slices.SortFunc(gotFeat.Attributes.Properties, sortTagPropertiesByTag)
	params.assert.ElementsMatch(
		collection.Map(
			testFeatureData.Attributes.Properties,
			extractTagAndValue,
		),
		collection.Map(gotFeat.Attributes.Properties, extractTagAndValue),
		"should have matching properties",
	)
}

func testGetNonExistentFeatureByName(params *testParams) {
	nonExistentName := "this_feature_does_not_exist_12345"
	req := &feature.FeatureName{Name: nonExistentName}
	_, err := params.client.GetFeatureAnnotationByName(params.ctx, req)

	params.assert.Error(
		err,
		"should return an error for non-existent feature name",
	)
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.NotFound,
		expectedMsgSubstring: "not found",
	})
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

func testGetFeatureWithEmptyName(params *testParams) {
	req := &feature.FeatureName{Name: ""} // Empty name
	_, err := params.client.GetFeatureAnnotationByName(params.ctx, req)

	params.assert.Error(err, "should return an error for empty feature name")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.InvalidArgument,
		expectedMsgSubstring: "validation",
	})
}

// tagPropertyCreateParams holds the parameters for the
// createServiceTagPropertyCreate function.
type tagPropertyCreateParams struct {
	tag       string
	value     string
	createdBy string
	timestamp *time.Time
}

// Helper functions for AddTags tests

// createServiceTagPropertyCreate creates a TagPropertyCreate for service-level testing.
func createServiceTagPropertyCreate(
	params *tagPropertyCreateParams,
) *feature.TagPropertyCreate {
	tagCreate := &feature.TagPropertyCreate{
		Tag:       params.tag,
		Value:     params.value,
		CreatedBy: params.createdBy,
	}

	if params.timestamp != nil {
		tagCreate.CreatedAt = timestamppb.New(*params.timestamp)
	}

	return tagCreate
}

// createAddTagsServiceRequest creates an AddTagsRequest for service-level testing.
func createAddTagsServiceRequest(
	featureId string,
	tags []*feature.TagPropertyCreate,
) *feature.AddTagsRequest {
	return &feature.AddTagsRequest{
		Id:   featureId,
		Tags: tags,
	}
}

// verifyServiceTagsAddedParams holds the parameters for the
// verifyServiceTagsAdded function.
type verifyServiceTagsAddedParams struct {
	params           *testParams
	result           *feature.FeatureAnnotation
	expectedTags     []*feature.TagPropertyCreate
	originalTagCount int
}

// verifyServiceTagsAdded verifies that tags were correctly added at the service level.
func verifyServiceTagsAdded(args *verifyServiceTagsAddedParams) {
	args.params.t.Helper()

	// Verify tag count increased
	args.params.assert.Len(
		args.result.Attributes.Properties,
		args.originalTagCount+len(args.expectedTags),
		"should have original tags plus new tags",
	)

	// Verify each expected tag is present
	for _, expectedTag := range args.expectedTags {
		otk := slices.ContainsFunc(args.result.Attributes.Properties,
			func(prop *feature.TagProperty) bool {
				return prop.Tag == expectedTag.Tag &&
					prop.Value == expectedTag.Value &&
					prop.CreatedBy == expectedTag.CreatedBy
			})
		args.params.assert.True(
			otk,
			"should find tag %s",
			expectedTag.Tag,
		)
	}
}

func testAddTagsSuccess(params *testParams) {
	params.t.Helper()
	createReq := newTestFeature()
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")
	originalTagCount := len(created.Attributes.Properties)

	// Create tags to add
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "category",
			value:     "enzyme",
			createdBy: "tester@example.org",
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "priority",
			value:     "high",
			createdBy: "tester@example.org",
		}),
	}

	// Add tags
	addReq := createAddTagsServiceRequest(created.Id, newTags)
	result, err := params.client.AddTags(params.ctx, addReq)
	params.assert.NoError(err, "should successfully add tags")

	// Verify tags were added
	verifyServiceTagsAdded(&verifyServiceTagsAddedParams{
		params:           params,
		result:           result,
		expectedTags:     newTags,
		originalTagCount: originalTagCount,
	})
}

func testAddTagsSingleTag(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285501" // Use unique ID
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")
	originalTagCount := len(created.Attributes.Properties)

	// Create single tag to add
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "single_tag",
			value:     "single_value",
			createdBy: "tester@example.org",
		}),
	}

	// Add tags
	addReq := createAddTagsServiceRequest(created.Id, newTags)
	result, err := params.client.AddTags(params.ctx, addReq)
	params.assert.NoError(err, "should successfully add single tag")

	// Verify tag was added
	verifyServiceTagsAdded(&verifyServiceTagsAddedParams{
		params:           params,
		result:           result,
		expectedTags:     newTags,
		originalTagCount: originalTagCount,
	})
}

func testAddTagsMultipleTags(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285502" // Use unique ID
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")
	originalTagCount := len(created.Attributes.Properties)

	// Create multiple tags to add
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "category",
			value:     "enzyme",
			createdBy: "tester1@example.org",
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "organism",
			value:     "dictyostelium",
			createdBy: "tester2@example.org",
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "priority",
			value:     "high",
			createdBy: "tester1@example.org",
		}),
	}

	// Add tags
	addReq := createAddTagsServiceRequest(created.Id, newTags)
	result, err := params.client.AddTags(params.ctx, addReq)
	params.assert.NoError(err, "should successfully add multiple tags")

	// Verify tags were added
	verifyServiceTagsAdded(&verifyServiceTagsAddedParams{
		params:           params,
		result:           result,
		expectedTags:     newTags,
		originalTagCount: originalTagCount,
	})
}

func testAddTagsAppendToExisting(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285503" // Use unique ID
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")
	originalTags := created.Attributes.Properties
	originalTagCount := len(originalTags)

	// Create new tags with different names to ensure no conflicts
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "new_category",
			value:     "new_value",
			createdBy: "new_tester@example.org",
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "additional_info",
			value:     "extra_data",
			createdBy: "new_tester@example.org",
		}),
	}

	// Add tags
	addReq := createAddTagsServiceRequest(created.Id, newTags)
	result, err := params.client.AddTags(params.ctx, addReq)
	params.assert.NoError(
		err,
		"should successfully append tags to existing properties",
	)

	// Verify total count
	params.assert.Len(
		result.Attributes.Properties,
		originalTagCount+len(newTags),
		"should have original tags plus new tags",
	)

	// Verify original tags are preserved
	for _, originalTag := range originalTags {
		_, found := collection.Find(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == originalTag.Tag &&
					p.Value == originalTag.Value
			},
		)
		params.assert.True(
			found,
			"original tag %s should be preserved",
			originalTag.Tag,
		)
	}

	// Verify new tags were added
	verifyServiceTagsAdded(&verifyServiceTagsAddedParams{
		params:           params,
		result:           result,
		expectedTags:     newTags,
		originalTagCount: originalTagCount,
	})
}

func testAddTagsEmptyRequest(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285504" // Use unique ID
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Add empty tags (should fail validation)
	addReq := createAddTagsServiceRequest(
		created.Id,
		[]*feature.TagPropertyCreate{},
	)
	_, err = params.client.AddTags(params.ctx, addReq)

	params.assert.Error(err, "should return error for empty tags request")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.InvalidArgument,
		expectedMsgSubstring: "validation",
	})
}

func testAddTagsDefaultTimestamps(params *testParams) {
	params.t.Helper()
	testTimestampBehavior(
		params,
		"DDB_G0285505",
		false,
		func(featureID string, tags []*feature.TagPropertyCreate) (*feature.FeatureAnnotation, error) {
			addReq := createAddTagsServiceRequest(featureID, tags)
			return params.client.AddTags(params.ctx, addReq)
		},
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Create tags without timestamps
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"auto_timestamp1",
			"value1",
			"tester@example.org",
			nil,
		),
		createServiceTagPropertyCreate(
			"auto_timestamp2",
			"value2",
			"tester@example.org",
			nil,
		),
	}

	// Add tags
	addReq := createAddTagsServiceRequest(created.Id, newTags)
	result, err := params.client.AddTags(params.ctx, addReq)
	params.assert.NoError(
		err,
		"should successfully add tags with default timestamps",
	)

	// Verify timestamps are auto-generated and recent
	for _, expectedTag := range newTags {
		found, otk := collection.Find(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		params.assert.True(otk, "should find tag %s", expectedTag.Tag)
		params.assert.WithinDuration(
			time.Now(),
			(*found).CreatedAt.AsTime(),
			5*time.Second,
			"CreatedAt should be recent for tag %s",
			expectedTag.Tag,
		)
	}
}

func testAddTagsProvidedTimestamps(params *testParams) {
	params.t.Helper()
	testTimestampBehavior(
		params,
		"DDB_G0285506",
		true,
		func(featureID string, tags []*feature.TagPropertyCreate) (*feature.FeatureAnnotation, error) {
			addReq := createAddTagsServiceRequest(featureID, tags)
			return params.client.AddTags(params.ctx, addReq)
		},
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Create tags with specific timestamps
	specTs1 := time.Now().
		Add(-48 * time.Hour).
		UTC().
		Truncate(time.Microsecond)
	specTs2 := time.Now().
		Add(-24 * time.Hour).
		UTC().
		Truncate(time.Microsecond)
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "provided_timestamp1",
			value:     "value1",
			createdBy: "tester@example.org",
			timestamp: &specTs1,
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "provided_timestamp2",
			value:     "value2",
			createdBy: "tester@example.org",
			timestamp: &specTs2,
		}),
	}

	// Add tags
	addReq := createAddTagsServiceRequest(created.Id, newTags)
	result, err := params.client.AddTags(params.ctx, addReq)
	params.assert.NoError(
		err,
		"should successfully add tags with provided timestamps",
	)

	// Verify provided timestamps are preserved
	expectedTimestamps := []time.Time{specTs1, specTs2}
	for idx, expectedTag := range newTags {
		found, otk := collection.Find(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		params.assert.True(otk, "should find tag %s", expectedTag.Tag)
		params.assert.Equal(
			expectedTimestamps[idx].Truncate(time.Second),
			(*found).CreatedAt.AsTime().Truncate(time.Second),
			"CreatedAt should match provided timestamp for tag %s",
			expectedTag.Tag,
		)
	}
}

func testAddTagsNonExistentFeature(params *testParams) {
	params.t.Helper()
	// Create tags to add
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "test_tag",
			value:     "test_value",
			createdBy: "tester@example.org",
		}),
	}

	// Attempt to add tags to non-existent feature
	addReq := createAddTagsServiceRequest("DDB_G0000000", newTags)
	_, err := params.client.AddTags(params.ctx, addReq)

	params.assert.Error(err, "should return error for non-existent feature")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.NotFound,
		expectedMsgSubstring: "not found",
	})
}

func testAddTagsInvalidRequest(params *testParams) {
	params.t.Helper()
	// Create tags with invalid data
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "",
			value:     "test_value",
			createdBy: "tester@example.org",
		}), // Empty tag name
	}

	// Attempt to add invalid tags
	addReq := createAddTagsServiceRequest("DDB_G0285425", newTags)
	_, err := params.client.AddTags(params.ctx, addReq)

	params.assert.Error(err, "should return error for invalid request")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.InvalidArgument,
		expectedMsgSubstring: "validation",
	})
}

// Helper functions for SetTags tests

// createSetTagsServiceRequest creates a SetTagsRequest for service-level testing.
func createSetTagsServiceRequest(
	featureId string,
	tags []*feature.TagPropertyCreate,
) *feature.SetTagsRequest {
	return &feature.SetTagsRequest{
		Id:   featureId,
		Tags: tags,
	}
}

// verifyServiceTagsSet verifies that tags were correctly set at the service level (replaces all existing tags).
func verifyServiceTagsSet(
	params *testParams,
	result *feature.FeatureAnnotation,
	expectedTags []*feature.TagPropertyCreate,
) {
	params.t.Helper()

	// Verify tag count matches exactly (should replace, not append)
	params.assert.Len(
		result.Attributes.Properties,
		len(expectedTags),
		"should have exactly the number of new tags",
	)

	// Verify each expected tag is present
	for _, expectedTag := range expectedTags {
		found, otk := collection.Find(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == expectedTag.Tag && p.Value == expectedTag.Value
			},
		)
		params.assert.True(otk, "should find tag %s", expectedTag.Tag)
		params.assert.Equal(
			expectedTag.CreatedBy,
			(*found).CreatedBy,
			"should match created by for tag %s",
			expectedTag.Tag,
		)
	}
}

// verifyTagTimestamps verifies that tag timestamps are correct (either auto-generated or preserved).
func verifyTagTimestamps(
	params *testParams,
	result *feature.FeatureAnnotation,
	expectedTags []*feature.TagPropertyCreate,
	expectedTimestamps []time.Time,
	autoGenerated bool,
) {
	params.t.Helper()

	if autoGenerated {
		// Verify timestamps are auto-generated and recent
		for _, expectedTag := range expectedTags {
			found, otk := collection.Find(
				result.Attributes.Properties,
				func(p *feature.TagProperty) bool {
					return p.Tag == expectedTag.Tag
				},
			)
			params.assert.True(otk, "should find tag %s", expectedTag.Tag)
			params.assert.WithinDuration(
				time.Now(),
				(*found).CreatedAt.AsTime(),
				5*time.Second,
				"CreatedAt should be recent for tag %s",
				expectedTag.Tag,
			)
		}
	} else {
		// Verify provided timestamps are preserved
		for idx, expectedTag := range expectedTags {
			found, otk := collection.Find(
				result.Attributes.Properties,
				func(p *feature.TagProperty) bool {
					return p.Tag == expectedTag.Tag
				},
			)
			params.assert.True(otk, "should find tag %s", expectedTag.Tag)
			params.assert.Equal(
				expectedTimestamps[idx].Truncate(time.Second),
				(*found).CreatedAt.AsTime().Truncate(time.Second),
				"CreatedAt should match provided timestamp for tag %s",
				expectedTag.Tag,
			)
		}
	}
}

// createTestTagsWithTimestamps creates test tags with specific timestamps.
func createTestTagsWithTimestamps() ([]*feature.TagPropertyCreate, []time.Time) {
	specTs1 := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Microsecond)
	specTs2 := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Microsecond)
	expectedTimestamps := []time.Time{specTs1, specTs2}
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate("provided_timestamp1", "value1", "tester@example.org", &specTs1),
		createServiceTagPropertyCreate("provided_timestamp2", "value2", "tester@example.org", &specTs2),
	}
	return newTags, expectedTimestamps
}

// createTestTagsWithoutTimestamps creates test tags without timestamps.
func createTestTagsWithoutTimestamps() []*feature.TagPropertyCreate {
	return []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate("auto_timestamp1", "value1", "tester@example.org", nil),
		createServiceTagPropertyCreate("auto_timestamp2", "value2", "tester@example.org", nil),
	}
}

// testTimestampBehavior tests timestamp behavior for tag operations (both AddTags and SetTags).
func testTimestampBehavior(
	params *testParams,
	featureID string,
	withProvidedTimestamps bool,
	operation func(string, []*feature.TagPropertyCreate) (*feature.FeatureAnnotation, error),
) {
	params.t.Helper()

	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = featureID
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")

	var newTags []*feature.TagPropertyCreate
	var expectedTimestamps []time.Time

	if withProvidedTimestamps {
		newTags, expectedTimestamps = createTestTagsWithTimestamps()
	} else {
		newTags = createTestTagsWithoutTimestamps()
	}

	// Perform the operation
	result, err := operation(created.Id, newTags)
	params.assert.NoError(err, "should successfully perform tag operation")

	// Verify timestamps
	verifyTagTimestamps(
		params,
		result,
		newTags,
		expectedTimestamps,
		!withProvidedTimestamps,
	)
}

func testSetTagsSuccess(params *testParams) {
	params.t.Helper()
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285601"
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Create tags to set (replace existing ones)
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"category",
			"enzyme",
			"tester@example.org",
			nil,
		),
		createServiceTagPropertyCreate(
			"priority",
			"high",
			"tester@example.org",
			nil,
		),
	}

	// Set tags
	setReq := createSetTagsServiceRequest(created.Id, newTags)
	result, err := params.client.SetTags(params.ctx, setReq)
	params.assert.NoError(err, "should successfully set tags")

	// Verify tags were set (replaced)
	verifyServiceTagsSet(params, result, newTags)
}

func testSetTagsSingleTag(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285602"
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Create single tag to set
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"single_tag",
			"single_value",
			"tester@example.org",
			nil,
		),
	}

	// Set tags
	setReq := createSetTagsServiceRequest(created.Id, newTags)
	result, err := params.client.SetTags(params.ctx, setReq)
	params.assert.NoError(err, "should successfully set single tag")

	// Verify tag was set
	verifyServiceTagsSet(params, result, newTags)
}

func testSetTagsMultipleTags(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285603"
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Create multiple tags to set
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"category",
			"enzyme",
			"tester1@example.org",
			nil,
		),
		createServiceTagPropertyCreate(
			"organism",
			"dictyostelium",
			"tester2@example.org",
			nil,
		),
		createServiceTagPropertyCreate(
			"priority",
			"high",
			"tester1@example.org",
			nil,
		),
	}

	// Set tags
	setReq := createSetTagsServiceRequest(created.Id, newTags)
	result, err := params.client.SetTags(params.ctx, setReq)
	params.assert.NoError(err, "should successfully set multiple tags")

	// Verify tags were set
	verifyServiceTagsSet(params, result, newTags)
}

func testSetTagsReplaceExisting(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285604"
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")
	originalTags := created.Attributes.Properties

	// Verify original tags exist
	params.assert.NotEmpty(
		originalTags,
		"should have original tags to replace",
	)

	// Create completely new set of tags
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"replacement_category",
			"replacement_value",
			"replacement_tester@example.org",
			nil,
		),
		createServiceTagPropertyCreate(
			"new_info",
			"new_data",
			"replacement_tester@example.org",
			nil,
		),
	}

	// Set tags (should replace all existing)
	setReq := createSetTagsServiceRequest(created.Id, newTags)
	result, err := params.client.SetTags(params.ctx, setReq)
	params.assert.NoError(
		err,
		"should successfully replace existing tags",
	)

	// Verify old tags are gone and new tags are present
	params.assert.Len(
		result.Attributes.Properties,
		len(newTags),
		"should have only the new tags",
	)

	// Verify original tags are no longer present
	for _, originalTag := range originalTags {
		_, found := collection.Find(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == originalTag.Tag &&
					p.Value == originalTag.Value
			},
		)
		params.assert.False(
			found,
			"original tag %s should be removed",
			originalTag.Tag,
		)
	}

	// Verify new tags were set
	verifyServiceTagsSet(params, result, newTags)
}

func testSetTagsEmptyRequest(params *testParams) {
	params.t.Helper()
	// Create a feature first
	createReq := newTestFeature()
	createReq.Id = "DDB_G0285605"
	created, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err, "should successfully create test feature")

	// Set empty tags (should fail validation)
	setReq := createSetTagsServiceRequest(
		created.Id,
		[]*feature.TagPropertyCreate{},
	)
	_, err = params.client.SetTags(params.ctx, setReq)

	params.assert.Error(err, "should return error for empty tags request")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.InvalidArgument,
		expectedMsgSubstring: "validation",
	})
}

func testSetTagsDefaultTimestamps(params *testParams) {
	params.t.Helper()
	testTimestampBehavior(
		params,
		"DDB_G0285606",
		false,
		func(
			featureID string,
			tags []*feature.TagPropertyCreate,
		) (*feature.FeatureAnnotation, error) {
			setReq := createSetTagsServiceRequest(featureID, tags)
			return params.client.SetTags(params.ctx, setReq)
		},
	)
}

func testSetTagsProvidedTimestamps(params *testParams) {
	params.t.Helper()
	testTimestampBehavior(
		params,
		"DDB_G0285607",
		true,
		func(featureID string, tags []*feature.TagPropertyCreate) (*feature.FeatureAnnotation, error) {
			setReq := createSetTagsServiceRequest(featureID, tags)
			return params.client.SetTags(params.ctx, setReq)
		},
	)
}

func testSetTagsNonExistentFeature(params *testParams) {
	params.t.Helper()
	// Create tags to set
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"test_tag",
			"test_value",
			"tester@example.org",
			nil,
		),
	}

	// Attempt to set tags on non-existent feature
	setReq := createSetTagsServiceRequest("DDB_G0000000", newTags)
	_, err := params.client.SetTags(params.ctx, setReq)

	params.assert.Error(err, "should return error for non-existent feature")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.NotFound,
		expectedMsgSubstring: "not found",
	})
}

func testSetTagsInvalidRequest(params *testParams) {
	params.t.Helper()
	// Create tags with invalid data
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(
			"",
			"test_value",
			"tester@example.org",
			nil,
		), // Empty tag name
	}

	// Attempt to set invalid tags
	setReq := createSetTagsServiceRequest("DDB_G0285425", newTags)
	_, err := params.client.SetTags(params.ctx, setReq)

	params.assert.Error(err, "should return error for invalid request")
	assertGrpcError(assertGrpcErrorParams{
		assert:               params.assert,
		err:                  err,
		expectedCode:         codes.InvalidArgument,
		expectedMsgSubstring: "validation",
	})
}
