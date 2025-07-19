package service

import (
	"slices"
	"time"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// verifyServiceTagsAddedParams holds the parameters for the
// verifyServiceTagsAdded function.
type verifyServiceTagsAddedParams struct {
	params           *testParams
	result           *feature.FeatureAnnotation
	expectedTags     []*feature.TagPropertyCreate
	originalTagCount int
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
		pdx := slices.IndexFunc(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == originalTag.Tag &&
					p.Value == originalTag.Value
			},
		)
		params.assert.Greater(
			pdx,
			-1,
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
		func(
			featureID string,
			tags []*feature.TagPropertyCreate,
		) (*feature.FeatureAnnotation, error) {
			addReq := createAddTagsServiceRequest(featureID, tags)
			return params.client.AddTags(params.ctx, addReq)
		},
	)
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
		pdx := slices.IndexFunc(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == expectedTag.Tag && p.Value == expectedTag.Value
			},
		)
		params.assert.Greater(pdx, -1, "should find tag %s", expectedTag.Tag)
		params.assert.Equal(
			expectedTag.CreatedBy,
			result.Attributes.Properties[pdx].CreatedBy,
			"should match created by for tag %s",
			expectedTag.Tag,
		)
	}
}

// verifyAutoGeneratedTimestamps verifies that tag timestamps are auto-generated and recent.
func verifyAutoGeneratedTimestamps(
	params *testParams,
	result *feature.FeatureAnnotation,
	expectedTags []*feature.TagPropertyCreate,
) {
	params.t.Helper()
	// Verify timestamps are auto-generated and recent
	for _, expectedTag := range expectedTags {
		pdx := slices.IndexFunc(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		params.assert.Greater(
			pdx,
			-1,
			"should find tag %s",
			expectedTag.Tag,
		)
		params.assert.WithinDuration(
			time.Now(),
			result.Attributes.Properties[pdx].CreatedAt.AsTime(),
			5*time.Second,
			"CreatedAt should be recent for tag %s",
			expectedTag.Tag,
		)
	}
}

// verifyProvidedTimestamps verifies that provided timestamps are preserved.
func verifyProvidedTimestamps(
	params *testParams,
	result *feature.FeatureAnnotation,
	expectedTags []*feature.TagPropertyCreate,
	expectedTimestamps []time.Time,
) {
	params.t.Helper()
	// Verify provided timestamps are preserved
	for idx, expectedTag := range expectedTags {
		pdx := slices.IndexFunc(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		params.assert.Greater(
			pdx,
			-1,
			"should find tag %s",
			expectedTag.Tag,
		)
		params.assert.Equal(
			expectedTimestamps[idx].Truncate(time.Second),
			result.Attributes.Properties[pdx].CreatedAt.AsTime().
				Truncate(time.Second),
			"CreatedAt should match provided timestamp for tag %s",
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
		verifyAutoGeneratedTimestamps(params, result, expectedTags)
	} else {
		verifyProvidedTimestamps(params, result, expectedTags, expectedTimestamps)
	}
}

// createTestTagsWithTimestamps creates test tags with specific timestamps.
func createTestTagsWithTimestamps() ([]*feature.TagPropertyCreate, []time.Time) {
	specTs1 := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Microsecond)
	specTs2 := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Microsecond)
	expectedTimestamps := []time.Time{specTs1, specTs2}
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
	return newTags, expectedTimestamps
}

// createTestTagsWithoutTimestamps creates test tags without timestamps.
func createTestTagsWithoutTimestamps() []*feature.TagPropertyCreate {
	return []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "auto_timestamp1",
			value:     "value1",
			createdBy: "tester@example.org",
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "auto_timestamp2",
			value:     "value2",
			createdBy: "tester@example.org",
		}),
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
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "single_tag",
			value:     "single_value",
			createdBy: "tester@example.org",
		}),
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
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "replacement_category",
			value:     "replacement_value",
			createdBy: "replacement_tester@example.org",
		}),
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "new_info",
			value:     "new_data",
			createdBy: "replacement_tester@example.org",
		}),
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
		pdx := slices.IndexFunc(
			result.Attributes.Properties,
			func(p *feature.TagProperty) bool {
				return p.Tag == originalTag.Tag &&
					p.Value == originalTag.Value
			},
		)
		params.assert.Equal(
			-1,
			pdx,
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
		func(
			featureID string,
			tags []*feature.TagPropertyCreate,
		) (*feature.FeatureAnnotation, error) {
			setReq := createSetTagsServiceRequest(featureID, tags)
			return params.client.SetTags(params.ctx, setReq)
		},
	)
}

func testSetTagsNonExistentFeature(params *testParams) {
	params.t.Helper()
	// Create tags to set
	newTags := []*feature.TagPropertyCreate{
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "test_tag",
			value:     "test_value",
			createdBy: "tester@example.org",
		}),
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
		createServiceTagPropertyCreate(&tagPropertyCreateParams{
			tag:       "",
			value:     "test_value",
			createdBy: "tester@example.org",
		}), // Empty tag name
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
