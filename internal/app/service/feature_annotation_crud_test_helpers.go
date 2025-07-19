package service

import (
	"slices"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testCreateValidFeature(params *testParams) {
	params.t.Helper()
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
	params.assert.Equal(
		req.Attributes.Synonyms,
		resp.Attributes.Synonyms,
	)

	// Validate properties
	params.assert.Len(resp.Attributes.Properties, 2)
	slices.SortFunc(
		req.Attributes.Properties,
		sortTagPropertiesByTag,
	)
	slices.SortFunc(
		resp.Attributes.Properties,
		sortTagPropertiesByTag,
	)
	params.assert.ElementsMatch(
		collection.Map(
			req.Attributes.Properties,
			extractTagAndValue,
		),
		collection.Map(
			resp.Attributes.Properties,
			extractTagAndValue,
		),
		"should have matching properties",
	)
}

func testCreateMissingFields(params *testParams) {
	params.t.Helper()
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
}

func testCreateDuplicateFeature(params *testParams) {
	params.t.Helper()
	req := &feature.NewFeatureAnnotation{
		Id:        "DDB_G02854297",
		CreatedBy: "testuser@dictybase.org",
		CreatedAt: timestamppb.Now(),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "Duplicate Feature",
		},
	}
	_, firstErr := params.client.CreateFeatureAnnotation(
		params.ctx,
		req,
	)
	params.assert.NoError(firstErr)
	_, dupErr := params.client.CreateFeatureAnnotation(
		params.ctx,
		req,
	)
	params.assert.Error(dupErr)
	sts, ok := status.FromError(dupErr)
	params.assert.True(ok)
	params.assert.Equal(codes.AlreadyExists, sts.Code())
}

func testGetExistingFeature(params *testParams) {
	params.t.Helper()
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
	_, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
	params.assert.NoError(err)

	// Then retrieve it
	getReq := &feature.FeatureAnnotationId{
		Id: "DDB_G0285426",
	}
	resp, err := params.client.GetFeatureAnnotation(params.ctx, getReq)
	params.assert.NoError(err)
	params.assert.Equal(createReq.Id, resp.Id)
	params.assert.Equal(createReq.CreatedBy, resp.CreatedBy)
	params.assert.Equal(
		createReq.Attributes.Name,
		resp.Attributes.Name,
	)
	params.assert.Equal(
		createReq.Attributes.Synonyms,
		resp.Attributes.Synonyms,
	)
}

func testGetNonExistentFeature(params *testParams) {
	params.t.Helper()
	req := &feature.FeatureAnnotationId{
		Id: "DDB_G0000000",
	}
	_, err := params.client.GetFeatureAnnotation(params.ctx, req)
	params.assert.Error(err)
	sts, ok := status.FromError(err)
	params.assert.True(ok)
	params.assert.Equal(codes.NotFound, sts.Code())
}

func testGetFeatureWithInvalidID(params *testParams) {
	params.t.Helper()
	req := &feature.FeatureAnnotationId{
		Id: "", // Empty ID
	}
	_, err := params.client.GetFeatureAnnotation(params.ctx, req)
	params.assert.Error(err)
	sts, ok := status.FromError(err)
	params.assert.True(ok)
	params.assert.Equal(codes.InvalidArgument, sts.Code())
}

func testUpdateExistingFeature(params *testParams) {
	params.t.Helper()
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
	_, err := params.client.CreateFeatureAnnotation(
		params.ctx,
		createReq,
	)
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
	params.assert.Equal(
		updateReq.Attributes.Name,
		resp.Attributes.Name,
	)
	params.assert.ElementsMatch(
		slices.Concat(
			createReq.Attributes.Synonyms,
			updateReq.Attributes.Synonyms,
		),
		resp.Attributes.Synonyms,
	)
	params.assert.Equal(createReq.CreatedBy, resp.CreatedBy)
}

func testUpdateNonExistentFeature(params *testParams) {
	params.t.Helper()
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
	params.t.Log(sts.Code().String())
	params.assert.Equal(codes.Internal, sts.Code())
}

func testUpdateWithInvalidData(params *testParams) {
	params.t.Helper()
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
