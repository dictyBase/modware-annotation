package service

import (
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	testListByPublicationHelper(&testListByPublicationHelperParams{
		params:            params,
		publicationType:   "doi",
		publicationID:     "10.1234/j.abcd.2023.01.001",
		featureID1:        "DDB_G0285430",
		featureID2:        "DDB_G0285431",
		featureNamePrefix: "Feature DOI",
	})
}

func testListByDOINotFound(params *testParams) {
	params.t.Helper()
	req := &feature.DOI{Id: "10.9999/non.existent.doi"} // Non-existent DOI
	_, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
	params.assert.Error(err)
	sts, ok := status.FromError(err)
	params.assert.True(ok)
	params.assert.Equal(codes.NotFound, sts.Code())
}

func testListByDOIInvalid(params *testParams) {
	params.t.Helper()
	req := &feature.DOI{Id: ""} // Invalid (empty) DOI
	_, err := params.client.ListFeatureAnnotationsByDOI(params.ctx, req)
	params.assert.Error(err)
	sts, ok := status.FromError(err)
	params.assert.True(ok)
	params.assert.Equal(codes.InvalidArgument, sts.Code())
}

func testListByPubmedIdValid(params *testParams) {
	params.t.Helper()
	testListByPublicationHelper(&testListByPublicationHelperParams{
		params:            params,
		publicationType:   "pubmed",
		publicationID:     "12345678",
		featureID1:        "DDB_G0285428",
		featureID2:        "DDB_G0285429",
		featureNamePrefix: "Feature Pubmed",
	})
}

func testListByPubmedIdNotFound(params *testParams) {
	params.t.Helper()
	req := &feature.PubmedId{Id: "99999999"} // Non-existent pubmed ID
	_, err := params.client.ListFeatureAnnotationsByPubmedId(
		params.ctx,
		req,
	)
	params.assert.Error(err)
	sts, ok := status.FromError(err)
	params.assert.True(ok)
	params.assert.Equal(codes.NotFound, sts.Code())
}

func testListByPubmedIdInvalid(params *testParams) {
	params.t.Helper()
	req := &feature.PubmedId{Id: ""} // Invalid (empty) pubmed ID
	_, err := params.client.ListFeatureAnnotationsByPubmedId(
		params.ctx,
		req,
	)
	params.assert.Error(err)
	sts, ok := status.FromError(err)
	params.assert.True(ok)
	params.assert.Equal(codes.InvalidArgument, sts.Code())
}
