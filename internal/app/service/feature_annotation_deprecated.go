package service

import (
	"context"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//nolint:staticcheck // SA1019: Using deprecated types in deprecated method implementation
func (srv *FeatureAnnotationService) UpdateTag(
	ctx context.Context,
	req *feature.UpdateTagRequest,
) (*feature.FeatureAnnotation, error) {
	//nolint:wrapcheck // gRPC status errors should not be wrapped
	return nil, status.Error(
		codes.Unimplemented,
		"UpdateTag method is deprecated and no longer supported. "+
			"Use RemoveTags followed by AddTags, or SetTags for complete tag replacement",
	)
}

//nolint:staticcheck // SA1019: Using deprecated types in deprecated method implementation
func (srv *FeatureAnnotationService) RemoveTag(
	ctx context.Context,
	req *feature.RemoveTagRequest,
) (*feature.FeatureAnnotation, error) {
	//nolint:wrapcheck // gRPC status errors should not be wrapped
	return nil, status.Error(
		codes.Unimplemented,
		"RemoveTag method is deprecated and no longer supported. Use RemoveTags method instead",
	)
}
