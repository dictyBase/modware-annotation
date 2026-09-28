package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deprecatedFeatureMethods maps fully qualified deprecated feature annotation
// RPCs to the explanatory message clients receive instead of invoking them.
var deprecatedFeatureMethods = map[string]string{
	"/dictybase.feature_annotation.FeatureAnnotationService/UpdateTag": "UpdateTag method is deprecated and no longer supported. " +
		"Use RemoveTags followed by AddTags, or SetTags for complete tag replacement",
	"/dictybase.feature_annotation.FeatureAnnotationService/RemoveTag": "RemoveTag method is deprecated and no longer supported. " +
		"Use RemoveTags method instead",
}

// DeprecatedMethodInterceptor rejects calls to deprecated RPCs with an
// explanatory Unimplemented error before they reach the service handlers.
func DeprecatedMethodInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if msg, ok := deprecatedFeatureMethods[info.FullMethod]; ok {
		return nil, status.Error(codes.Unimplemented, msg)
	}

	return handler(ctx, req)
}
