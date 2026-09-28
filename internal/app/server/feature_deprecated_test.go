package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeprecatedMethodInterceptorRejects(t *testing.T) {
	t.Parallel()
	handler := func(
		_ context.Context,
		_ any,
	) (any, error) {
		return "reached-handler", nil
	}
	for _, method := range []string{
		"/dictybase.feature_annotation.FeatureAnnotationService/UpdateTag",
		"/dictybase.feature_annotation.FeatureAnnotationService/RemoveTag",
	} {
		info := &grpc.UnaryServerInfo{FullMethod: method}
		resp, err := DeprecatedMethodInterceptor(
			context.Background(),
			nil,
			info,
			handler,
		)
		require.Nil(t, resp, "expect no response for %s", method)
		require.Error(t, err, "expect error for %s", method)
		require.Equal(
			t,
			codes.Unimplemented,
			status.Code(err),
			"expect Unimplemented for %s",
			method,
		)
		require.Contains(
			t,
			status.Convert(err).Message(),
			"deprecated",
			"expect explanatory message for %s",
			method,
		)
	}
}

func TestDeprecatedMethodInterceptorPassesThrough(t *testing.T) {
	t.Parallel()
	handler := func(
		_ context.Context,
		_ any,
	) (any, error) {
		return "reached-handler", nil
	}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/dictybase.feature_annotation.FeatureAnnotationService/RemoveTags",
	}
	resp, err := DeprecatedMethodInterceptor(
		context.Background(),
		nil,
		info,
		handler,
	)
	require.NoError(t, err)
	require.Equal(t, "reached-handler", resp)
}
