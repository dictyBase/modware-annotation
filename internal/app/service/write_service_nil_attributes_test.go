package service

import (
	"context"
	"testing"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockTaggedAnnotationRepo is a stub repository for testing service level
// validation without touching a database. Only methods invoked before the
// missing-attributes guard would be reached; unimplemented calls panic.
type mockTaggedAnnotationRepo struct {
	repository.TaggedAnnotationRepository
}

// MockTaggedMessage is a no-op tagged annotation message publisher.
type MockTaggedMessage struct{}

// Publish implements the message.Publisher interface as a no-op.
func (mtm *MockTaggedMessage) Publish(
	_ string,
	_ *annotation.TaggedAnnotation,
) error {
	return nil
}

// Close implements the message.Publisher interface as a no-op.
func (mtm *MockTaggedMessage) Close() error {
	return nil
}

func newTestAnnotationService(t *testing.T) *AnnotationService {
	t.Helper()
	svc, err := NewAnnotationService(&Params{
		Repository: &mockTaggedAnnotationRepo{},
		Publisher:  &MockTaggedMessage{},
		Options: []aphgrpc.Option{
			aphgrpc.TopicsOption(map[string]string{
				"annotationCreate": "annotation_create_test",
				"annotationUpdate": "annotation_update_test",
			}),
		},
		Group: "annotation_group_test",
	})
	require.NoError(t, err, "expect no error creating annotation service")

	return svc
}

func TestCreateAnnotationMissingAttributes(t *testing.T) {
	t.Parallel()
	svc := newTestAnnotationService(t)
	req := &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{Type: "annotations"},
	}
	_, err := svc.CreateAnnotation(context.Background(), req)
	require.Error(t, err, "expect error for missing attributes")
	require.Equal(
		t,
		codes.InvalidArgument,
		status.Code(err),
		"expect InvalidArgument for missing attributes",
	)
}

func TestUpdateAnnotationMissingAttributes(t *testing.T) {
	t.Parallel()
	svc := newTestAnnotationService(t)
	req := &annotation.TaggedAnnotationUpdate{
		Data: &annotation.TaggedAnnotationUpdate_Data{
			Type: "annotations",
			Id:   "DDB_G0267474",
		},
	}
	_, err := svc.UpdateAnnotation(context.Background(), req)
	require.Error(t, err, "expect error for missing attributes")
	require.Equal(
		t,
		codes.InvalidArgument,
		status.Code(err),
		"expect InvalidArgument for missing attributes",
	)
}

func TestCreateAnnotationMissingData(t *testing.T) {
	t.Parallel()
	svc := newTestAnnotationService(t)
	req := &annotation.NewTaggedAnnotation{}
	_, err := svc.CreateAnnotation(context.Background(), req)
	require.Error(t, err, "expect error for missing data")
	require.Equal(
		t,
		codes.InvalidArgument,
		status.Code(err),
		"expect InvalidArgument for missing data",
	)
}
