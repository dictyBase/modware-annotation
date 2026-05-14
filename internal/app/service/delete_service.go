// Package service implements the gRPC service handlers for the annotation service.
package service

import (
	"context"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

// DeleteAnnotationGroup removes an annotation group identified by its group ID.
func (srv *AnnotationService) DeleteAnnotationGroup(ctx context.Context, r *annotation.GroupEntryId) (*empty.Empty, error) {
	if err := protovalidate.Validate(r); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := srv.repo.RemoveAnnotationGroup(r.GroupId); err != nil {
		return nil, aphgrpc.HandleDeleteError(ctx, err)
	}

	return &empty.Empty{}, nil
}

// DeleteAnnotation removes a tagged annotation by its ID.
func (srv *AnnotationService) DeleteAnnotation(ctx context.Context, r *annotation.DeleteAnnotationRequest) (*empty.Empty, error) {
	if err := protovalidate.Validate(r); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := srv.repo.RemoveAnnotation(r.Id, r.Purge); err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleDeleteError(ctx, err)
	}

	return &empty.Empty{}, nil
}
