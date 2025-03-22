package service

import (
	"context"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

func (s *AnnotationService) DeleteAnnotationGroup(ctx context.Context, r *annotation.GroupEntryId) (*empty.Empty, error) {
	if err := protovalidate.Validate(r); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotationGroup(r.GroupId); err != nil {
		return nil, aphgrpc.HandleDeleteError(ctx, err)
	}

	return &empty.Empty{}, nil
}

func (s *AnnotationService) DeleteAnnotation(ctx context.Context, r *annotation.DeleteAnnotationRequest) (*empty.Empty, error) {
	if err := protovalidate.Validate(r); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotation(r.Id, r.Purge); err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleDeleteError(ctx, err)
	}

	return &empty.Empty{}, nil
}
