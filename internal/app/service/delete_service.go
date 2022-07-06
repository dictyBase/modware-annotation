package service

import (
	"context"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

func (s *AnnotationService) DeleteAnnotationGroup(ctx context.Context, r *annotation.GroupEntryId) (*empty.Empty, error) {
	emt := &empty.Empty{}
	if err := r.Validate(); err != nil {
		return emt, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotationGroup(r.GroupId); err != nil {
		return emt, aphgrpc.HandleDeleteError(ctx, err)
	}

	return emt, nil
}

func (s *AnnotationService) DeleteAnnotation(ctx context.Context, r *annotation.DeleteAnnotationRequest) (*empty.Empty, error) {
	emt := &empty.Empty{}
	if err := r.Validate(); err != nil {
		return emt, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotation(r.Id, r.Purge); err != nil {
		if repository.IsAnnotationNotFound(err) {
			return emt, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return emt, aphgrpc.HandleDeleteError(ctx, err)
	}

	return emt, nil
}
