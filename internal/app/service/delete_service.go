package service

import (
	"context"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

func (s *AnnotationService) DeleteAnnotationGroup(ctx context.Context, r *annotation.GroupEntryId) (*empty.Empty, error) {
	e := &empty.Empty{}
	if err := r.Validate(); err != nil {
		return e, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotationGroup(r.GroupId); err != nil {
		return e, aphgrpc.HandleDeleteError(ctx, err)
	}
	return e, nil
}

func (s *AnnotationService) DeleteAnnotation(ctx context.Context, r *annotation.DeleteAnnotationRequest) (*empty.Empty, error) {
	e := &empty.Empty{}
	if err := r.Validate(); err != nil {
		return e, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotation(r.Id, r.Purge); err != nil {
		if repository.IsAnnotationNotFound(err) {
			return e, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return e, aphgrpc.HandleDeleteError(ctx, err)
	}
	return e, nil
}
