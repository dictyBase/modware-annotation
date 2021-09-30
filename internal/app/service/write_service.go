package service

import (
	"context"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (s *AnnotationService) UpdateAnnotation(
	ctx context.Context,
	r *annotation.TaggedAnnotationUpdate,
) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.EditAnnotation(r)
	if err != nil {
		return ta, aphgrpc.HandleUpdateError(ctx, err)
	}
	if m.NotFound {
		return ta, aphgrpc.HandleNotFoundError(ctx, err)
	}
	ta.Data = s.getAnnoData(m)
	err = s.publisher.Publish(s.Topics["annotationUpdate"], ta)
	if err != nil {
		return ta, aphgrpc.HandleUpdateError(ctx, err)
	}
	return ta, nil
}

func (s *AnnotationService) CreateAnnotation(
	ctx context.Context,
	r *annotation.NewTaggedAnnotation,
) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.AddAnnotation(r)
	if err != nil {
		return ta, aphgrpc.HandleInsertError(ctx, err)
	}
	ta.Data = s.getAnnoData(m)
	err = s.publisher.Publish(s.Topics["annotationCreate"], ta)
	if err != nil {
		return ta, aphgrpc.HandleInsertError(ctx, err)
	}
	return ta, nil
}

func (s *AnnotationService) AddToAnnotationGroup(
	ctx context.Context, r *annotation.AnnotationGroupId,
) (*annotation.TaggedAnnotationGroup, error) {
	g := &annotation.TaggedAnnotationGroup{}
	if err := r.Validate(); err != nil {
		return g, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mg, err := s.repo.AppendToAnnotationGroup(r.GroupId, r.Id)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return g, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return g, aphgrpc.HandleUpdateError(ctx, err)
	}
	return s.getGroup(mg), nil
}

func (s *AnnotationService) CreateAnnotationGroup(
	ctx context.Context, r *annotation.AnnotationIdList,
) (*annotation.TaggedAnnotationGroup, error) {
	g := &annotation.TaggedAnnotationGroup{}
	if err := r.Validate(); err != nil {
		return g, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mg, err := s.repo.AddAnnotationGroup(r.Ids...)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return g, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return g, aphgrpc.HandleInsertError(ctx, err)
	}
	return s.getGroup(mg), nil
}
