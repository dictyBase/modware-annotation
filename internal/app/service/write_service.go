package service

import (
	"context"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (s *AnnotationService) UpdateAnnotation(
	ctx context.Context,
	rta *annotation.TaggedAnnotationUpdate,
) (*annotation.TaggedAnnotation, error) {
	tga := &annotation.TaggedAnnotation{}
	if err := rta.Validate(); err != nil {
		return tga, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mde, err := s.repo.EditAnnotation(rta)
	if err != nil {
		return tga, aphgrpc.HandleUpdateError(ctx, err)
	}
	if mde.NotFound {
		return tga, aphgrpc.HandleNotFoundError(ctx, err)
	}
	tga.Data = s.getAnnoData(mde)
	err = s.publisher.Publish(s.Topics["annotationUpdate"], tga)
	if err != nil {
		return tga, aphgrpc.HandleUpdateError(ctx, err)
	}
	return tga, nil
}

func (s *AnnotationService) CreateAnnotation(
	ctx context.Context,
	rta *annotation.NewTaggedAnnotation,
) (*annotation.TaggedAnnotation, error) {
	tga := &annotation.TaggedAnnotation{}
	if err := rta.Validate(); err != nil {
		return tga, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.AddAnnotation(rta)
	if err != nil {
		return tga, aphgrpc.HandleInsertError(ctx, err)
	}
	tga.Data = s.getAnnoData(m)
	err = s.publisher.Publish(s.Topics["annotationCreate"], tga)
	if err != nil {
		return tga, aphgrpc.HandleInsertError(ctx, err)
	}
	return tga, nil
}

func (s *AnnotationService) AddToAnnotationGroup(
	ctx context.Context, rta *annotation.AnnotationGroupId,
) (*annotation.TaggedAnnotationGroup, error) {
	gta := &annotation.TaggedAnnotationGroup{}
	if err := rta.Validate(); err != nil {
		return gta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := s.repo.AppendToAnnotationGroup(rta.GroupId, rta.Id)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return gta, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return gta, aphgrpc.HandleUpdateError(ctx, err)
	}
	return s.getGroup(mga), nil
}

func (s *AnnotationService) CreateAnnotationGroup(
	ctx context.Context, rta *annotation.AnnotationIdList,
) (*annotation.TaggedAnnotationGroup, error) {
	gta := &annotation.TaggedAnnotationGroup{}
	if err := rta.Validate(); err != nil {
		return gta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := s.repo.AddAnnotationGroup(rta.Ids...)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return gta, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return gta, aphgrpc.HandleInsertError(ctx, err)
	}
	return s.getGroup(mga), nil
}
