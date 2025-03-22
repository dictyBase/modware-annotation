package service

import (
	"context"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (s *AnnotationService) UpdateAnnotation(
	ctx context.Context,
	rta *annotation.TaggedAnnotationUpdate,
) (*annotation.TaggedAnnotation, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tga := &annotation.TaggedAnnotation{}
	mde, err := s.repo.EditAnnotation(rta)
	if err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	if mde.NotFound {
		return nil, aphgrpc.HandleNotFoundError(ctx, err)
	}
	tga.Data = s.getAnnoData(mde)
	err = s.publisher.Publish(s.Topics["annotationUpdate"], tga)
	if err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return tga, nil
}

func (s *AnnotationService) CreateAnnotation(
	ctx context.Context,
	rta *annotation.NewTaggedAnnotation,
) (*annotation.TaggedAnnotation, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tga := &annotation.TaggedAnnotation{}
	m, err := s.repo.AddAnnotation(rta)
	if err != nil {
		return nil, aphgrpc.HandleInsertError(ctx, err)
	}
	tga.Data = s.getAnnoData(m)
	err = s.publisher.Publish(s.Topics["annotationCreate"], tga)
	if err != nil {
		return nil, aphgrpc.HandleInsertError(ctx, err)
	}

	return tga, nil
}

func (s *AnnotationService) AddToAnnotationGroup(
	ctx context.Context, rta *annotation.AnnotationGroupId,
) (*annotation.TaggedAnnotationGroup, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := s.repo.AppendToAnnotationGroup(rta.GroupId, rta.Id)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return s.getGroup(mga), nil
}

func (s *AnnotationService) CreateAnnotationGroup(
	ctx context.Context, rta *annotation.AnnotationIdList,
) (*annotation.TaggedAnnotationGroup, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := s.repo.AddAnnotationGroup(rta.Ids...)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleInsertError(ctx, err)
	}

	return s.getGroup(mga), nil
}
