package service

import (
	"context"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

// UpdateAnnotation updates an existing tagged annotation.
func (srv *AnnotationService) UpdateAnnotation(
	ctx context.Context,
	rta *annotation.TaggedAnnotationUpdate,
) (*annotation.TaggedAnnotation, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tga := &annotation.TaggedAnnotation{}
	mde, err := srv.repo.EditAnnotation(rta)
	if err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	if mde.NotFound {
		return nil, aphgrpc.HandleNotFoundError(ctx, err)
	}
	tga.Data = srv.getAnnoData(mde)
	err = srv.publisher.Publish(srv.Topics["annotationUpdate"], tga)
	if err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return tga, nil
}

// CreateAnnotation creates a new tagged annotation.
func (srv *AnnotationService) CreateAnnotation(
	ctx context.Context,
	rta *annotation.NewTaggedAnnotation,
) (*annotation.TaggedAnnotation, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tga := &annotation.TaggedAnnotation{}
	m, err := srv.repo.AddAnnotation(rta)
	if err != nil {
		return nil, aphgrpc.HandleInsertError(ctx, err)
	}
	tga.Data = srv.getAnnoData(m)
	err = srv.publisher.Publish(srv.Topics["annotationCreate"], tga)
	if err != nil {
		return nil, aphgrpc.HandleInsertError(ctx, err)
	}

	return tga, nil
}

// AddToAnnotationGroup appends an annotation to an existing annotation group.
func (srv *AnnotationService) AddToAnnotationGroup(
	ctx context.Context, rta *annotation.AnnotationGroupId,
) (*annotation.TaggedAnnotationGroup, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := srv.repo.AppendToAnnotationGroup(rta.GroupId, rta.Id)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return srv.getGroup(mga), nil
}

// CreateAnnotationGroup creates a new annotation group from a list of annotation IDs.
func (srv *AnnotationService) CreateAnnotationGroup(
	ctx context.Context, rta *annotation.AnnotationIdList,
) (*annotation.TaggedAnnotationGroup, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := srv.repo.AddAnnotationGroup(rta.Ids...)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleInsertError(ctx, err)
	}

	return srv.getGroup(mga), nil
}
