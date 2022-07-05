package service

import (
	"context"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

const limit = 10

func (s *AnnotationService) GetAnnotation(ctx context.Context, r *annotation.AnnotationId) (*annotation.TaggedAnnotation, error) {
	tna := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return tna, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mid, err := s.repo.GetAnnotationByID(r.Id)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return tna, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return tna, aphgrpc.HandleGetError(ctx, err)
	}
	if mid.NotFound {
		return tna, aphgrpc.HandleNotFoundError(ctx, err)
	}
	tna.Data = s.getAnnoData(mid)

	return tna, nil
}

func (s *AnnotationService) GetEntryAnnotation(
	ctx context.Context, rea *annotation.EntryAnnotationRequest,
) (*annotation.TaggedAnnotation, error) {
	tna := &annotation.TaggedAnnotation{}
	if err := rea.Validate(); err != nil {
		return tna, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mne, err := s.repo.GetAnnotationByEntry(rea)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return tna, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return tna, aphgrpc.HandleGetError(ctx, err)
	}
	tna.Data = s.getAnnoData(mne)

	return tna, nil
}

func (s *AnnotationService) GetAnnotationGroup(
	ctx context.Context, rid *annotation.GroupEntryId,
) (*annotation.TaggedAnnotationGroup, error) {
	gta := &annotation.TaggedAnnotationGroup{}
	if err := rid.Validate(); err != nil {
		return gta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := s.repo.GetAnnotationGroup(rid.GroupId)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return gta, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return gta, aphgrpc.HandleGetError(ctx, err)
	}

	return s.getGroup(mga), nil
}

func (s *AnnotationService) ListAnnotationGroups(
	ctx context.Context, rgp *annotation.ListGroupParameters,
) (*annotation.TaggedAnnotationGroupCollection, error) {
	gac := &annotation.TaggedAnnotationGroupCollection{}
	// default value of limit
	limit := int64(limit)
	if rgp.Limit > 0 {
		limit = rgp.Limit
	}
	astmt, err := filterStrToQuery(rgp.Filter)
	if err != nil {
		return gac, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mgc, err := s.repo.ListAnnotationGroup(rgp.Cursor, limit, astmt)
	if err != nil {
		if repository.IsAnnotationGroupListNotFound(err) {
			return gac, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return gac, aphgrpc.HandleGetError(ctx, err)
	}
	gcdata := make([]*annotation.TaggedAnnotationGroupCollection_Data, 0)
	for _, mgs := range mgc {
		var gdata []*annotation.TaggedAnnotationGroup_Data
		for _, m := range mgs.AnnoDocs {
			gdata = append(gdata, s.getAnnoGroupData(m))
		}
		gcdata = append(gcdata, &annotation.TaggedAnnotationGroupCollection_Data{
			Type: s.GetGroupResourceName(),
			Group: &annotation.TaggedAnnotationGroup{
				Data:      gdata,
				GroupId:   mgs.GroupId,
				CreatedAt: aphgrpc.TimestampProto(mgs.CreatedAt),
				UpdatedAt: aphgrpc.TimestampProto(mgs.UpdatedAt),
			},
		})
	}
	if len(gcdata) < int(limit)-2 {
		return &annotation.TaggedAnnotationGroupCollection{
			Data: gcdata,
			Meta: &annotation.Meta{Limit: rgp.Limit},
		}, nil
	}

	return &annotation.TaggedAnnotationGroupCollection{
		Data: gcdata[:len(gcdata)-1],
		Meta: &annotation.Meta{
			Limit:      limit,
			NextCursor: genNextCursorVal(mgc[len(mgc)-1].CreatedAt),
		},
	}, nil
}

func (s *AnnotationService) ListAnnotations(
	ctx context.Context, ral *annotation.ListParameters,
) (*annotation.TaggedAnnotationCollection, error) {
	tac := &annotation.TaggedAnnotationCollection{}
	// default value of limit
	limit := int64(limit)
	if ral.Limit > 0 {
		limit = ral.Limit
	}
	astmt, err := filterStrToQuery(ral.Filter)
	if err != nil {
		return tac, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mlc, err := s.repo.ListAnnotations(ral.Cursor, limit, astmt)
	if err != nil {
		if repository.IsAnnotationListNotFound(err) {
			return tac, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return tac, aphgrpc.HandleGetError(ctx, err)
	}
	tcdata := make([]*annotation.TaggedAnnotationCollection_Data, 0)
	for _, m := range mlc {
		tcdata = append(tcdata, &annotation.TaggedAnnotationCollection_Data{
			Type:       s.GetResourceName(),
			Id:         m.Key,
			Attributes: getAnnoAttributes(m),
		})
	}
	if len(tcdata) < int(limit)-2 { // fewer result than limit
		tac.Data = tcdata
		tac.Meta = &annotation.Meta{Limit: ral.Limit}

		return tac, nil
	}
	tac.Data = tcdata[:len(tcdata)-1]
	tac.Meta = &annotation.Meta{
		Limit:      limit,
		NextCursor: genNextCursorVal(mlc[len(mlc)-1].CreatedAt),
	}

	return tac, nil
}

func (s *AnnotationService) GetAnnotationTag(
	ctx context.Context, rta *annotation.TagRequest,
) (*annotation.AnnotationTag, error) {
	tag := &annotation.AnnotationTag{}
	if err := rta.Validate(); err != nil {
		return tag, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mta, err := s.repo.GetAnnotationTag(rta.Name, rta.Ontology)
	if err != nil {
		if repository.IsAnnoTagNotFound(err) {
			return tag, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return tag, aphgrpc.HandleGetError(ctx, err)
	}
	tag.Id = mta.ID
	tag.Name = mta.Name
	tag.Ontology = mta.Ontology
	tag.IsObsolete = mta.IsObsolete

	return tag, nil
}

func (s *AnnotationService) getGroup(mga *model.AnnoGroup) *annotation.TaggedAnnotationGroup {
	gta := &annotation.TaggedAnnotationGroup{}
	gdata := make([]*annotation.TaggedAnnotationGroup_Data, 0)
	for _, m := range mga.AnnoDocs {
		gdata = append(gdata, s.getAnnoGroupData(m))
	}
	gta.Data = gdata
	gta.GroupId = mga.GroupId
	gta.CreatedAt = aphgrpc.TimestampProto(mga.CreatedAt)
	gta.UpdatedAt = aphgrpc.TimestampProto(mga.UpdatedAt)

	return gta
}

func (s *AnnotationService) getAnnoGroupData(m *model.AnnoDoc) *annotation.TaggedAnnotationGroup_Data {
	return &annotation.TaggedAnnotationGroup_Data{
		Type:       s.GetGroupResourceName(),
		Id:         m.Key,
		Attributes: getAnnoAttributes(m),
	}
}

func (s *AnnotationService) getAnnoData(m *model.AnnoDoc) *annotation.TaggedAnnotation_Data {
	return &annotation.TaggedAnnotation_Data{
		Type:       s.GetGroupResourceName(),
		Id:         m.Key,
		Attributes: getAnnoAttributes(m),
	}
}
