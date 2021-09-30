package service

import (
	"context"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (s *AnnotationService) GetAnnotation(ctx context.Context, r *annotation.AnnotationId) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.GetAnnotationByID(r.Id)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return ta, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return ta, aphgrpc.HandleGetError(ctx, err)
	}
	if m.NotFound {
		return ta, aphgrpc.HandleNotFoundError(ctx, err)
	}
	ta.Data = s.getAnnoData(m)
	return ta, nil
}

func (s *AnnotationService) GetEntryAnnotation(
	ctx context.Context, r *annotation.EntryAnnotationRequest,
) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.GetAnnotationByEntry(r)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return ta, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return ta, aphgrpc.HandleGetError(ctx, err)
	}
	ta.Data = s.getAnnoData(m)
	return ta, nil
}

func (s *AnnotationService) GetAnnotationGroup(
	ctx context.Context, r *annotation.GroupEntryId,
) (*annotation.TaggedAnnotationGroup, error) {
	g := &annotation.TaggedAnnotationGroup{}
	if err := r.Validate(); err != nil {
		return g, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mg, err := s.repo.GetAnnotationGroup(r.GroupId)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return g, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return g, aphgrpc.HandleGetError(ctx, err)
	}
	return s.getGroup(mg), nil
}

func (s *AnnotationService) ListAnnotationGroups(
	ctx context.Context, r *annotation.ListGroupParameters,
) (*annotation.TaggedAnnotationGroupCollection, error) {
	gc := &annotation.TaggedAnnotationGroupCollection{}
	// default value of limit
	limit := int64(10)
	if r.Limit > 0 {
		limit = r.Limit
	}
	astmt, err := filterStrToQuery(r.Filter)
	if err != nil {
		return gc, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mgc, err := s.repo.ListAnnotationGroup(r.Cursor, limit, astmt)
	if err != nil {
		if repository.IsAnnotationGroupListNotFound(err) {
			return gc, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return gc, aphgrpc.HandleGetError(ctx, err)
	}
	var gcdata []*annotation.TaggedAnnotationGroupCollection_Data
	for _, mg := range mgc {
		var gdata []*annotation.TaggedAnnotationGroup_Data
		for _, m := range mg.AnnoDocs {
			gdata = append(gdata, s.getAnnoGroupData(m))
		}
		gcdata = append(gcdata, &annotation.TaggedAnnotationGroupCollection_Data{
			Type: s.GetGroupResourceName(),
			Group: &annotation.TaggedAnnotationGroup{
				Data:      gdata,
				GroupId:   mg.GroupId,
				CreatedAt: aphgrpc.TimestampProto(mg.CreatedAt),
				UpdatedAt: aphgrpc.TimestampProto(mg.UpdatedAt),
			},
		})
	}
	if len(gcdata) < int(limit)-2 {
		return &annotation.TaggedAnnotationGroupCollection{
			Data: gcdata,
			Meta: &annotation.Meta{Limit: r.Limit},
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
	ctx context.Context, r *annotation.ListParameters,
) (*annotation.TaggedAnnotationCollection, error) {
	tac := &annotation.TaggedAnnotationCollection{}
	// default value of limit
	limit := int64(10)
	if r.Limit > 0 {
		limit = r.Limit
	}
	astmt, err := filterStrToQuery(r.Filter)
	if err != nil {
		return tac, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mc, err := s.repo.ListAnnotations(r.Cursor, limit, astmt)
	if err != nil {
		if repository.IsAnnotationListNotFound(err) {
			return tac, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return tac, aphgrpc.HandleGetError(ctx, err)
	}
	var tcdata []*annotation.TaggedAnnotationCollection_Data
	for _, m := range mc {
		tcdata = append(tcdata, &annotation.TaggedAnnotationCollection_Data{
			Type:       s.GetResourceName(),
			Id:         m.Key,
			Attributes: getAnnoAttributes(m),
		})
	}
	if len(tcdata) < int(limit)-2 { // fewer result than limit
		tac.Data = tcdata
		tac.Meta = &annotation.Meta{Limit: r.Limit}
		return tac, nil
	}
	tac.Data = tcdata[:len(tcdata)-1]
	tac.Meta = &annotation.Meta{
		Limit:      limit,
		NextCursor: genNextCursorVal(mc[len(mc)-1].CreatedAt),
	}
	return tac, nil
}

func (s *AnnotationService) GetAnnotationTag(
	ctx context.Context, r *annotation.TagRequest,
) (*annotation.AnnotationTag, error) {
	tag := &annotation.AnnotationTag{}
	if err := r.Validate(); err != nil {
		return tag, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.GetAnnotationTag(r.Name, r.Ontology)
	if err != nil {
		if repository.IsAnnoTagNotFound(err) {
			return tag, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return tag, aphgrpc.HandleGetError(ctx, err)
	}
	tag.Id = m.ID
	tag.Name = m.Name
	tag.Ontology = m.Ontology
	tag.IsObsolete = m.IsObsolete
	return tag, nil
}

func (s *AnnotationService) getGroup(mg *model.AnnoGroup) *annotation.TaggedAnnotationGroup {
	g := &annotation.TaggedAnnotationGroup{}
	var gdata []*annotation.TaggedAnnotationGroup_Data
	for _, m := range mg.AnnoDocs {
		gdata = append(gdata, s.getAnnoGroupData(m))
	}
	g.Data = gdata
	g.GroupId = mg.GroupId
	g.CreatedAt = aphgrpc.TimestampProto(mg.CreatedAt)
	g.UpdatedAt = aphgrpc.TimestampProto(mg.UpdatedAt)
	return g
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
