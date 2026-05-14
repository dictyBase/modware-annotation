package service

import (
	"context"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

// LIMIT is the default number of results returned by list operations.
var LIMIT int64 = 10

// GetAnnotation retrieves a tagged annotation by its ID.
func (srv *AnnotationService) GetAnnotation(
	ctx context.Context,
	req *annotation.AnnotationId,
) (*annotation.TaggedAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tna := &annotation.TaggedAnnotation{}
	mid, err := srv.repo.GetAnnotationByID(req.Id)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}
	if mid.NotFound {
		return nil, aphgrpc.HandleNotFoundError(ctx, err)
	}
	tna.Data = srv.getAnnoData(mid)

	return tna, nil
}

// GetEntryAnnotation retrieves a tagged annotation by entry annotation request parameters.
func (srv *AnnotationService) GetEntryAnnotation(
	ctx context.Context, rea *annotation.EntryAnnotationRequest,
) (*annotation.TaggedAnnotation, error) {
	if err := protovalidate.Validate(rea); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tna := &annotation.TaggedAnnotation{}
	mne, err := srv.repo.GetAnnotationByEntry(rea)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}
	tna.Data = srv.getAnnoData(mne)

	return tna, nil
}

// GetAnnotationGroup retrieves a tagged annotation group by its group ID.
func (srv *AnnotationService) GetAnnotationGroup(
	ctx context.Context, rid *annotation.GroupEntryId,
) (*annotation.TaggedAnnotationGroup, error) {
	if err := protovalidate.Validate(rid); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mga, err := srv.repo.GetAnnotationGroup(rid.GroupId)
	if err != nil {
		if repository.IsGroupNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}

	return srv.getGroup(mga), nil
}

// ListAnnotationGroups returns a paginated collection of tagged annotation groups.
func (srv *AnnotationService) ListAnnotationGroups(
	ctx context.Context, rgp *annotation.ListGroupParameters,
) (*annotation.TaggedAnnotationGroupCollection, error) {
	if err := protovalidate.Validate(rgp); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	// default value of limit
	searchLimit := LIMIT
	if rgp.Limit > 0 {
		searchLimit = rgp.Limit
	}
	astmt, err := filterStrToQuery(rgp.Filter)
	if err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	mgc, err := srv.repo.ListAnnotationGroup(rgp.Cursor, searchLimit, astmt)
	if err != nil {
		if repository.IsAnnotationGroupListNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}
	gcdata := make([]*annotation.TaggedAnnotationGroupCollection_Data, 0)
	for _, mgs := range mgc {
		var gdata []*annotation.TaggedAnnotationGroup_Data
		for _, m := range mgs.AnnoDocs {
			gdata = append(gdata, srv.getAnnoGroupData(m))
		}
		gcdata = append(
			gcdata,
			&annotation.TaggedAnnotationGroupCollection_Data{
				Type: srv.GetGroupResourceName(),
				Group: &annotation.TaggedAnnotationGroup{
					Data:      gdata,
					GroupId:   mgs.GroupID,
					CreatedAt: aphgrpc.TimestampProto(mgs.CreatedAt),
					UpdatedAt: aphgrpc.TimestampProto(mgs.UpdatedAt),
				},
			},
		)
	}
	if len(gcdata) < int(searchLimit)-2 {
		return &annotation.TaggedAnnotationGroupCollection{
			Data: gcdata,
			Meta: &annotation.Meta{Limit: rgp.Limit},
		}, nil
	}

	return &annotation.TaggedAnnotationGroupCollection{
		Data: gcdata[:len(gcdata)-1],
		Meta: &annotation.Meta{
			Limit:      searchLimit,
			NextCursor: genNextCursorVal(mgc[len(mgc)-1].CreatedAt),
		},
	}, nil
}

// ListAnnotations returns a paginated collection of tagged annotations.
func (srv *AnnotationService) ListAnnotations(
	ctx context.Context, ral *annotation.ListParameters,
) (*annotation.TaggedAnnotationCollection, error) {
	if err := protovalidate.Validate(ral); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tac := &annotation.TaggedAnnotationCollection{}
	// default value of limit
	searchLimit := LIMIT
	if ral.Limit > 0 {
		searchLimit = ral.Limit
	}
	mlc, err := srv.repo.ListAnnotations(
		&repository.ListAnnotationsParams{
			Cursor: ral.Cursor,
			Limit:  searchLimit,
			Filter: ral.Filter,
		})
	if err != nil {
		if repository.IsAnnotationListNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}
	tcdata := collection.Map(mlc, srv.modelToCollectionData)
	if len(tcdata) < int(searchLimit)-2 { // fewer result than limit
		tac.Data = tcdata
		tac.Meta = &annotation.Meta{Limit: ral.Limit}

		return tac, nil
	}
	tac.Data = tcdata[:len(tcdata)-1]
	tac.Meta = &annotation.Meta{
		Limit:      searchLimit,
		NextCursor: genNextCursorVal(mlc[len(mlc)-1].CreatedAt),
	}

	return tac, nil
}

// GetAnnotationTag retrieves an annotation tag by name and ontology.
func (srv *AnnotationService) GetAnnotationTag(
	ctx context.Context, rta *annotation.TagRequest,
) (*annotation.AnnotationTag, error) {
	if err := protovalidate.Validate(rta); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	tag := &annotation.AnnotationTag{}
	mta, err := srv.repo.GetAnnotationTag(rta.Name, rta.Ontology)
	if err != nil {
		if repository.IsAnnoTagNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}
	tag.Id = mta.ID
	tag.Name = mta.Name
	tag.Ontology = mta.Ontology
	tag.IsObsolete = mta.IsObsolete

	return tag, nil
}

// modelToCollectionData converts an AnnoDoc model to TaggedAnnotationCollection_Data.
func (srv *AnnotationService) modelToCollectionData(
	m *model.AnnoDoc,
) *annotation.TaggedAnnotationCollection_Data {
	return &annotation.TaggedAnnotationCollection_Data{
		Type:       srv.GetResourceName(),
		Id:         m.Key,
		Attributes: getAnnoAttributes(m),
	}
}

func (srv *AnnotationService) getGroup(
	mga *model.AnnoGroup,
) *annotation.TaggedAnnotationGroup {
	gta := &annotation.TaggedAnnotationGroup{}
	gta.Data = srv.getGroupData(mga)
	gta.GroupId = mga.GroupID
	gta.CreatedAt = aphgrpc.TimestampProto(mga.CreatedAt)
	gta.UpdatedAt = aphgrpc.TimestampProto(mga.UpdatedAt)

	return gta
}

func (srv *AnnotationService) getAnnoGroupData(
	m *model.AnnoDoc,
) *annotation.TaggedAnnotationGroup_Data {
	return &annotation.TaggedAnnotationGroup_Data{
		Type:       srv.GetGroupResourceName(),
		Id:         m.Key,
		Attributes: getAnnoAttributes(m),
	}
}

func (srv *AnnotationService) getAnnoData(
	m *model.AnnoDoc,
) *annotation.TaggedAnnotation_Data {
	return &annotation.TaggedAnnotation_Data{
		Type:       srv.GetGroupResourceName(),
		Id:         m.Key,
		Attributes: getAnnoAttributes(m),
	}
}

func (srv *AnnotationService) getGroupData(
	mga *model.AnnoGroup,
) []*annotation.TaggedAnnotationGroup_Data {
	gdata := make([]*annotation.TaggedAnnotationGroup_Data, 0)
	for _, m := range mga.AnnoDocs {
		gdata = append(gdata, srv.getAnnoGroupData(m))
	}

	return gdata
}
