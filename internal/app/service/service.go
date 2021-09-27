package service

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-playground/validator/v10"
	"golang.org/x/sync/errgroup"

	"github.com/dictyBase/arangomanager/query"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"

	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-genproto/dictybaseapis/api/upload"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/repository"
	empty "google.golang.org/protobuf/types/known/emptypb"
)

type oboStreamHandler struct {
	writer *io.PipeWriter
	stream annotation.TaggedAnnotationService_OboJsonFileUploadServer
}

func (oh *oboStreamHandler) Write() error {
	defer oh.writer.Close()
	for {
		req, err := oh.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		oh.writer.Write(req.Content)
	}
	return nil
}

// AnnotationService is the container for managing annotation service
// definition
type AnnotationService struct {
	*aphgrpc.Service
	repo      repository.TaggedAnnotationRepository
	publisher message.Publisher
	group     string
}

// ServiceParams are the attributes that are required for creating new AnnotationService
type ServiceParams struct {
	Repository repository.TaggedAnnotationRepository `validate:"required"`
	Publisher  message.Publisher                     `validate:"required"`
	Options    []aphgrpc.Option                      `validate:"required"`
	Group      string                                `validate:"required"`
}

func defaultOptions() *aphgrpc.ServiceOptions {
	return &aphgrpc.ServiceOptions{Resource: "annotations"}
}

// NewAnnotationService is the constructor for creating a new instance of AnnotationService
func NewAnnotationService(srvP *ServiceParams) (*AnnotationService, error) {
	if err := validator.New().Struct(srvP); err != nil {
		return &AnnotationService{}, err
	}
	so := defaultOptions()
	for _, optfn := range srvP.Options {
		optfn(so)
	}
	srv := &aphgrpc.Service{}
	aphgrpc.AssignFieldsToStructs(so, srv)
	return &AnnotationService{
		Service:   srv,
		repo:      srvP.Repository,
		publisher: srvP.Publisher,
		group:     srvP.Group,
	}, nil
}

func (s *AnnotationService) GetGroupResourceName() string {
	return s.group
}

func (s *AnnotationService) GetAnnotation(ctx context.Context, r *annotation.AnnotationId) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.GetAnnotationById(r.Id)
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

func (s *AnnotationService) GetEntryAnnotation(ctx context.Context, r *annotation.EntryAnnotationRequest) (*annotation.TaggedAnnotation, error) {
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

func (s *AnnotationService) GetAnnotationGroup(ctx context.Context, r *annotation.GroupEntryId) (*annotation.TaggedAnnotationGroup, error) {
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

func (s *AnnotationService) CreateAnnotationGroup(ctx context.Context, r *annotation.AnnotationIdList) (*annotation.TaggedAnnotationGroup, error) {
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

func (s *AnnotationService) AddToAnnotationGroup(ctx context.Context, r *annotation.AnnotationGroupId) (*annotation.TaggedAnnotationGroup, error) {
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

func (s *AnnotationService) ListAnnotationGroups(ctx context.Context, r *annotation.ListGroupParameters) (*annotation.TaggedAnnotationGroupCollection, error) {
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
	if len(gcdata) < int(limit)-2 { //fewer result than limit
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

func (s *AnnotationService) ListAnnotations(ctx context.Context, r *annotation.ListParameters) (*annotation.TaggedAnnotationCollection, error) {
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

func (s *AnnotationService) CreateAnnotation(ctx context.Context, r *annotation.NewTaggedAnnotation) (*annotation.TaggedAnnotation, error) {
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

func (s *AnnotationService) UpdateAnnotation(ctx context.Context, r *annotation.TaggedAnnotationUpdate) (*annotation.TaggedAnnotation, error) {
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

func (s *AnnotationService) GetAnnotationTag(ctx context.Context, r *annotation.TagRequest) (*annotation.AnnotationTag, error) {
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

func (s *AnnotationService) OboJsonFileUpload(stream annotation.TaggedAnnotationService_OboJsonFileUploadServer) error {
	in, out := io.Pipe()
	grp := new(errgroup.Group)
	defer in.Close()
	oh := &oboStreamHandler{writer: out, stream: stream}
	grp.Go(oh.Write)
	m, err := s.repo.LoadOboJson(in)
	if err != nil {
		return aphgrpc.HandleGenericError(context.Background(), err)
	}
	if err := grp.Wait(); err != nil {
		return aphgrpc.HandleGenericError(context.Background(), err)
	}
	return stream.SendAndClose(&upload.FileUploadResponse{
		Status: uploadResponse(m),
		Msg:    "obojson file is uploaded",
	})
}

func uploadResponse(m model.UploadStatus) upload.FileUploadResponse_Status {
	if m == model.Created {
		return upload.FileUploadResponse_CREATED
	}
	return upload.FileUploadResponse_UPDATED
}

// genNextCursorVal converts to epoch(https://en.wikipedia.org/wiki/Unix_time)
// in milliseconds
func genNextCursorVal(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func getAnnoAttributes(m *model.AnnoDoc) *annotation.TaggedAnnotationAttributes {
	return &annotation.TaggedAnnotationAttributes{
		Value:         m.Value,
		EditableValue: m.EditableValue,
		CreatedBy:     m.CreatedBy,
		CreatedAt:     aphgrpc.TimestampProto(m.CreatedAt),
		Version:       m.Version,
		EntryId:       m.EnrtyId,
		Rank:          m.Rank,
		IsObsolete:    m.IsObsolete,
		Tag:           m.Tag,
		Ontology:      m.Ontology,
	}
}

func filterStrToQuery(filter string) (string, error) {
	var empty string
	if len(filter) == 0 {
		return empty, nil
	}
	p, err := query.ParseFilterString(filter)
	if err != nil {
		return empty, fmt.Errorf("error in parsing filter string")
	}
	q, err := query.GenQualifiedAQLFilterStatement(arangodb.FilterMap(), p)
	if err != nil {
		return empty, fmt.Errorf("error in generating aql statement")
	}
	return q, nil
}
