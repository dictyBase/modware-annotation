package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/dictyBase/apihelpers/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

// AnnotationService is the container for managing annotation service
// definition
type AnnotationService struct {
	*aphgrpc.Service
	repo      repository.TaggedAnnotationRepository
	publisher message.Publisher
}

func defaultOptions() *aphgrpc.ServiceOptions {
	return &aphgrpc.ServiceOptions{Resource: "annotations"}
}

// NewAnnotationService is the constructor for creating a new instance of AnnotationService
func NewAnnotationService(repo repository.TaggedAnnotationRepository, pub message.Publisher, opt ...aphgrpc.Option) *AnnotationService {
	so := defaultOptions()
	for _, optfn := range opt {
		optfn(so)
	}
	srv := &aphgrpc.Service{}
	aphgrpc.AssignFieldsToStructs(so, srv)
	return &AnnotationService{
		Service:   srv,
		repo:      repo,
		publisher: pub,
	}
}

func (s *AnnotationService) GetAnnotation(ctx context.Context, r *annotation.AnnotationId) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.GetAnnotationById(r.Id)
	if err != nil {
		return ta, aphgrpc.HandleGetError(ctx, err)
	}
	if m.NotFound {
		return ta, aphgrpc.HandleNotFoundError(ctx, err)
	}
	ta.Data = &annotation.TaggedAnnotation_Data{
		Type: s.GetResourceName(),
		Id:   m.Key,
		Attributes: &annotation.TaggedAnnotationAttributes{
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
		},
	}
	return ta, nil
}

func (s *AnnotationService) GetEntryAnnotation(ctx context.Context, r *annotation.EntryAnnotationRequest) (*annotation.TaggedAnnotation, error) {
	ta := &annotation.TaggedAnnotation{}
	if err := r.Validate(); err != nil {
		return ta, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	m, err := s.repo.GetAnnotationByEntry(r)
	if err != nil {
		return ta, aphgrpc.HandleGetError(ctx, err)
	}
	if m.NotFound {
		return ta, aphgrpc.HandleNotFoundError(ctx, err)
	}
	ta.Data = &annotation.TaggedAnnotation_Data{
		Type: s.GetResourceName(),
		Id:   m.Key,
		Attributes: &annotation.TaggedAnnotationAttributes{
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
		},
	}
	return ta, nil
}

func (s *AnnotationService) ListAnnotations(ctx context.Context, r *annotation.ListParameters) (*annotation.TaggedAnnotationCollection, error) {
	tac := &annotation.TaggedAnnotationCollection{}
	if len(r.Filter) == 0 { // no filter parameters
		mc, err := s.repo.ListAnnotations(r.Cursor, r.Limit)
		if err != nil {
			return tac, aphgrpc.HandleGetError(ctx, err)
		}
		if len(mc) == 0 {
			return tac, aphgrpc.HandleNotFoundError(ctx, err)
		}
		var tcdata []*annotation.TaggedAnnotationCollection_Data
		for _, m := range mc {
			tcdata = append(tcdata, &annotation.TaggedAnnotationCollection_Data{
				Type: s.GetResourceName(),
				Id:   m.Key,
				Attributes: &annotation.TaggedAnnotationAttributes{
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
				},
			})
		}
		if len(tcdata) < int(r.Limit)-2 { // fewer result than limit
			tac.Data = tcdata
			tac.Meta = &annotation.Meta{Limit: r.Limit}
			return tac, nil
		}
		tac.Data = tcdata[:len(tcdata)-1]
		tac.Meta = &annotation.Meta{
			Limit:      r.Limit,
			NextCursor: genNextCursorVal(tcdata[len(tcdata)-1]),
		}
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
	if m.NotFound {
		return ta, aphgrpc.HandleNotFoundError(ctx, err)
	}
	ta.Data = &annotation.TaggedAnnotation_Data{
		Type: s.GetResourceName(),
		Id:   m.Key,
		Attributes: &annotation.TaggedAnnotationAttributes{
			Value:         m.Value,
			EditableValue: m.EditableValue,
			CreatedBy:     m.CreatedBy,
			CreatedAt:     aphgrpc.TimestampProto(m.CreatedAt),
			Version:       m.Version,
			EntryId:       m.EnrtyId,
			Rank:          m.Rank,
			IsObsolete:    m.IsObsolete,
			Tag:           r.Data.Attributes.Tag,
			Ontology:      r.Data.Attributes.Ontology,
		},
	}
	s.publisher.Publish(s.Topics["annotationCreate"], ta)
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
	ta.Data = &annotation.TaggedAnnotation_Data{
		Type: s.GetResourceName(),
		Id:   m.Key,
		Attributes: &annotation.TaggedAnnotationAttributes{
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
		},
	}
	s.publisher.Publish(s.Topics["annotationUpdate"], ta)
	return ta, nil
}

func (s *AnnotationService) DeleteAnnotation(ctx context.Context, r *annotation.AnnotationId) (*empty.Empty, error) {
	e := &empty.Empty{}
	if err := r.Validate(); err != nil {
		return e, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	if err := s.repo.RemoveAnnotation(r.Id); err != nil {
		return e, aphgrpc.HandleDeleteError(ctx, err)
	}
	return e, nil
}

func genNextCursorVal(tcd *annotation.TaggedAnnotationCollection_Data) int64 {
	tint, _ := strconv.ParseInt(
		fmt.Sprintf("%d%d", tcd.Attributes.CreatedAt.GetSeconds(), tcd.Attributes.CreatedAt.GetNanos()),
		10,
		64,
	)
	return tint / 1000000
}
