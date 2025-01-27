package service

import (
	"context"
	"fmt"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/aphgrpc"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FeatureAnnotationService struct {
	*aphgrpc.Service
	repo      repository.FeatureAnnotationRepository
	publisher message.FeatureAnnotationPublisher
	feature.UnimplementedFeatureAnnotationServiceServer
}

type FeatureParams struct {
	Repository repository.FeatureAnnotationRepository `validate:"required"`
	Publisher  message.FeatureAnnotationPublisher     `validate:"required"`
	Options    []aphgrpc.Option                       `validate:"required"`
}

func featureAnnoDefaultOptions() *aphgrpc.ServiceOptions {
	return &aphgrpc.ServiceOptions{
		Resource: "feature_annotations",
		Topics: map[string]string{
			"featureAnnotationCreate": "FeatureAnnotationCreated",
			"featureAnnotationUpdate": "FeatureAnnotationUpdated",
		},
	}
}

func NewFeatureAnnotationService(
	params *FeatureParams,
) (*FeatureAnnotationService, error) {
	if err := validator.New().Struct(params); err != nil {
		return &FeatureAnnotationService{}, fmt.Errorf(
			"error in validating params %s",
			err,
		)
	}
	svcOpt := featureAnnoDefaultOptions()
	for _, optfn := range params.Options {
		optfn(svcOpt)
	}
	srv := &aphgrpc.Service{}
	aphgrpc.AssignFieldsToStructs(svcOpt, srv)

	return &FeatureAnnotationService{
		Service:   srv,
		repo:      params.Repository,
		publisher: params.Publisher,
	}, nil
}

func (srv *FeatureAnnotationService) GetFeatureAnnotation(
	ctx context.Context,
	req *feature.FeatureAnnotationId,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.GetFeatureAnnotation(req.Id)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleGetError(ctx, err)
	}

	return convertToProto(feat), nil
}

func (srv *FeatureAnnotationService) CreateFeatureAnnotation(
	ctx context.Context,
	req *feature.NewFeatureAnnotation,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.AddFeatureAnnotation(req)
	if err != nil {
		return &feature.FeatureAnnotation{}, aphgrpc.HandleInsertError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(srv.Topics["featureAnnotationCreate"], featProto); err != nil {
		return featProto, aphgrpc.HandleInsertError(ctx, err)
	}

	return featProto, nil
}

func (srv *FeatureAnnotationService) UpdateFeatureAnnotation(
	ctx context.Context,
	req *feature.FeatureAnnotationUpdate,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.EditFeatureAnnotation(req)
	if err != nil {
		return &feature.FeatureAnnotation{}, aphgrpc.HandleUpdateError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(srv.Topics["featureAnnotationUpdate"], featProto); err != nil {
		return featProto, aphgrpc.HandleUpdateError(ctx, err)
	}

	return featProto, nil
}

func (srv *FeatureAnnotationService) DeleteFeatureAnnotation(
	ctx context.Context,
	req *feature.DeleteFeatureAnnotationRequest,
) (*emptypb.Empty, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	err := srv.repo.RemoveFeatureAnnotation(req.Id, req.Purge)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}

		return nil, aphgrpc.HandleDeleteError(ctx, err)
	}

	return &emptypb.Empty{}, nil
}

func convertToProto(
	feat *model.FeatureAnnotationDoc,
) *feature.FeatureAnnotation {
	attrs := &feature.FeatureAnnotationAttributes{Name: feat.Name}

	// Handle optional attributes using functional constructs
	attrs.Synonyms = feat.Synonyms
	attrs.Publications = feat.Publications
	attrs.Pubmed = feat.Pubmed
	attrs.Dblinks = collection.Map(feat.DbLinks, convertDbLink)
	attrs.Properties = collection.Map(feat.Properties, convertProperty)

	return &feature.FeatureAnnotation{
		Type:       "feature_annotations",
		Id:         feat.AnnoId,
		CreatedBy:  feat.CreatedBy,
		UpdatedBy:  feat.UpdatedBy,
		CreatedAt:  timestamppb.New(feat.CreatedAt),
		UpdatedAt:  timestamppb.New(feat.UpdatedAt),
		IsObsolete: feat.IsObsolete,
		Attributes: attrs,
	}
}

func convertDbLink(link model.DbLinkDoc) *feature.DbLink {
	return &feature.DbLink{
		Database:  link.Database,
		PrimaryId: link.PrimaryId,
		Version:   link.Version,
		Linktype:  link.LinkType,
		Url:       link.URL,
		Label:     link.Label,
	}
}

func convertProperty(prop model.TagPropertyDoc) *feature.TagProperty {
	return &feature.TagProperty{
		Tag:   prop.Tag,
		Value: prop.Value,
	}
}
