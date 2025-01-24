package service

import (
	"context"
	"fmt"

	"github.com/dictyBase/aphgrpc"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
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
	if req == nil {
		return &feature.FeatureAnnotation{}, aphgrpc.HandleInvalidParamError(
			ctx,
			fmt.Errorf("feature ID is required"),
		)
	}
	feat, err := srv.repo.GetFeatureAnnotation(req.Id)
	if err != nil {
		return &feature.FeatureAnnotation{}, aphgrpc.HandleGetError(ctx, err)
	}

	return convertToProto(feat), nil
}

func (srv *FeatureAnnotationService) CreateFeatureAnnotation(
	ctx context.Context,
	req *feature.NewFeatureAnnotation,
) (*feature.FeatureAnnotation, error) {
	if err := req.Validate(); err != nil {
		return &feature.FeatureAnnotation{}, aphgrpc.HandleInvalidParamError(
			ctx,
			err,
		)
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
	if err := req.Validate(); err != nil {
		return &feature.FeatureAnnotation{}, aphgrpc.HandleInvalidParamError(
			ctx,
			err,
		)
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

func convertToProto(
	feat *model.FeatureAnnotationDoc,
) *feature.FeatureAnnotation {
	return &feature.FeatureAnnotation{
		Type: "feature_annotations",
		Id:   feat.Id,
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:       feat.Name,
			Synonyms:   feat.Synonyms,
			Dblinks:    convertDbLinks(feat.DbLinks),
			Properties: convertProperties(feat.Properties),
		},
	}
}

func convertDbLinks(links []model.DbLinkDoc) []*feature.DbLink {
	dblinks := make([]*feature.DbLink, 0)
	for _, link := range links {
		dblink := &feature.DbLink{
			Database:  link.Database,
			PrimaryId: link.PrimaryId,
			Version:   link.Version,
		}
		// Only include optional fields if they have values
		if link.LinkType != "" {
			dblink.Linktype = link.LinkType
		}
		if link.URL != "" {
			dblink.Url = link.URL
		}
		if link.Label != "" {
			dblink.Label = link.Label
		}
		dblinks = append(dblinks, dblink)
	}

	return dblinks
}

func convertProperties(props []model.TagPropertyDoc) []*feature.TagProperty {
	properties := make([]*feature.TagProperty, 0)
	for _, prop := range props {
		properties = append(properties, &feature.TagProperty{
			Tag:   prop.Tag,
			Value: prop.Value,
		})
	}

	return properties
}
