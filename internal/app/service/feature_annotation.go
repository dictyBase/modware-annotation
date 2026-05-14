package service

import (
	"context"
	"fmt"
	"strings"

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

// FeatureAnnotationService implements the gRPC FeatureAnnotationService server.
type FeatureAnnotationService struct {
	*aphgrpc.Service
	repo      repository.FeatureAnnotationRepository
	publisher message.FeatureAnnotationPublisher
	feature.UnimplementedFeatureAnnotationServiceServer
}

// FeatureParams holds the required parameters for creating a FeatureAnnotationService.
type FeatureParams struct {
	Repository repository.FeatureAnnotationRepository `validate:"required"`
	Publisher  message.FeatureAnnotationPublisher     `validate:"required"`
	Options    []aphgrpc.Option
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

// NewFeatureAnnotationService creates a new FeatureAnnotationService from the given parameters.
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

// GetFeatureAnnotation retrieves a feature annotation by its ID.
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

// GetFeatureAnnotationByName retrieves a feature annotation by its name.
func (srv *FeatureAnnotationService) GetFeatureAnnotationByName(
	ctx context.Context,
	req *feature.FeatureName,
) (*feature.FeatureAnnotation, error) {
	// Validate the request using protovalidate (assuming FeatureName has rules)
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}

	feat, err := srv.repo.GetFeatureAnnotationByName(req.Name)
	if err != nil {
		// Check for both ID not found and Name not found errors
		if repository.IsAnnotationNotFound(err) ||
			repository.IsFeatureNameNotFound(err) {
			// Pass the original error for context
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleGetError(ctx, err)
	}

	return convertToProto(feat), nil
}

// CreateFeatureAnnotation creates a new feature annotation from the provided request.
func (srv *FeatureAnnotationService) CreateFeatureAnnotation(
	ctx context.Context,
	req *feature.NewFeatureAnnotation,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.AddFeatureAnnotation(req)
	if err != nil {
		if strings.Contains(err.Error(), "unique constraint violated") {
			return nil, aphgrpc.HandleExistError(ctx, err)
		}

		return nil, aphgrpc.HandleInsertError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(srv.Topics["featureAnnotationCreate"], featProto); err != nil {
		return featProto, aphgrpc.HandleInsertError(ctx, err)
	}

	return featProto, nil
}

// UpdateFeatureAnnotation updates an existing feature annotation.
func (srv *FeatureAnnotationService) UpdateFeatureAnnotation(
	ctx context.Context,
	req *feature.FeatureAnnotationUpdate,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.EditFeatureAnnotation(req)
	if err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(
		srv.Topics["featureAnnotationUpdate"], featProto,
	); err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return featProto, nil
}

// DeleteFeatureAnnotation removes a feature annotation by its ID.
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

// AddTag adds a single tag to the specified feature annotation.
func (srv *FeatureAnnotationService) AddTag(
	ctx context.Context,
	req *feature.AddTagRequest,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.AddTag(req)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(
		srv.Topics["featureAnnotationUpdate"],
		featProto,
	); err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return featProto, nil
}

// AddTags adds multiple tags to the specified feature annotation.
func (srv *FeatureAnnotationService) AddTags(
	ctx context.Context,
	req *feature.AddTagsRequest,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.AddTags(req)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(
		srv.Topics["featureAnnotationUpdate"],
		featProto,
	); err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return featProto, nil
}

// SetTags replaces all tags on the specified feature annotation.
func (srv *FeatureAnnotationService) SetTags(
	ctx context.Context,
	req *feature.SetTagsRequest,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.SetTags(req)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(
		srv.Topics["featureAnnotationUpdate"],
		featProto,
	); err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return featProto, nil
}

// RemoveTags removes specified tags from a feature annotation.
func (srv *FeatureAnnotationService) RemoveTags(
	ctx context.Context,
	req *feature.RemoveTagsRequest,
) (*feature.FeatureAnnotation, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feat, err := srv.repo.RemoveTags(req)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}
	featProto := convertToProto(feat)
	if err := srv.publisher.Publish(
		srv.Topics["featureAnnotationUpdate"],
		featProto,
	); err != nil {
		return nil, aphgrpc.HandleUpdateError(ctx, err)
	}

	return featProto, nil
}

// ListFeatureAnnotationsByPubmedId lists all feature annotations associated with a PubMed ID.
//
//nolint:staticcheck // must match gRPC-generated interface name
func (srv *FeatureAnnotationService) ListFeatureAnnotationsByPubmedId(
	ctx context.Context,
	req *feature.PubmedId,
) (*feature.FeatureAnnotationCollection, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	// Assuming "pubmed" is the correct source identifier for PubMed IDs in the repository
	feats, err := srv.repo.ListByPublicationID(req.Id, "pubmed")
	if err != nil {
		if repository.IsPublicationAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleGetError(ctx, err)
	}

	return &feature.FeatureAnnotationCollection{
		Data: collection.Map(feats, convertToProto),
	}, nil
}

// ListFeatureAnnotationsByDOI lists all feature annotations associated with a DOI.
func (srv *FeatureAnnotationService) ListFeatureAnnotationsByDOI(
	ctx context.Context,
	req *feature.DOI,
) (*feature.FeatureAnnotationCollection, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, aphgrpc.HandleInvalidParamError(ctx, err)
	}
	feats, err := srv.repo.ListByPublicationID(req.Id, "doi")
	if err != nil {
		if repository.IsPublicationAnnotationNotFound(err) {
			return nil, aphgrpc.HandleNotFoundError(ctx, err)
		}
		return nil, aphgrpc.HandleGetError(ctx, err)
	}

	return &feature.FeatureAnnotationCollection{
		Data: collection.Map(feats, convertToProto),
	}, nil
}

func convertToProto(
	feat *model.FeatureAnnotationDoc,
) *feature.FeatureAnnotation {
	attrs := &feature.FeatureAnnotationAttributes{Name: feat.Name}

	// Handle optional attributes using functional constructs
	attrs.Synonyms = feat.Synonyms
	attrs.Publications = feat.Publications
	attrs.Pubmed = feat.Pubmed
	attrs.Dblinks = collection.Map(feat.DBLinks, convertDBLink)
	attrs.Properties = collection.Map(feat.Properties, convertProperty)

	return &feature.FeatureAnnotation{
		Type:       "feature_annotations",
		Id:         feat.AnnoID,
		CreatedBy:  feat.CreatedBy,
		UpdatedBy:  feat.UpdatedBy,
		CreatedAt:  timestamppb.New(feat.CreatedAt),
		UpdatedAt:  timestamppb.New(feat.UpdatedAt),
		IsObsolete: feat.IsObsolete,
		Attributes: attrs,
	}
}

func convertDBLink(link model.DBLinkDoc) *feature.DbLink {
	return &feature.DbLink{
		Database:  link.Database,
		PrimaryId: link.PrimaryID,
		Version:   link.Version,
		Linktype:  link.LinkType,
		Url:       link.URL,
		Label:     link.Label,
	}
}

func convertProperty(prop model.TagPropertyDoc) *feature.TagProperty {
	return &feature.TagProperty{
		Tag:       prop.Tag,
		Value:     prop.Value,
		CreatedBy: prop.CreatedBy,
		UpdatedBy: prop.UpdatedBy,
		CreatedAt: timestamppb.New(prop.CreatedAt),
		UpdatedAt: timestamppb.New(prop.UpdatedAt),
	}
}
