package repository

import (
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// FeatureAnnotationRepository is an interface for accessing feature annotation data
// from its data sources.
type FeatureAnnotationRepository interface {
	// GetFeatureAnnotation retrieves a feature annotation by ID
	GetFeatureAnnotation(id string) (*model.FeatureAnnotationDoc, error)
	// GetFeatureAnnotationByName retrieves a feature annotation by name
	GetFeatureAnnotationByName(name string) (*model.FeatureAnnotationDoc, error)
	// AddFeatureAnnotation creates a new feature annotation
	AddFeatureAnnotation(
		doc *feature.NewFeatureAnnotation,
	) (*model.FeatureAnnotationDoc, error)
	// EditFeatureAnnotation updates an existing feature annotation
	EditFeatureAnnotation(
		doc *feature.FeatureAnnotationUpdate,
	) (*model.FeatureAnnotationDoc, error)
	// RemoveFeatureAnnotation deletes a feature annotation
	RemoveFeatureAnnotation(id string, purge bool) error
	// ClearFeatureAnnotations removes all feature annotations
	ClearFeatureAnnotations() error
	// Dbh returns the underlying database handler
	Dbh() *manager.Database
	// Tag management methods
	AddTag(req *feature.AddTagRequest) (*model.FeatureAnnotationDoc, error)
	//nolint:staticcheck // SA1019: Interface methods for deprecated functionality during transition period
	UpdateTag(
		req *feature.UpdateTagRequest,
	) (*model.FeatureAnnotationDoc, error)
	//nolint:staticcheck // SA1019: Interface methods for deprecated functionality during transition period
	RemoveTag(
		req *feature.RemoveTagRequest,
	) error
	// ListByPublicationId retrieves all feature annotations associated with the given publication ID and source
	ListByPublicationId(id string, source string) ([]*model.FeatureAnnotationDoc, error)
}
