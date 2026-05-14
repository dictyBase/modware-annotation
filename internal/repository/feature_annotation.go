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
	// AddTag adds a single tag to an existing feature annotation
	AddTag(req *feature.AddTagRequest) (*model.FeatureAnnotationDoc, error)
	// AddTags adds multiple tags to an existing feature annotation
	AddTags(req *feature.AddTagsRequest) (*model.FeatureAnnotationDoc, error)
	// SetTags replaces all tags for a feature annotation with the provided set
	SetTags(req *feature.SetTagsRequest) (*model.FeatureAnnotationDoc, error)
	// RemoveTags removes tags from a feature annotation by tag and value
	RemoveTags(req *feature.RemoveTagsRequest) (*model.FeatureAnnotationDoc, error)

	// Publication-based queries
	// ListByPublicationID retrieves all feature annotations associated with the given publication ID and source
	ListByPublicationID(id string, source string) ([]*model.FeatureAnnotationDoc, error)

	// Embed deprecated interface for backward compatibility during transition period
	DeprecatedFeatureAnnotationRepository
}
