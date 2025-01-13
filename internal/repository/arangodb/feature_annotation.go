package arangodb

import (
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
)

type featureAnnoRepo struct {
	sess     *manager.Session
	database *manager.Database
	feature  driver.Collection
}

// NewFeatureAnnoRepo is the constructor for creating a new instance of
// FeatureAnnotationRepository.
func NewFeatureAnnoRepo(
	connP *manager.ConnectParams,
	collP *FeatureCollectionParams,
) (repository.FeatureAnnotationRepository, error) {
	// Validate collection parameters
	if err := validator.New().Struct(collP); err != nil {
		return nil, fmt.Errorf("error in validation %s", err)
	}

	// Create new database session
	sess, dbh, err := manager.NewSessionDb(connP)
	if err != nil {
		return nil, fmt.Errorf("error in creating new session %s", err)
	}

	// Create or find feature collection
	featureColl, err := dbh.FindOrCreateCollection(
		collP.Feature,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"error in finding or creating feature collection %s",
			err,
		)
	}
	// Create persistent index on Id field
	_, _, err = dbh.EnsurePersistentIndex(
		featureColl.Name(),
		[]string{"id", "vesion"},
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
			Unique:       true,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error creating index on id field %s", err)
	}

	return &featureAnnoRepo{
		sess:     sess,
		database: dbh,
		feature:  featureColl,
	}, nil
}

// GetFeatureAnnotation retrieves a feature annotation by ID.
func (fr *featureAnnoRepo) GetFeatureAnnotation(
	id string,
) (*model.FeatureAnnotationDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

// AddFeatureAnnotation creates a new feature annotation.
func (fr *featureAnnoRepo) AddFeatureAnnotation(
	doc *feature.NewFeatureAnnotation,
) (*model.FeatureAnnotationDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

// EditFeatureAnnotation updates an existing feature annotation.
func (fr *featureAnnoRepo) EditFeatureAnnotation(
	doc *feature.FeatureAnnotationUpdate,
) (*model.FeatureAnnotationDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

// ListFeatureAnnotations lists all feature annotations.
func (fr *featureAnnoRepo) ListFeatureAnnotations() ([]*model.FeatureAnnotationDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

// RemoveFeatureAnnotation deletes a feature annotation.
func (fr *featureAnnoRepo) RemoveFeatureAnnotation(id string) error {
	return fmt.Errorf("not implemented")
}

// ClearFeatureAnnotations removes all feature annotations.
func (fr *featureAnnoRepo) ClearFeatureAnnotations() error {
	return fmt.Errorf("not implemented")
}

// Dbh returns the underlying database handler.
func (fr *featureAnnoRepo) Dbh() *manager.Database {
	return fr.database
}
