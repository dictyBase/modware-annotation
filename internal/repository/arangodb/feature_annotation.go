package arangodb

import (
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
)

type featureAnnoRepo struct {
	sess     *manager.Session
	database *manager.Database
	feature  driver.Collection
}

// NewFeatureAnnoRepo is the constructor for creating a new instance of
// FeatureAnnotationRepository
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

	return &featureAnnoRepo{
		sess:     sess,
		database: dbh,
		feature:  featureColl,
	}, nil
}
