package arangodb

import (
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
)

type organismRepo struct {
	sess     *manager.Session
	database *manager.Database
	organism driver.Collection
}

// NewOrganismRepo is the constructor for creating a new instance of OrganismRepository
func NewOrganismRepo(
	connP *manager.ConnectParams,
	collP *OrganismCollectionParams,
) (repository.OrganismRepository, error) {
	// Validate collection parameters
	if err := validator.New().Struct(collP); err != nil {
		return nil, fmt.Errorf("error in validation %s", err)
	}

	// Create new database session
	sess, dbh, err := manager.NewSessionDb(connP)
	if err != nil {
		return nil, fmt.Errorf("error in creating new session %s", err)
	}

	// Create or find organism collection
	orgColl, err := dbh.FindOrCreateCollection(
		collP.Organism,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"error in finding or creating organism collection %s",
			err,
		)
	}

	return &organismRepo{
		sess:     sess,
		database: dbh,
		organism: orgColl,
	}, nil
}
