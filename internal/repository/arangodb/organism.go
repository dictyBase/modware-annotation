package arangodb

import (
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
)

type organismRepo struct {
	sess     *manager.Session
	database *manager.Database
	organism driver.Collection
}

// NewOrganismRepo is the constructor for creating a new instance of
// OrganismRepository.
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

	// Create or find organism collection with schema validation
	schemaOpt := &driver.CollectionSchemaOptions{
		Level:   driver.CollectionSchemaLevelStrict,
		Message: "organism schema validation failed",
		Type:    "json",
	}
	if err := schemaOpt.LoadRule(model.Schema()); err != nil {
		return nil, fmt.Errorf("error in loading schema %s", err)
	}
	orgColl, err := dbh.FindOrCreateCollection(
		collP.Organism,
		&driver.CreateCollectionOptions{
			Schema: schemaOpt,
		},
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

func (org *organismRepo) GetOrganism(id string) (*model.OrganismDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

func (org *organismRepo) GetOrganismByName(
	genus, species string,
) (*model.OrganismDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

func (org *organismRepo) AddOrganism(
	doc *model.OrganismDoc,
) (*model.OrganismDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

func (org *organismRepo) EditOrganism(
	doc *model.OrganismDoc,
) (*model.OrganismDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

func (org *organismRepo) RemoveOrganism(id string) error {
	return fmt.Errorf("not implemented")
}

func (org *organismRepo) ListOrganisms(
	cursor int64,
	limit int64,
	filter string,
) ([]*model.OrganismDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

func (org *organismRepo) ClearOrganisms() error {
	return nil
}

func (org *organismRepo) Dbh() *manager.Database {
	return org.database
}
