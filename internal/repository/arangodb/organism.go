package arangodb

import (
	"context"
	"fmt"
	"time"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	dorg "github.com/dictyBase/go-genproto/dictybaseapis/organism"
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

// GetOrganism retrieves an organism by its unique identifier. It returns an
// OrganismDoc containing the organism's details or an error. If the organism is
// not found, it returns an OrganismNotFoundError.
func (org *organismRepo) GetOrganism(id string) (*model.OrganismDoc, error) {
	doc := &model.OrganismDoc{}
	meta, err := org.organism.ReadDocument(context.Background(), id, doc)
	if err != nil {
		if driver.IsNotFoundGeneral(err) {
			return nil, &repository.OrganismNotFoundError{ID: id}
		}

		return nil, fmt.Errorf("error reading organism document: %w", err)
	}
	doc.DocumentMeta = meta

	return doc, nil
}

// GetOrganismByName retrieves an organism by its genus and species names. It
// returns an OrganismDoc containing the organism's details or an error. If no
// organism matches the given genus and species combination, it returns an
// OrganismNotFoundError.
func (org *organismRepo) GetOrganismByName(
	genus, species string,
) (*model.OrganismDoc, error) {
	res, err := org.database.GetRow(orgGetByNameQ,
		map[string]any{
			"@collection": org.organism.Name(),
			"genus":       genus,
			"species":     species,
		})
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	if res.IsEmpty() {
		return nil, &repository.OrganismNotFoundError{
			ID: fmt.Sprintf("%s %s", genus, species),
		}
	}

	doc := &model.OrganismDoc{}
	if err := res.Read(doc); err != nil {
		return nil, fmt.Errorf("error reading document: %w", err)
	}

	return doc, nil
}

func (org *organismRepo) AddOrganism(
	doc *dorg.NewOrganism,
) (*model.OrganismDoc, error) {
	// First check if organism already exists
	existing, err := org.GetOrganismByName(
		doc.Attributes.Genus,
		doc.Attributes.Species,
	)
	if err != nil {
		// Only proceed if it's a "not found" error
		if !repository.IsOrganismNotFound(err) {
			return nil, fmt.Errorf("error checking existing organism: %w", err)
		}
	}
	// If organism exists, return error
	if existing != nil {
		return nil, fmt.Errorf(
			"organism %s %s already exists",
			doc.Attributes.Genus,
			doc.Attributes.Species,
		)
	}

	// Create new organism document
	orgDoc := &model.OrganismDoc{
		CreatedAt:    doc.CreatedAt.AsTime(),
		UpdatedAt:    doc.CreatedAt.AsTime(),
		CreatedBy:    doc.CreatedBy,
		UpdatedBy:    doc.CreatedBy,
		Abbreviation: doc.Attributes.Abbreviation,
		CommonName:   doc.Attributes.CommonName,
		Species:      doc.Attributes.Species,
		Genus:        doc.Attributes.Genus,
	}

	// Insert document into collection
	meta, err := org.organism.CreateDocument(context.Background(), orgDoc)
	if err != nil {
		return nil, fmt.Errorf("error creating organism document: %w", err)
	}

	// Add document metadata
	orgDoc.DocumentMeta = meta

	return orgDoc, nil
}

func (org *organismRepo) EditOrganism(
	doc *dorg.OrganismUpdate,
) (*model.OrganismDoc, error) {
	orgDoc := &model.OrganismDoc{}
	_, err := org.organism.ReadDocument(context.Background(), doc.Id, orgDoc)
	if err != nil {
		if driver.IsNotFoundGeneral(err) {
			return nil, &repository.OrganismNotFoundError{ID: doc.Id}
		}

		return nil, fmt.Errorf("error reading organism document: %w", err)
	}

	update := map[string]any{
		"updated_at": time.Now(),
		"updated_by": doc.UpdatedBy,
	}
	attr := doc.Attributes
	if attr.Abbreviation != "" {
		update["abbreviation"] = attr.Abbreviation
	}
	if attr.CommonName != "" {
		update["common_name"] = attr.CommonName
	}
	if attr.Species != "" {
		update["species"] = attr.Species
	}
	if attr.Genus != "" {
		update["genus"] = attr.Genus
	}
	meta, err := org.organism.UpdateDocument(
		context.Background(),
		doc.Id,
		update,
	)
	if err != nil {
		return nil, fmt.Errorf("error updating organism document: %w", err)
	}

	// Read the updated document
	updatedDoc := &model.OrganismDoc{}
	_, err = org.organism.ReadDocument(
		context.Background(),
		doc.Id,
		updatedDoc,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"error reading updated organism document: %w",
			err,
		)
	}
	updatedDoc.DocumentMeta = meta

	return updatedDoc, nil
}

func (org *organismRepo) RemoveOrganism(oid string) error {
	// Check if organism exists first
	_, err := org.organism.ReadDocument(context.Background(), oid, nil)
	if err != nil {
		if driver.IsNotFoundGeneral(err) {
			return &repository.OrganismNotFoundError{ID: oid}
		}

		return fmt.Errorf("error checking organism existence: %w", err)
	}

	// Remove the organism document
	_, err = org.organism.RemoveDocument(context.Background(), oid)
	if err != nil {
		return fmt.Errorf("error removing organism document: %w", err)
	}

	return nil
}

func (org *organismRepo) ListOrganisms() ([]*model.OrganismDoc, error) {
	cursor, err := org.database.SearchRows(
		orgListQ,
		map[string]any{
			"@collection": org.organism.Name(),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error executing organism list query: %w", err)
	}
	defer func() { _ = cursor.Close() }()
	if cursor.IsEmpty() {
		return nil, &repository.ListNotFoundError{}
	}

	var organisms []*model.OrganismDoc
	for cursor.Scan() {
		omodel := &model.OrganismDoc{}
		if err := cursor.Read(omodel); err != nil {
			return nil, fmt.Errorf("error reading organism document: %w", err)
		}
		organisms = append(organisms, omodel)
	}

	return organisms, nil
}

func (org *organismRepo) ClearOrganisms() error {
	if err := org.organism.Truncate(context.Background()); err != nil {
		return fmt.Errorf("error clearing organisms collection: %w", err)
	}

	return nil
}

func (org *organismRepo) Dbh() *manager.Database {
	return org.database
}
