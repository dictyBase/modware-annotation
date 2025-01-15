package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
)

type featureAnnoRepo struct {
	sess     *manager.Session
	database *manager.Database
	feature  driver.Collection
}

// NewFeatureAnnoRepo creates a new instance of FeatureAnnotationRepository
func NewFeatureAnnoRepo(
	connP *manager.ConnectParams,
	collP *FeatureCollectionParams,
) (repository.FeatureAnnotationRepository, error) {
	if err := validateParams(collP); err != nil {
		return nil, err
	}

	sess, dbh, err := createSession(connP)
	if err != nil {
		return nil, err
	}

	featureColl, err := createFeatureCollection(dbh, collP)
	if err != nil {
		return nil, err
	}

	if err := createIndices(dbh, featureColl); err != nil {
		return nil, err
	}

	return &featureAnnoRepo{
		sess:     sess,
		database: dbh,
		feature:  featureColl,
	}, nil
}

func validateParams(collP *FeatureCollectionParams) error {
	if err := validator.New().Struct(collP); err != nil {
		return fmt.Errorf("invalid collection parameters: %w", err)
	}
	return nil
}

func createSession(
	connP *manager.ConnectParams,
) (*manager.Session, *manager.Database, error) {
	sess, dbh, err := manager.NewSessionDb(connP)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to create database session: %w",
			err,
		)
	}
	return sess, dbh, nil
}

func createFeatureCollection(
	dbh *manager.Database,
	collP *FeatureCollectionParams,
) (driver.Collection, error) {
	// Get JSON schema for validation
	schema, err := model.FeatureAnnotationSchema()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to generate feature annotation schema: %w",
			err,
		)
	}

	schemaOpt := &driver.CollectionSchemaOptions{
		Level:   driver.CollectionSchemaLevelModerate,
		Message: "Feature annotation validation failed",
		Type:    "json",
	}
	if err := schemaOpt.LoadRule(schema); err != nil {
		return nil, fmt.Errorf("error in loading schema %s", err)
	}
	// Create collection with schema validation
	coll, err := dbh.FindOrCreateCollection(
		collP.Feature,
		&driver.CreateCollectionOptions{
			Schema: schemaOpt,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create/find feature collection: %w",
			err,
		)
	}
	return coll, nil
}

func createIndices(dbh *manager.Database, coll driver.Collection) error {
	// Create compound index for id and version
	_, _, err := dbh.EnsurePersistentIndex(
		coll.Name(),
		[]string{"id", "vesion"},
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
			Unique:       true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create id-version index: %w", err)
	}

	// Create index for name field
	_, _, err = dbh.EnsurePersistentIndex(
		coll.Name(),
		[]string{"name"},
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create name index: %w", err)
	}

	return nil
}

// GetFeatureAnnotation retrieves a feature annotation by ID.
func (fr *featureAnnoRepo) GetFeatureAnnotation(
	fid string,
) (*model.FeatureAnnotationDoc, error) {
	doc := &model.FeatureAnnotationDoc{}
	res, err := fr.database.GetRow(
		featureGetByIdQ,
		map[string]interface{}{
			"@collection": fr.feature.Name(),
			"id":          fid,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	if res.IsEmpty() {
		return nil, &repository.AnnoNotFoundError{Id: fid}
	}
	if err := res.Read(doc); err != nil {
		return nil, fmt.Errorf("error reading document: %w", err)
	}

	return doc, nil
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
func setOptionalFields(
	doc *feature.NewFeatureAnnotation,
	faDoc *model.FeatureAnnotationDoc,
) {
	if len(doc.Attributes.Synonyms) > 0 {
		faDoc.Synonyms = doc.Attributes.Synonyms
	}
	if len(doc.Attributes.Publications) > 0 {
		faDoc.Publications = doc.Attributes.Publications
	}
	if len(doc.Attributes.Pubmed) > 0 {
		faDoc.Pubmed = doc.Attributes.Pubmed
	}
	if len(doc.Attributes.Properties) > 0 {
		faDoc.Properties = collection.Map(
			doc.Attributes.Properties,
			func(prop *feature.TagProperty) model.TagPropertyDoc {
				return model.TagPropertyDoc{
					Tag:   prop.Tag,
					Value: prop.Value,
				}
			},
		)
	}
}

func toDbLink(link *feature.DbLink) model.DbLinkDoc {
	dbLink := model.DbLinkDoc{
		PrimaryId: link.PrimaryId,
		Version:   link.Version,
		Database:  link.Database,
	}

	// Set optional fields only if present
	if link.Linktype != "" {
		dbLink.LinkType = link.Linktype
	}
	if link.Url != "" {
		dbLink.URL = link.Url
	}
	if link.Label != "" {
		dbLink.Label = link.Label
	}

	return dbLink
}

func (fr *featureAnnoRepo) Dbh() *manager.Database {
	return fr.database
}
