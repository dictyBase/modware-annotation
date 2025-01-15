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
)

type featureAnnoRepo struct {
	sess     *manager.Session
	database *manager.Database
	feature  driver.Collection
}

// NewFeatureAnnoRepo creates a new instance of FeatureAnnotationRepository.
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

// GetFeatureAnnotation retrieves a feature annotation by ID.
func (fann *featureAnnoRepo) GetFeatureAnnotation(
	fid string,
) (*model.FeatureAnnotationDoc, error) {
	doc := &model.FeatureAnnotationDoc{}
	res, err := fann.database.GetRow(
		featureGetByIdQ,
		map[string]interface{}{
			"@collection": fann.feature.Name(),
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
func (fann *featureAnnoRepo) AddFeatureAnnotation(
	doc *feature.NewFeatureAnnotation,
) (*model.FeatureAnnotationDoc, error) {
	// Create new feature annotation document
	faDoc := &model.FeatureAnnotationDoc{
		Id:        doc.Id,
		Version:   doc.Version,
		Name:      doc.Attributes.Name,
		CreatedAt: doc.CreatedAt.AsTime(),
		UpdatedAt: doc.CreatedAt.AsTime(), // Initially same as created_at
		CreatedBy: doc.CreatedBy,
		UpdatedBy: doc.CreatedBy, // Initially same as created_by
	}

	// Set optional fields
	setOptionalFields(doc, faDoc)

	// Add DbLinks if present
	if len(doc.Attributes.Dblinks) > 0 {
		faDoc.DbLinks = collection.Map(doc.Attributes.Dblinks, toDbLink)
	}

	// Insert document into collection
	meta, err := fann.feature.CreateDocument(context.Background(), faDoc)
	if err != nil {
		return nil, fmt.Errorf(
			"error creating feature annotation document: %w",
			err,
		)
	}

	// Add document metadata
	faDoc.DocumentMeta = meta

	return faDoc, nil
}

// EditFeatureAnnotation updates an existing feature annotation.
func (fann *featureAnnoRepo) EditFeatureAnnotation(
	doc *feature.FeatureAnnotationUpdate,
) (*model.FeatureAnnotationDoc, error) {
	faDoc, err := fann.GetFeatureAnnotation(doc.Id)
	if err != nil {
		return nil, err
	}

	updateBasicFields(faDoc, doc)
	if doc.Attributes != nil {
		updateAttributes(faDoc, doc.Attributes)
	}

	meta, err := fann.feature.UpdateDocument(
		context.Background(),
		doc.Id,
		faDoc,
	)
	if err != nil {
		return nil, fmt.Errorf("error in updating document %s", err)
	}
	faDoc.DocumentMeta = meta

	return faDoc, nil
}

// ListFeatureAnnotations lists all feature annotations.
func (fann *featureAnnoRepo) ListFeatureAnnotations() ([]*model.FeatureAnnotationDoc, error) {
	return nil, fmt.Errorf("not implemented")
}

// RemoveFeatureAnnotation deletes a feature annotation.
func (fann *featureAnnoRepo) RemoveFeatureAnnotation(fid string) error {
	_, err := fann.feature.RemoveDocument(context.Background(), fid)
	if err != nil {
		if driver.IsNotFoundGeneral(err) {
			return &repository.AnnoNotFoundError{Id: fid}
		}

		return fmt.Errorf(
			"error in removing feature annotation %s: %s",
			fid,
			err,
		)
	}

	return nil
}

// ClearFeatureAnnotations removes all feature annotations.
func (fann *featureAnnoRepo) ClearFeatureAnnotations() error {
	if err := fann.feature.Truncate(context.Background()); err != nil {
		return fmt.Errorf(
			"error clearing feature annotations collection: %w",
			err,
		)
	}

	return nil
}

// Dbh returns the underlying database handler.

func (fann *featureAnnoRepo) Dbh() *manager.Database {
	return fann.database
}
