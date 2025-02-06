package arangodb

import (
	"context"
	"fmt"
	"slices"
	"time"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
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
		AnnoId:     doc.Id,
		Name:       doc.Attributes.Name,
		CreatedAt:  doc.CreatedAt.AsTime(),
		UpdatedAt:  doc.CreatedAt.AsTime(), // Initially same as created_at
		CreatedBy:  doc.CreatedBy,
		UpdatedBy:  doc.CreatedBy, // Initially same as created_by
		IsObsolete: false,
	}
	if doc.UpdatedAt.IsValid() {
		faDoc.UpdatedAt = doc.UpdatedAt.AsTime()
	}
	if len(doc.UpdatedBy) > 0 {
		faDoc.UpdatedBy = doc.UpdatedBy
	}

	// Set optional fields
	// Make sure a new version of faDoc is returned. AI!
	setOptionalFields(doc, faDoc)
	
	// Create context to return new document
	newDoc := &model.FeatureAnnotationDoc{}
	ctx := driver.WithReturnNew(context.Background(), newDoc)
	
	// Insert document and get updated version
	meta, err := fann.feature.CreateDocument(ctx, faDoc)
	if err != nil {
		return nil, fmt.Errorf(
			"error creating feature annotation document: %w",
			err,
		)
	}
	newDoc.DocumentMeta = meta

	return newDoc, nil
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
		faDoc.Key,
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

func (fann *featureAnnoRepo) RemoveFeatureAnnotation(
	fid string,
	purge bool,
) error {
	bindVars := map[string]interface{}{
		"@collection": fann.feature.Name(),
		"id":          fid,
	}

	// Check if document exists
	existRes, err := fann.database.GetRow(featureExistQ, bindVars)
	if err != nil {
		return fmt.Errorf("error checking document existence: %w", err)
	}
	if existRes.IsEmpty() {
		return &repository.AnnoNotFoundError{Id: fid}
	}

	if purge {
		err := fann.database.Do(featurePurgeQ, bindVars)
		if err != nil {
			return fmt.Errorf("error executing purge query: %w", err)
		}

		return nil
	}

	err = fann.database.Do(featureObsoleteQ, bindVars)
	if err != nil {
		return fmt.Errorf("error executing obsolete query: %w", err)
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

func (fann *featureAnnoRepo) AddTag(
	req *feature.AddTagRequest,
) (*model.FeatureAnnotationDoc, error) {
	// Get existing document
	doc, err := fann.GetFeatureAnnotation(req.Id)
	if err != nil {
		return nil, err
	}

	// Append new tag
	newTags := doc.Properties
	newTags = append(newTags, model.TagPropertyDoc{
		Tag:       req.Tag.Tag,
		Value:     req.Tag.Value,
		CreatedBy: req.Tag.CreatedBy,
		CreatedAt: time.Now(),
		UpdatedBy: req.Tag.CreatedBy,
		UpdatedAt: time.Now(),
	})
	newDoc := &model.FeatureAnnotationDoc{}
	ctx := driver.WithReturnNew(context.Background(), newDoc)
	meta, err := fann.feature.UpdateDocument(
		ctx,
		doc.Key,
		map[string]interface{}{"properties": newTags},
	)
	if err != nil {
		return nil, fmt.Errorf("error adding tag: %w", err)
	}
	newDoc.DocumentMeta = meta
	return newDoc, nil
}

func (fann *featureAnnoRepo) UpdateTag(
	req *feature.UpdateTagRequest,
) (*model.FeatureAnnotationDoc, error) {
	doc, err := fann.GetFeatureAnnotation(req.Id)
	if err != nil {
		return nil, err
	}

	// Find tag index using IndexFunc
	idx := slices.IndexFunc(doc.Properties, func(p model.TagPropertyDoc) bool {
		return p.Tag == req.Tag.Tag
	})
	if idx == -1 {
		return nil, fmt.Errorf("tag %s not found", req.Tag.Tag)
	}

	// Create updated properties slice
	newProps := make([]model.TagPropertyDoc, len(doc.Properties))
	copy(newProps, doc.Properties)
	newProps[idx] = model.TagPropertyDoc{
		Tag:       req.Tag.Tag,
		Value:     req.Tag.Value,
		CreatedBy: doc.Properties[idx].CreatedBy, // Preserve original creator
		CreatedAt: doc.Properties[idx].CreatedAt, // Preserve creation time
		UpdatedBy: req.Tag.UpdatedBy,
		UpdatedAt: time.Now(),
	}

	// Perform partial update and return new document
	newDoc := &model.FeatureAnnotationDoc{}
	ctx := driver.WithReturnNew(context.Background(), newDoc)
	meta, err := fann.feature.UpdateDocument(
		ctx,
		doc.Key,
		map[string]interface{}{"properties": newProps},
	)
	if err != nil {
		return nil, fmt.Errorf("error updating tag: %w", err)
	}
	newDoc.DocumentMeta = meta

	return newDoc, nil
}
// Dbh returns the underlying database handler.

func (fann *featureAnnoRepo) Dbh() *manager.Database {
	return fann.database
}
