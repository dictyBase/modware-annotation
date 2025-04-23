package arangodb

import (
	"context"
	"fmt"
	"slices"
	"time"

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
	pub      driver.Collection
	edge     driver.Collection
	featPub  driver.Graph
}

// NewFeatureAnnoRepo creates a new instance of FeatureAnnotationRepository.
func NewFeatureAnnoRepo(
	connP *manager.ConnectParams,
	collP *FeatureCollectionParams,
) (repository.FeatureAnnotationRepository, error) {
	if err := validator.New().Struct(collP); err != nil {
		return nil, fmt.Errorf(
			"invalid feature collection parameters: %w", err,
		)
	}

	// Execute the initialization pipeline
	finalState := collection.Pipe7(
		&repoInitState{
			connP: connP,
			collP: collP,
		},
		stepCreateSession,
		stepCreateFeatureCollection,
		stepCreatePubCollection,
		stepCreateEdgeCollection,
		stepCreateFeatureIndices,
		stepCreatePubIndices,
		stepCreateGraph,
	)

	// Check for errors during the pipeline execution
	if finalState.Err != nil {
		return nil, fmt.Errorf(
			"error during repository initialization: %w",
			finalState.Err,
		)
	}

	return &featureAnnoRepo{
		sess:     finalState.sess,
		database: finalState.dbh,
		feature:  finalState.featureColl,
		pub:      finalState.pubColl,
		edge:     finalState.edgeColl,
		featPub:  finalState.graph,
	}, nil
}

// GetFeatureAnnotation retrieves a feature annotation by ID.
func (fann *featureAnnoRepo) GetFeatureAnnotation(
	fid string,
) (*model.FeatureAnnotationDoc, error) {
	res, err := fann.database.GetRow(
		featureGetByIdQ,
		map[string]interface{}{
			"@collection": fann.feature.Name(),
			"graph":       fann.featPub.Name(),
			"id":          fid,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	if res.IsEmpty() {
		return nil, &repository.AnnoNotFoundError{Id: fid}
	}
	doc := &model.FeatureAnnotationDoc{}
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
	faDoc := createFeatureAnnotationDoc(doc)

	// Setup transaction options
	txOptions := &manager.TransactionOptions{
		WriteCollections: []string{
			fann.feature.Name(),
			fann.pub.Name(),
			fann.edge.Name(),
		},
	}

	// Begin transaction with context
	txr, err := fann.database.BeginTransaction(
		context.Background(),
		txOptions,
	)
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}

	// Store feature annotation using AQL with transaction
	newDoc, err := fann.storeFeatureAnnotation(txr, faDoc)
	if err != nil {
		if abortErr := txr.Abort(); abortErr != nil {
			return nil, fmt.Errorf(
				"error in aborting transaction after %v: %w",
				err,
				abortErr,
			)
		}

		return nil, err
	}

	// Handle publications
	if err := fann.handlePublications(txr, doc, newDoc); err != nil {
		if abortErr := txr.Abort(); abortErr != nil {
			return nil, fmt.Errorf(
				"error in aborting transaction after %v: %w",
				err,
				abortErr,
			)
		}

		return nil, err
	}

	// Commit the transaction
	if err := txr.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return newDoc, nil
}

// createFeatureAnnotationDoc creates a new feature annotation document from the input.

// storeFeatureAnnotation stores the feature annotation in the database and returns the new document.
func (fann *featureAnnoRepo) storeFeatureAnnotation(
	txr *manager.TransactionHandler,
	faDoc *model.FeatureAnnotationDoc,
) (*model.FeatureAnnotationDoc, error) {
	result, err := txr.DoRun(
		fmt.Sprintf("INSERT @doc INTO %s RETURN NEW", fann.feature.Name()),
		map[string]interface{}{
			"doc": faDoc,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error storing feature annotation: %w", err)
	}

	// Read the result into a document
	newDoc := &model.FeatureAnnotationDoc{}
	if err := result.Read(newDoc); err != nil {
		return nil, fmt.Errorf("error reading result: %w", err)
	}

	return newDoc, nil
}

// handlePublications processes both types of publications (DOI and Pubmed).
func (fann *featureAnnoRepo) handlePublications(
	txr *manager.TransactionHandler,
	doc *feature.NewFeatureAnnotation,
	newDoc *model.FeatureAnnotationDoc,
) error {
	// Handle pubmed publications
	if !collection.IsEmpty(doc.Attributes.Pubmed) {
		if err := fann.processPublicationType(
			txr,
			newDoc,
			doc.Attributes.Pubmed,
			"pubmed",
		); err != nil {
			return err
		}
	}

	// Handle doi publications
	if !collection.IsEmpty(doc.Attributes.Publications) {
		if err := fann.processPublicationType(
			txr,
			newDoc,
			doc.Attributes.Publications,
			"doi",
		); err != nil {
			return err
		}
	}

	return nil
}

// processPublicationType handles a specific type of publication (DOI or Pubmed).
func (fann *featureAnnoRepo) processPublicationType(
	txr *manager.TransactionHandler,
	newDoc *model.FeatureAnnotationDoc,
	pubIDs []string,
	sourceType string,
) error {
	// Upsert publications
	pubKeys, err := fann.upsertPublicationsTx(txr, pubIDs)
	if err != nil {
		return err // Error already formatted in helper
	}

	// Create edges between feature and publications
	err = fann.createPublicationEdgesTx(
		txr,
		newDoc.ID.String(),
		pubKeys,
		sourceType,
	)
	if err != nil {
		return err // Error already formatted in helper
	}

	// Update the document with the publication IDs
	if sourceType == "pubmed" {
		newDoc.Pubmed = pubIDs
	} else {
		newDoc.Publications = pubIDs
	}

	return nil
}

// EditFeatureAnnotation updates an existing feature annotation with new values.
// It handles updating basic properties, attributes, and related publications.
//
// Parameters:
//   - doc: A FeatureAnnotationUpdate containing the ID and updated values
//
// Returns:
//   - The updated FeatureAnnotationDoc with all changes applied
//   - An error if the operation fails at any stage
//
// The update process occurs within a transaction to maintain data consistency.
// If publications are updated, the method ensures proper handling of both DOI
// and Pubmed IDs, including the creation of appropriate graph edges.
func (fann *featureAnnoRepo) EditFeatureAnnotation(
	doc *feature.FeatureAnnotationUpdate,
) (*model.FeatureAnnotationDoc, error) {
	// Execute the edit pipeline
	finalState := collection.Pipe8(
		&editState{
			fann: fann,
			doc:  doc,
		},
		stepValidateInput,
		stepFetchOriginalDoc,
		stepBeginTransaction,
		stepUpdateDocFields,
		stepExecuteUpdate,
		stepHandlePublications,
		stepCommitTransaction,
		stepRefreshDocumentState,
	)

	// Check for errors during the pipeline execution
	if finalState.Err != nil {
		return nil, finalState.Err
	}

	return finalState.updatedDoc, nil
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

func (fann *featureAnnoRepo) RemoveTag(
	req *feature.RemoveTagRequest,
) error {
	doc, err := fann.GetFeatureAnnotation(req.Id)
	if err != nil {
		return err
	}
	// Find tag index using IndexFunc
	idx := slices.IndexFunc(doc.Properties, func(p model.TagPropertyDoc) bool {
		return p.Tag == req.Tag
	})
	if idx == -1 {
		return fmt.Errorf("tag %s not found", req.Tag)
	}

	// Create updated properties using Delete
	newProps := slices.Delete(doc.Properties, idx, idx+1)

	_, err = fann.feature.UpdateDocument(
		context.Background(),
		doc.Key,
		map[string]interface{}{"properties": newProps},
	)
	if err != nil {
		return fmt.Errorf("error removing tag: %w", err)
	}

	return nil
}

// Dbh returns the underlying database handler.
func (fann *featureAnnoRepo) Dbh() *manager.Database {
	return fann.database
}

// upsertPublicationsTx handles the upsert logic for a list of publication IDs
// within a transaction and returns their corresponding document keys.
func (fann *featureAnnoRepo) upsertPublicationsTx(
	txr *manager.TransactionHandler,
	ids []string,
) ([]string, error) {
	result, err := txr.DoRun(
		pubUpsertQ,
		map[string]interface{}{
			"ids":         ids,
			"@collection": fann.pub.Name(),
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"error upserting publications within transaction: %w",
			err,
		)
	}
	pubKeys := make([]string, 0)
	err = result.Read(&pubKeys)
	if err != nil {
		return nil, fmt.Errorf(
			"error reading publication keys from transaction: %w",
			err,
		)
	}

	return pubKeys, nil
}

// createPublicationEdgesTx creates edges between a feature and publications within a transaction.
func (fann *featureAnnoRepo) createPublicationEdgesTx(
	txr *manager.TransactionHandler,
	featureKey string,
	pubKeys []string,
	source string,
) error {
	err := txr.Do(
		featurePubEdgeQ,
		map[string]interface{}{
			"feature_key":      featureKey,
			"pub_keys":         pubKeys,
			"source":           source,
			"@edge_collection": fann.edge.Name(),
		},
	)
	if err != nil {
		return fmt.Errorf(
			"error creating feature-publication edges within transaction: %w",
			err,
		)
	}

	return nil
}

func createFeatureAnnotationDoc(
	doc *feature.NewFeatureAnnotation,
) *model.FeatureAnnotationDoc {
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
	setOptionalFields(doc, faDoc)

	return faDoc
}
