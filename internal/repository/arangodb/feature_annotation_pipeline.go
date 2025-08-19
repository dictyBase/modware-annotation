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

// editState holds the state during the annotation editing pipeline.
type editState struct {
	fann        *featureAnnoRepo
	doc         *feature.FeatureAnnotationUpdate
	txr         *manager.TransactionHandler
	origDoc     *model.FeatureAnnotationDoc
	updatedDoc  *model.FeatureAnnotationDoc
	updateQuery string
	Err         error
}

// repoInitState holds the state during the repository initialization pipeline.
type repoInitState struct {
	connP       *manager.ConnectParams
	collP       *FeatureCollectionParams
	sess        *manager.Session
	dbh         *manager.Database
	featureColl driver.Collection
	pubColl     driver.Collection
	edgeColl    driver.Collection
	graph       driver.Graph
	Err         error
}

// stepCreateSession creates the database session and handle.
func stepCreateSession(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.sess, state.dbh, state.Err = createSession(state.connP)

	return state
}

// stepCreateFeatureCollection creates the feature collection.
func stepCreateFeatureCollection(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.featureColl, state.Err = createFeatureCollection(
		state.dbh,
		state.collP,
	)

	return state
}

// stepCreatePubCollection creates the publication collection.
func stepCreatePubCollection(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.pubColl, state.Err = createPubCollection(state.dbh, state.collP)

	return state
}

// stepCreateEdgeCollection creates the edge collection.
func stepCreateEdgeCollection(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.edgeColl, state.Err = createEdgeCollection(state.dbh, state.collP)

	return state
}

// stepCreateFeatureIndices creates indices for the feature collection.
func stepCreateFeatureIndices(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.Err = createFeatureIndices(state.dbh, state.featureColl)

	return state
}

// stepCreatePubIndices creates indices for the publication collection.
func stepCreatePubIndices(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.Err = createPubIndices(state.dbh, state.pubColl)

	return state
}

// stepCreateGraph creates the graph connecting feature and pub collections.
func stepCreateGraph(state *repoInitState) *repoInitState {
	if state.Err != nil {
		return state
	}
	state.graph, state.Err = createFeaturePubGraph(
		state.dbh,
		state.collP.Graph,
		state.featureColl,
		state.pubColl,
		state.edgeColl,
	)

	return state
}

// stepFetchOriginalDoc fetches the original document to be updated.
func stepFetchOriginalDoc(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	var err error
	state.origDoc, err = state.fann.GetFeatureAnnotation(state.doc.Id)
	if err != nil {
		if repository.IsAnnotationNotFound(err) {
			state.Err = err // Preserve specific error type
		} else {
			state.Err = fmt.Errorf(
				"error fetching original document: %w",
				err,
			)
		}
	}

	return state
}

// stepBeginTransaction starts a transaction for the annotation update.
func stepBeginTransaction(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	state.txr, state.Err = state.fann.database.BeginTransaction(
		context.Background(),
		&manager.TransactionOptions{
			WriteCollections: []string{
				state.fann.feature.Name(),
				state.fann.pub.Name(),
				state.fann.edge.Name(),
			},
		})

	if state.Err != nil {
		state.Err = fmt.Errorf("error beginning transaction: %w", state.Err)
	}

	return state
}

// stepUpdateDocFields updates the document fields with new values.
func stepUpdateDocFields(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	// Start with a copy of the original document
	updatedDoc := copyFeatureAnnotationDoc(state.origDoc)

	// Update basic fields (returns new copy)
	updatedDoc = updateBasicFields(updatedDoc, state.doc)

	// Handle attributes update - prefer UpdateAttributes over deprecated Attributes
	if state.doc.UpdateAttributes != nil {
		// Use the new partial update logic
		updatedDoc = updateAttributesPartial(
			updatedDoc,
			state.doc.UpdateAttributes,
		)
	} else if state.doc.Attributes != nil { //nolint:staticcheck // Backward compatibility with deprecated field
		// Fall back to deprecated full update for backward compatibility
		//nolint:staticcheck // Backward compatibility with deprecated field
		updatedDoc = updateAttributes(updatedDoc, state.doc.Attributes)
	}

	// Store the updated document in state
	state.updatedDoc = updatedDoc

	// Prepare the update query
	state.updateQuery = fmt.Sprintf(
		"UPDATE @doc WITH @data IN %s RETURN NEW",
		state.fann.feature.Name(),
	)

	return state
}

// stepExecuteUpdate executes the update query and reads the result.
func stepExecuteUpdate(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	result, err := state.txr.DoRun(
		state.updateQuery,
		map[string]interface{}{
			"doc":  state.updatedDoc.Key, // Use updatedDoc instead of origDoc
			"data": state.updatedDoc,     // Use updatedDoc instead of origDoc
		},
	)
	if err != nil {
		state.Err = fmt.Errorf("error updating feature annotation: %w", err)

		return state
	}

	// Read the result from the database
	dbDoc := &model.FeatureAnnotationDoc{}
	if err := result.Read(dbDoc); err != nil {
		state.Err = fmt.Errorf("error reading updated document: %w", err)
	} else {
		state.updatedDoc = dbDoc
	}

	return state
}

// stepHandlePublications processes publications if needed.
func stepHandlePublications(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	// Determine which attributes to use - prefer UpdateAttributes over deprecated Attributes
	var pubmedIDs []string
	var doiPublications []string

	switch {
	case state.doc.UpdateAttributes != nil:
		// Use publications from the new UpdateAttributes field
		pubmedIDs = state.doc.UpdateAttributes.Pubmed
		doiPublications = state.doc.UpdateAttributes.Publications
	case state.doc.Attributes != nil: //nolint:staticcheck // Backward compatibility with deprecated field
		// Fall back to deprecated Attributes field for backward compatibility
		pubmedIDs = state.doc.Attributes.Pubmed             //nolint:staticcheck // Backward compatibility with deprecated field
		doiPublications = state.doc.Attributes.Publications //nolint:staticcheck // Backward compatibility with deprecated field
	default:
		// No attributes to process
		return state
	}

	// Process Pubmed IDs
	if !collection.IsEmpty(pubmedIDs) {
		if err := state.fann.processPublicationType(
			state.txr,
			state.updatedDoc,
			pubmedIDs,
			"pubmed",
		); err != nil {
			state.Err = fmt.Errorf(
				"error processing pubmed publications: %w",
				err,
			)

			return state
		}
	}

	// Process DOI publications
	if !collection.IsEmpty(doiPublications) {
		if err := state.fann.processPublicationType(
			state.txr,
			state.updatedDoc,
			doiPublications,
			"doi",
		); err != nil {
			state.Err = fmt.Errorf("error processing DOI publications: %w", err)

			return state
		}
	}

	return state
}

// stepCommitTransaction commits the transaction.
func stepCommitTransaction(state *editState) *editState {
	// If there's no error, commit the transaction
	if state.Err == nil {
		if err := state.txr.Commit(); err != nil {
			state.Err = fmt.Errorf("error committing transaction: %w", err)
		}

		return state
	}
	// If there's an error but no transaction, just return the state
	if state.txr == nil {
		return state
	}

	// If there's an error and a transaction, abort it
	abortErr := state.txr.Abort()
	if abortErr != nil {
		state.Err = fmt.Errorf(
			"%v, also failed to abort transaction: %w",
			state.Err,
			abortErr,
		)
	}

	return state
}

// featureAnnotationUpdateValidator defines validation rules for feature
// annotation updates.
type featureAnnotationUpdateValidator struct {
	ID        string `validate:"required"       json:"id"`
	UpdatedBy string `validate:"required,email" json:"updated_by"`
}

// stepValidateInput validates the input document before proceeding.
func stepValidateInput(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	// Perform validation
	if err := validate.Struct(&featureAnnotationUpdateValidator{
		ID:        state.doc.Id,
		UpdatedBy: state.doc.UpdatedBy,
	}); err != nil {
		state.Err = fmt.Errorf("invalid feature annotation update: %w", err)

		return state
	}

	return state
}

// stepRefreshDocumentState retrieves the final document state after all
// modifications.
func stepRefreshDocumentState(state *editState) *editState {
	if state.Err != nil {
		return state
	}

	// Get the latest document state with all changes (including publication edges)
	updatedDoc, err := state.fann.GetFeatureAnnotation(state.doc.Id)
	if err != nil {
		state.Err = fmt.Errorf("error retrieving final document state: %w", err)

		return state
	}

	// Update the state with the refreshed document
	state.updatedDoc = updatedDoc

	return state
}
