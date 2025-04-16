package arangodb

import (
	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
)

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
