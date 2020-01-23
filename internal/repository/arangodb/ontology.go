package arangodb

import (
	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
)

type ontoc struct {
	term driver.Collection
	rel  driver.Collection
	cv   driver.Collection
	obog driver.Graph
}

func setOntologyCollection(db *manager.Database, collP *CollectionParams) (*ontoc, error) {
	oc := &ontoc{}
	termc, err := db.FindOrCreateCollection(collP.Term, &driver.CreateCollectionOptions{})
	if err != nil {
		return oc, err
	}
	relc, err := db.FindOrCreateCollection(
		collP.Relationship,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	if err != nil {
		return oc, err
	}
	graphc, err := db.FindOrCreateCollection(
		collP.GraphInfo,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return oc, err
	}
	obog, err := db.FindOrCreateGraph(
		collP.OboGraph,
		[]driver.EdgeDefinition{
			{
				Collection: relc.Name(),
				From:       []string{termc.Name()},
				To:         []string{termc.Name()},
			},
		},
	)
	return &ontoc{
		term: termc,
		rel:  relc,
		cv:   graphc,
		obog: obog,
	}, err
}
