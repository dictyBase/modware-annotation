package arangodb

import (
	"fmt"
	"io"

	"github.com/dictyBase/go-obograph/graph"
	ontostorage "github.com/dictyBase/go-obograph/storage"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/model"
)

func (ar *arangorepository) LoadOboJson(r io.Reader) (model.UploadStatus, error) {
	ds, err := ontoarango.NewDataSourceFromDb(ar.database,
		&ontoarango.CollectionParams{
			OboGraph:     ar.ontoc.Obog.Name(),
			GraphInfo:    ar.ontoc.Cv.Name(),
			Relationship: ar.ontoc.Rel.Name(),
			Term:         ar.ontoc.Term.Name(),
		})
	if err != nil {
		return model.Failed, err
	}
	g, err := graph.BuildGraph(r)
	if err != nil {
		return model.Failed, err
	}
	if ds.ExistsOboGraph(g) {
		return persistExistOboGraph(ds, g)
	}
	return persistNewOboGraph(ds, g)
}

func persistExistOboGraph(ds ontostorage.DataSource, g graph.OboGraph) (model.UploadStatus, error) {
	if err := ds.UpdateOboGraphInfo(g); err != nil {
		return model.Failed, fmt.Errorf("error in updating graph information %s", err)
	}
	if _, err := ds.SaveOrUpdateTerms(g); err != nil {
		return model.Failed, fmt.Errorf("error in updating terms %s", err)
	}
	if _, err := ds.SaveNewRelationships(g); err != nil {
		return model.Failed, fmt.Errorf("error in saving relationships %s", err)
	}
	return model.Updated, nil
}

func persistNewOboGraph(ds ontostorage.DataSource, g graph.OboGraph) (model.UploadStatus, error) {
	if err := ds.SaveOboGraphInfo(g); err != nil {
		return model.Failed, fmt.Errorf("error in saving graph information %s", err)
	}
	if _, err := ds.SaveTerms(g); err != nil {
		return model.Failed, fmt.Errorf("error in saving terms %s", err)
	}
	if _, err := ds.SaveRelationships(g); err != nil {
		return model.Failed, fmt.Errorf("error in saving relationships %s", err)
	}
	return model.Created, nil
}
