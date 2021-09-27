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
			OboGraph:     ar.onto.Obog.Name(),
			GraphInfo:    ar.onto.Cv.Name(),
			Relationship: ar.onto.Rel.Name(),
			Term:         ar.onto.Term.Name(),
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

func (ar *arangorepository) termID(onto, term string) (string, error) {
	var id string
	r, err := ar.database.GetRow(annExistTagQ, map[string]interface{}{
		"@cv_collection":     ar.onto.Cv.Name(),
		"@cvterm_collection": ar.onto.Term.Name(),
		"ontology":           onto,
		"tag":                term,
	})
	if err != nil {
		return id, fmt.Errorf("error in running obograph retrieving query %s", err)
	}
	if r.IsEmpty() {
		return id, fmt.Errorf("ontology %s and tag %s does not exist", onto, term)
	}
	if err := r.Read(&id); err != nil {
		return id, fmt.Errorf("error in retrieving obograph id %s", err)
	}
	return id, nil
}

func (ar *arangorepository) termName(id string) (string, error) {
	var name string
	cvtr, err := ar.database.GetRow(cvtID2LblQ, map[string]interface{}{
		"@cvterm_collection": ar.onto.Term.Name(),
		"id":                 id,
	})
	if err != nil {
		return name,
			fmt.Errorf("error in running tag retrieving query %s", err)
	}
	if cvtr.IsEmpty() {
		return name, fmt.Errorf("cvterm id %s does not exist", id)
	}
	if err := cvtr.Read(&name); err != nil {
		return name, fmt.Errorf("error in retrieving tag %s", err)
	}
	return name, nil
}
