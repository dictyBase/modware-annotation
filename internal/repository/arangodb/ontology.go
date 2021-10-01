package arangodb

import (
	"fmt"
	"io"

	"github.com/dictyBase/go-obograph/storage"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
)

func (ar *arangorepository) LoadOboJSON(r io.Reader) (*storage.UploadInformation, error) {
	ds, err := ontoarango.NewDataSourceFromDb(ar.database, &ontoarango.CollectionParams{
		OboGraph:     ar.onto.Obog.Name(),
		GraphInfo:    ar.onto.Cv.Name(),
		Relationship: ar.onto.Rel.Name(),
		Term:         ar.onto.Term.Name(),
	})
	if err != nil {
		return &storage.UploadInformation{}, err
	}
	return storage.LoadOboJSONFromDataSource(r, ds)
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
