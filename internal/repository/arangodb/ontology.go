package arangodb

import (
	"fmt"
	"io"

	"github.com/dictyBase/go-obograph/storage"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
)

func (ar *arangorepository) LoadOboJSON(rde io.Reader) (*storage.UploadInformation, error) {
	dsb, err := ontoarango.NewDataSourceFromDb(ar.database, &ontoarango.CollectionParams{
		OboGraph:     ar.onto.Obog.Name(),
		GraphInfo:    ar.onto.Cv.Name(),
		Relationship: ar.onto.Rel.Name(),
		Term:         ar.onto.Term.Name(),
	})
	if err != nil {
		return &storage.UploadInformation{}, fmt.Errorf("error in creating new data source %s", err)
	}

	info, err := storage.LoadOboJSONFromDataSource(rde, dsb)
	if err != nil {
		return &storage.UploadInformation{}, fmt.Errorf("error in uploading JSON %s", err)
	}

	return info, nil
}

func (ar *arangorepository) termID(onto, term string) (string, error) {
	var tid string
	row, err := ar.database.GetRow(annExistTagQ, map[string]interface{}{
		"@cv_collection":     ar.onto.Cv.Name(),
		"@cvterm_collection": ar.onto.Term.Name(),
		"ontology":           onto,
		"tag":                term,
	})
	if err != nil {
		return tid, fmt.Errorf("error in running obograph retrieving query %s", err)
	}
	if row.IsEmpty() {
		return tid, fmt.Errorf("ontology %s and tag %s does not exist", onto, term)
	}
	if err := row.Read(&tid); err != nil {
		return tid, fmt.Errorf("error in retrieving obograph id %s", err)
	}

	return tid, nil
}

func (ar *arangorepository) termName(tid string) (string, error) {
	var name string
	cvtr, err := ar.database.GetRow(cvtID2LblQ, map[string]interface{}{
		"@cvterm_collection": ar.onto.Term.Name(),
		"id":                 tid,
	})
	if err != nil {
		return name,
			fmt.Errorf("error in running tag retrieving query %s", err)
	}
	if cvtr.IsEmpty() {
		return name, fmt.Errorf("cvterm id %s does not exist", tid)
	}
	if err := cvtr.Read(&name); err != nil {
		return name, fmt.Errorf("error in retrieving tag %s", err)
	}

	return name, nil
}
