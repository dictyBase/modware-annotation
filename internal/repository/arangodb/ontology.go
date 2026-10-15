package arangodb

import (
	"fmt"
	"io"

	fperrors "github.com/IBM/fp-go/v2/errors"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	P "github.com/IBM/fp-go/v2/predicate"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-obograph/storage"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
)

var (
	runTermIDQueryFault = fperrors.OnError(
		"error in running obograph retrieving query",
	)
	readTermIDFault = fperrors.OnError(
		"error in retrieving obograph id",
	)
	isNonEmptyTermRow = P.Not(func(row *manager.Result) bool {
		return row.IsEmpty()
	})
)

func (ar *arangorepository) LoadOboJSON(rde io.Reader) (*storage.UploadInformation, error) {
	dsb, err := ontoarango.NewDataSourceFromDb(ar.database, &ontoarango.CollectionParams{
		OboGraph:     ar.onto.Obog.Name(),
		GraphInfo:    ar.onto.Cv.Name(),
		Relationship: ar.onto.Rel.Name(),
		Term:         ar.onto.Term.Name(),
	})
	if err != nil {
		return &storage.UploadInformation{}, fmt.Errorf(
			"error in creating new data source %s",
			err,
		)
	}

	info, err := storage.LoadOboJSONFromDataSource(rde, dsb)
	if err != nil {
		return &storage.UploadInformation{}, fmt.Errorf(
			"error in uploading JSON %s",
			err,
		)
	}

	return info, nil
}

func (ar *arangorepository) lookupTermID(
	onto, term string,
) IOE.IOEither[error, string] {
	missingTermFault := F.Constant1[*manager.Result](
		fmt.Errorf(
			"ontology %s and tag %s does not exist",
			onto,
			term,
		),
	)

	return F.Pipe4(
		IOE.TryCatchError(func() (*manager.Result, error) {
			return ar.database.GetRow(annExistTagQ, map[string]any{
				cvCollectionBind:     ar.onto.Cv.Name(),
				cvtermCollectionBind: ar.onto.Term.Name(),
				"ontology":           onto,
				"tag":                term,
			})
		}),
		IOE.MapLeft[*manager.Result](runTermIDQueryFault),
		IOE.FilterOrElse(isNonEmptyTermRow, missingTermFault),
		IOE.Chain(func(
			row *manager.Result,
		) IOE.IOEither[error, string] {
			return IOE.TryCatchError(func() (string, error) {
				var tid string
				err := row.Read(&tid)

				return tid, err
			})
		}),
		IOE.MapLeft[string](readTermIDFault),
	)
}

func (ar *arangorepository) termID(onto, term string) (string, error) {
	var tid string
	row, err := ar.database.GetRow(annExistTagQ, map[string]any{
		cvCollectionBind:     ar.onto.Cv.Name(),
		cvtermCollectionBind: ar.onto.Term.Name(),
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
	cvtr, err := ar.database.GetRow(cvtID2LblQ, map[string]any{
		cvtermCollectionBind: ar.onto.Term.Name(),
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
