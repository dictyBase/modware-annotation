package arangodb

import (
	"context"

	"github.com/go-playground/validator/v10"

	manager "github.com/dictyBase/arangomanager"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type arangorepository struct {
	sess     *manager.Session
	database *manager.Database
	anno     *annoc
	onto     *ontoarango.OntoCollection
}

func NewTaggedAnnotationRepo(connP *manager.ConnectParams, collP *CollectionParams, ontoP *ontoarango.CollectionParams) (repo.TaggedAnnotationRepository, error) {
	ar := &arangorepository{}
	if err := validator.New().Struct(collP); err != nil {
		return ar, err
	}
	sess, db, err := manager.NewSessionDb(connP)
	if err != nil {
		return ar, err
	}
	ontoc, err := ontoarango.CreateCollection(db, ontoP)
	if err != nil {
		return ar, err
	}
	annoc, err := setAnnotationCollection(db, ontoc, collP)
	return &arangorepository{
		sess:     sess,
		database: db,
		onto:     ontoc,
		anno:     annoc,
	}, err
}

// Clear clears all annotations and related ontologies from the repository
// datasource
func (ar *arangorepository) Clear() error {
	if err := ar.anno.annot.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.ver.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.term.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.verg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.annotg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.Term.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.Cv.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.Rel.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.Obog.Remove(context.Background()); err != nil {
		return err
	}
	return nil
}

// ClearAnnotations clears all annotations from the repository datasource
func (ar *arangorepository) ClearAnnotations() error {
	if err := ar.anno.annot.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.ver.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.term.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.verg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.annotg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.annog.Remove(context.Background()); err != nil {
		return err
	}
	return nil
}
