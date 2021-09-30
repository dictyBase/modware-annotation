package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/go-playground/validator/v10"

	manager "github.com/dictyBase/arangomanager"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/repository"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type annoc struct {
	annot  driver.Collection
	term   driver.Collection
	ver    driver.Collection
	annog  driver.Collection
	verg   driver.Graph
	annotg driver.Graph
}

type arangorepository struct {
	sess     *manager.Session
	database *manager.Database
	anno     *annoc
	onto     *ontoarango.OntoCollection
}

func NewTaggedAnnotationRepo(
	connP *manager.ConnectParams, collP *CollectionParams, ontoP *ontoarango.CollectionParams,
) (repo.TaggedAnnotationRepository, error) {
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

func setAnnotationCollection(db *manager.Database, onto *ontoarango.OntoCollection, collP *CollectionParams) (*annoc, error) {
	ac, err := setDocumentCollection(db, collP)
	if err != nil {
		return ac, err
	}
	verg, err := db.FindOrCreateGraph(
		collP.AnnoVerGraph,
		[]driver.EdgeDefinition{
			{
				Collection: ac.ver.Name(),
				From:       []string{ac.annot.Name()},
				To:         []string{ac.annot.Name()},
			},
		},
	)
	if err != nil {
		return ac, err
	}
	annotg, err := db.FindOrCreateGraph(
		collP.AnnoTagGraph,
		[]driver.EdgeDefinition{
			{
				Collection: ac.term.Name(),
				From:       []string{ac.annot.Name()},
				To:         []string{onto.Term.Name()},
			},
		},
	)
	if err != nil {
		return ac, err
	}
	ac.verg = verg
	ac.annotg = annotg
	_, _, err = db.EnsurePersistentIndex(
		ac.annot.Name(),
		collP.AnnoIndexes,
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
		},
	)
	return ac, err
}

func setDocumentCollection(db *manager.Database, collP *CollectionParams) (*annoc, error) {
	ac := &annoc{}
	anno, err := db.FindOrCreateCollection(
		collP.Annotation,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return ac, err
	}
	annogrp, err := db.FindOrCreateCollection(
		collP.AnnoGroup,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return ac, err
	}
	annocvt, err := db.FindOrCreateCollection(
		collP.AnnoTerm,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	if err != nil {
		return ac, err
	}
	annov, err := db.FindOrCreateCollection(
		collP.AnnoVersion,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	return &annoc{
		annot: anno,
		annog: annogrp,
		term:  annocvt,
		ver:   annov,
	}, err
}

// Clear clears all annotations and related ontologies from the repository
// datasource
func (ar *arangorepository) Clear() error {
	if err := ar.ClearAnnotations(); err != nil {
		return err
	}
	for _, c := range []driver.Collection{
		ar.onto.Term, ar.onto.Cv, ar.onto.Rel,
	} {
		if err := c.Truncate(context.Background()); err != nil {
			return err
		}
	}
	return ar.onto.Obog.Remove(context.Background())
}

// ClearAnnotations clears all annotations from the repository datasource
func (ar *arangorepository) ClearAnnotations() error {
	for _, c := range []driver.Collection{
		ar.anno.annot, ar.anno.ver, ar.anno.term, ar.anno.annog,
	} {
		if err := c.Truncate(context.Background()); err != nil {
			return err
		}
	}
	for _, g := range []driver.Graph{
		ar.anno.verg,
		ar.anno.annotg,
	} {
		if err := g.Remove(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

func DocumentsExists(c driver.Collection, ids ...string) error {
	for _, k := range ids {
		ok, err := c.DocumentExists(context.Background(), k)
		if err != nil {
			return fmt.Errorf("error in checking for existence of identifier %s %s", k, err)
		}
		if !ok {
			return &repository.AnnoNotFound{Id: k}
		}
	}
	return nil
}
