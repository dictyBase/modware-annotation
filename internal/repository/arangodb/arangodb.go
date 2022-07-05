package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/go-playground/validator/v10"

	manager "github.com/dictyBase/arangomanager"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
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
	arp := &arangorepository{}
	if err := validator.New().Struct(collP); err != nil {
		return arp, fmt.Errorf("error in validation %s", err)
	}
	sess, dbh, err := manager.NewSessionDb(connP)
	if err != nil {
		return arp, fmt.Errorf("error in creating new session %s", err)
	}
	ontoc, err := ontoarango.CreateCollection(dbh, ontoP)
	if err != nil {
		return arp, fmt.Errorf("error in creating ontology collection %s", err)
	}
	annoc, err := setAnnotationCollection(dbh, ontoc, collP)
	return &arangorepository{
		sess:     sess,
		database: dbh,
		onto:     ontoc,
		anno:     annoc,
	}, err
}

func setAnnotationCollection(dbh *manager.Database, onto *ontoarango.OntoCollection, collP *CollectionParams) (*annoc, error) {
	annoc, err := setDocumentCollection(dbh, collP)
	if err != nil {
		return annoc, fmt.Errorf("error in creating document collection %s", err)
	}
	verg, err := dbh.FindOrCreateGraph(
		collP.AnnoVerGraph,
		[]driver.EdgeDefinition{
			{
				Collection: annoc.ver.Name(),
				From:       []string{annoc.annot.Name()},
				To:         []string{annoc.annot.Name()},
			},
		},
	)
	if err != nil {
		return annoc, fmt.Errorf("error in creating graph %s", err)
	}
	annotg, err := dbh.FindOrCreateGraph(
		collP.AnnoTagGraph,
		[]driver.EdgeDefinition{
			{
				Collection: annoc.term.Name(),
				From:       []string{annoc.annot.Name()},
				To:         []string{onto.Term.Name()},
			},
		},
	)
	if err != nil {
		return annoc, fmt.Errorf("error in creating graph %s", err)
	}
	annoc.verg = verg
	annoc.annotg = annotg
	_, _, err = dbh.EnsurePersistentIndex(
		annoc.annot.Name(),
		collP.AnnoIndexes,
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
		},
	)
	return annoc, err
}

func setDocumentCollection(dbh *manager.Database, collP *CollectionParams) (*annoc, error) {
	anns := &annoc{}
	anno, err := dbh.FindOrCreateCollection(
		collP.Annotation,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return anns, fmt.Errorf("error in finding or creating collection %s", err)
	}
	annogrp, err := dbh.FindOrCreateCollection(
		collP.AnnoGroup,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return anns, fmt.Errorf("error in finding or creating collection %s", err)
	}
	annocvt, err := dbh.FindOrCreateCollection(
		collP.AnnoTerm,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	if err != nil {
		return anns, fmt.Errorf("error in finding or creating collection %s", err)
	}
	annov, err := dbh.FindOrCreateCollection(
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
// datasource.
func (ar *arangorepository) Clear() error {
	if err := ar.ClearAnnotations(); err != nil {
		return err
	}
	for _, c := range []driver.Collection{
		ar.onto.Term, ar.onto.Cv, ar.onto.Rel,
	} {
		if err := c.Truncate(context.Background()); err != nil {
			return fmt.Errorf("error in truncating %s", err)
		}
	}
	return ar.onto.Obog.Remove(context.Background())
}

// ClearAnnotations clears all annotations from the repository datasource.
func (ar *arangorepository) ClearAnnotations() error {
	for _, c := range []driver.Collection{
		ar.anno.annot, ar.anno.ver, ar.anno.term, ar.anno.annog,
	} {
		if err := c.Truncate(context.Background()); err != nil {
			return fmt.Errorf("error in truncating %s", err)
		}
	}
	for _, grph := range []driver.Graph{
		ar.anno.verg,
		ar.anno.annotg,
	} {
		arangoDb := ar.database.Handler()
		isok, err := arangoDb.GraphExists(context.Background(), grph.Name())
		if err != nil {
			return fmt.Errorf("error in checking existence of graph %s", err)
		}
		if !isok {
			continue
		}
		if err := grph.Remove(context.Background()); err != nil {
			return fmt.Errorf("error in removing graph %s", err)
		}
	}
	return nil
}

func DocumentsExists(c driver.Collection, ids ...string) error {
	for _, kdi := range ids {
		ok, err := c.DocumentExists(context.Background(), kdi)
		if err != nil {
			return fmt.Errorf("error in checking for existence of identifier %s %s", kdi, err)
		}
		if !ok {
			return &repo.AnnoNotFoundError{Id: kdi}
		}
	}
	return nil
}

func (ar *arangorepository) Dbh() *manager.Database {
	return ar.database
}
