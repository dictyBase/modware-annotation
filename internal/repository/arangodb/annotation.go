package arangodb

import (
	"context"
	"fmt"
	"time"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/thoas/go-funk"
)

type annoc struct {
	annot  driver.Collection
	term   driver.Collection
	ver    driver.Collection
	annog  driver.Collection
	verg   driver.Graph
	annotg driver.Graph
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

func setAnnotationCollection(db *manager.Database, onto *ontoc, collP *CollectionParams) (*annoc, error) {
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
				To:         []string{onto.term.Name()},
			},
		},
	)
	ac.verg = verg
	ac.annotg = annotg
	_, _, err = db.EnsurePersistentIndex(ac.annot.Name(), collP.AnnoIndexes, &driver.EnsurePersistentIndexOptions{})
	if err != nil {
		return ac, err
	}
	return ac, err
}

func uniqueAnno(a []*model.AnnoDoc) []*model.AnnoDoc {
	var ml []*model.AnnoDoc
	h := make(map[string]int)
	for _, m := range a {
		if _, ok := h[m.Key]; ok {
			continue
		}
		ml = append(ml, m)
		h[m.Key] = 1
	}
	return ml
}

func documentsExists(c driver.Collection, ids ...string) error {
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

func docToIds(ml []*model.AnnoDoc) []string {
	var s []string
	for _, m := range ml {
		s = append(s, m.Key)
	}
	return s
}

func convToModel(i interface{}) (*model.AnnoDoc, error) {
	c := i.(map[string]interface{})
	m := &model.AnnoDoc{
		Value:         c["value"].(string),
		EditableValue: c["editable_value"].(string),
		CreatedBy:     c["created_by"].(string),
		EnrtyId:       c["entry_id"].(string),
		Rank:          int64(c["rank"].(float64)),
		IsObsolete:    c["is_obsolete"].(bool),
		Version:       int64(c["version"].(float64)),
	}
	dstr := c["created_at"].(string)
	t, err := time.Parse(time.RFC3339, dstr)
	if err != nil {
		return m, err
	}
	m.CreatedAt = t
	m.DocumentMeta.Key = c["_key"].(string)
	m.DocumentMeta.Rev = c["_rev"].(string)
	return m, nil
}

// removeStringItems removes elements from a that are present in
// items
func removeStringItems(a []string, items ...string) []string {
	var s []string
	for _, v := range a {
		if !funk.ContainsString(items, v) {
			s = append(s, v)
		}
	}
	return s
}
