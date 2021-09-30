package arangodb

import (
	"context"
	"errors"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (ar *arangorepository) RemoveAnnotation(id string, purge bool) error {
	m := &model.AnnoDoc{}
	_, err := ar.anno.annot.ReadDocument(context.Background(), id, m)
	if err != nil {
		if driver.IsNotFound(err) {
			return &repository.AnnoNotFound{Id: id}
		}
		return err
	}
	if m.IsObsolete {
		return fmt.Errorf("annotation with id %s has already been obsolete", m.Key)
	}
	if purge {
		if _, err := ar.anno.annot.RemoveDocument(context.Background(), m.Key); err != nil {
			return fmt.Errorf("unable to purge annotation with id %s %s", m.Key, err)
		}
		return nil
	}
	_, err = ar.anno.annot.UpdateDocument(
		context.Background(),
		m.Key,
		map[string]interface{}{"is_obsolete": true},
	)
	if err != nil {
		return fmt.Errorf("unable to remove annotation with id %s %s", m.Key, err)
	}
	return nil
}

// RemoveFromAnnotationGroup remove annotations from an existing group
func (ar *arangorepository) RemoveFromAnnotationGroup(groupID string, idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// check if the group exists
	ok, err := ar.anno.annog.DocumentExists(
		context.Background(), groupID,
	)
	if err != nil {
		return g,
			fmt.Errorf(
				"error in checking for existence of group identifier %s %s",
				groupID, err,
			)
	}
	if !ok {
		return g, &repository.GroupNotFound{Id: groupID}
	}
	// retrieve all annotations ids for the group
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupID, dbg,
	)
	if err != nil {
		return g, fmt.Errorf("error in retrieving the group %s", err)
	}
	nids := collection.RemoveStringItems(dbg.Group, idslice...)
	// retrieve the annotation objects
	ml, err := ar.getAllAnnotations(nids...)
	if err != nil {
		return g, err
	}
	// update the new group
	r, err := ar.database.DoRun(
		annGroupUpd,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"key":                    groupID,
			"group":                  nids,
		})
	if err != nil {
		return g,
			fmt.Errorf(
				"error in removing group members with id %s %s",
				groupID, err,
			)
	}
	ndbg := &model.DbGroup{}
	if err := r.Read(ndbg); err != nil {
		return g, err
	}
	g.CreatedAt = ndbg.CreatedAt
	g.UpdatedAt = ndbg.UpdatedAt
	g.GroupId = ndbg.GroupId
	g.AnnoDocs = ml
	return g, nil
}
