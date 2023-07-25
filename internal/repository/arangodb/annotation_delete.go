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
	manno := &model.AnnoDoc{}
	_, err := ar.anno.annot.ReadDocument(context.Background(), id, manno)
	if err != nil {
		if driver.IsNotFoundGeneral(err) {
			return &repository.AnnoNotFoundError{Id: id}
		}

		return fmt.Errorf("error in reading document %s", err)
	}
	if manno.IsObsolete {
		return fmt.Errorf(
			"annotation with id %s has already been obsolete",
			manno.Key,
		)
	}
	if purge {
		if _, err := ar.anno.annot.RemoveDocument(context.Background(), manno.Key); err != nil {
			return fmt.Errorf(
				"unable to purge annotation with id %s %s",
				manno.Key,
				err,
			)
		}

		return nil
	}
	_, err = ar.anno.annot.UpdateDocument(
		context.Background(),
		manno.Key,
		map[string]interface{}{"is_obsolete": true},
	)
	if err != nil {
		return fmt.Errorf(
			"unable to remove annotation with id %s %s",
			manno.Key,
			err,
		)
	}

	return nil
}

// RemoveFromAnnotationGroup remove annotations from an existing group.
func (ar *arangorepository) RemoveFromAnnotationGroup(
	groupID string,
	idslice ...string,
) (*model.AnnoGroup, error) {
	manno := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return manno, errors.New(
			"need at least more than one entry to form a group",
		)
	}
	// check if the group exists
	isok, err := ar.anno.annog.DocumentExists(
		context.Background(), groupID,
	)
	if err != nil {
		return manno, fmt.Errorf(
			"error in checking for existence of group identifier %s %s",
			groupID, err,
		)
	}
	if !isok {
		return manno, &repository.GroupNotFoundError{Id: groupID}
	}
	// retrieve all annotations ids for the group
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupID, dbg,
	)
	if err != nil {
		return manno, fmt.Errorf("error in retrieving the group %s", err)
	}
	nids := collection.RemoveStringItems(dbg.Group, idslice...)
	mla, err := ar.getAllAnnotations(nids...)
	if err != nil {
		return manno, err
	}
	// update the new group
	res, err := ar.database.DoRun(
		annGroupUpd,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"key":                    groupID,
			"group":                  nids,
		})
	if err != nil {
		return manno, fmt.Errorf(
			"error in removing group members with id %s %s",
			groupID, err,
		)
	}
	ndbg := &model.DbGroup{}
	if err := res.Read(ndbg); err != nil {
		return manno, fmt.Errorf("error in reading data into struct %s", err)
	}
	manno.CreatedAt = ndbg.CreatedAt
	manno.UpdatedAt = ndbg.UpdatedAt
	manno.GroupId = ndbg.GroupId
	manno.AnnoDocs = mla

	return manno, nil
}
