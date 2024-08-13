package arangodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (ar *arangorepository) GetAnnotationByID(
	annoid string,
) (*model.AnnoDoc, error) {
	model := &model.AnnoDoc{}
	res, err := ar.database.Get(
		fmt.Sprintf(
			annGetQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.Cv.Name(),
			annoid,
		),
	)
	if err != nil {
		return model, fmt.Errorf("error in fetching id %s", err)
	}
	if res.IsEmpty() {
		model.NotFound = true

		return model, &repository.AnnoNotFoundError{Id: annoid}
	}
	if err := res.Read(model); err != nil {
		return model, fmt.Errorf("error in reading data to structure %s", err)
	}

	return model, nil
}

func (ar *arangorepository) GetAnnotationByEntry(
	req *annotation.EntryAnnotationRequest,
) (*model.AnnoDoc, error) {
	mann := &model.AnnoDoc{}
	res, err := ar.database.Get(
		fmt.Sprintf(
			annGetByEntryQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.Cv.Name(),
			req.EntryId,
			req.Rank,
			req.IsObsolete,
			req.Tag,
			req.Ontology,
		),
	)
	if err != nil {
		return mann, fmt.Errorf("error in fetching id %s", err)
	}
	if res.IsEmpty() {
		mann.NotFound = true

		return mann, &repository.AnnoNotFoundError{Id: req.EntryId}
	}
	if err := res.Read(mann); err != nil {
		return mann, fmt.Errorf("error in reading data to structure %s", err)
	}

	return mann, nil
}

func (ar *arangorepository) ListAnnotations(
	cursor int64,
	limit int64,
	filter string,
) ([]*model.AnnoDoc, error) {
	annoModel := make([]*model.AnnoDoc, 0)
	bindVars := map[string]interface{}{
		"@cvt_collection":   ar.onto.Term.Name(),
		"@cv_collection":    ar.onto.Cv.Name(),
		"anno_cvterm_graph": ar.anno.annotg.Name(),
		"limit":             limit + 1,
	}
	if cursor != 0 {
		bindVars["cursor"] = cursor
	}
	stmt := getListAnnoStatement(filter, cursor)
	res, err := ar.database.SearchRows(stmt, bindVars)
	if err != nil {
		return annoModel, fmt.Errorf("error in searching rows %s", err)
	}
	if res.IsEmpty() {
		return annoModel, &repository.AnnoListNotFoundError{}
	}
	for res.Scan() {
		amodel := &model.AnnoDoc{}
		if err := res.Read(amodel); err != nil {
			return annoModel, fmt.Errorf(
				"error in reading data to structure %s",
				err,
			)
		}
		annoModel = append(annoModel, amodel)
	}

	return annoModel, nil
}

// Retrieves an annotation group.
func (ar *arangorepository) GetAnnotationGroup(
	groupID string,
) (*model.AnnoGroup, error) {
	grp := &model.AnnoGroup{}
	ann, err := ar.groupID2Annotations(groupID)
	if err != nil {
		return grp, err
	}
	// retrieve group information
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupID,
		dbg,
	)
	if err != nil {
		return grp, fmt.Errorf("error in retrieving the group %s", err)
	}
	grp.CreatedAt = dbg.CreatedAt
	grp.UpdatedAt = dbg.UpdatedAt
	grp.GroupId = dbg.GroupId
	grp.AnnoDocs = ann

	return grp, nil
}

// ListAnnotationGroup provides a paginated list of annotation groups along
// with optional filtering.
func (ar *arangorepository) ListAnnotationGroup(
	cursor, limit int64,
	filter string,
) ([]*model.AnnoGroup, error) {
	var agrp []*model.AnnoGroup
	var stmt string
	if len(filter) > 0 { // filter
		// no cursor
		stmt = fmt.Sprintf(annGroupListFilterQ,
			ar.anno.annot.Name(), ar.anno.annotg.Name(), ar.onto.Cv.Name(),
			filter, ar.anno.annog.Name(), ar.anno.annot.Name(),
			ar.anno.annotg.Name(), ar.onto.Cv.Name(),
			limit,
		)
		if cursor != 0 { // with cursor
			stmt = fmt.Sprintf(annGroupListFilterWithCursorQ,
				ar.anno.annot.Name(), ar.anno.annotg.Name(),
				ar.onto.Cv.Name(), filter,
				ar.anno.annog.Name(), ar.anno.annot.Name(),
				ar.anno.annotg.Name(), ar.onto.Cv.Name(),
				cursor, limit,
			)
		}
	} else { // no filter
		// no cursor
		stmt = fmt.Sprintf(annGroupListQ,
			ar.anno.annog.Name(), ar.anno.annot.Name(),
			ar.anno.annotg.Name(), ar.onto.Cv.Name(),
			limit,
		)
		if cursor != 0 { // with cursor
			stmt = fmt.Sprintf(annGroupListWithCursorQ,
				ar.anno.annog.Name(), ar.anno.annot.Name(),
				ar.anno.annotg.Name(), ar.onto.Cv.Name(),
				cursor, limit,
			)
		}
	}
	res, err := ar.database.Search(stmt)
	if err != nil {
		return agrp, fmt.Errorf("error in searching rows %s", err)
	}
	if res.IsEmpty() {
		return agrp, &repository.AnnoGroupListNotFoundError{}
	}
	for res.Scan() {
		amodel := &model.AnnoGroup{}
		if err := res.Read(amodel); err != nil {
			return agrp, fmt.Errorf(
				"error in reading data to structure %s",
				err,
			)
		}
		agrp = append(agrp, amodel)
	}

	return agrp, nil
}

// GetAnnotationTag retrieves tag information.
func (ar *arangorepository) GetAnnotationTag(
	tag, ontology string,
) (*model.AnnoTag, error) {
	annoModel := new(model.AnnoTag)
	res, err := ar.database.GetRow(
		tagGetQ,
		map[string]interface{}{
			"@cvterm_collection": ar.onto.Term.Name(),
			"@cv_collection":     ar.onto.Cv.Name(),
			"ontology":           ontology,
			"tag":                tag,
		})
	if err != nil {
		return annoModel, fmt.Errorf("error in running tag query %s", err)
	}
	if res.IsEmpty() {
		return annoModel, &repository.AnnoTagNotFoundError{Tag: tag}
	}
	if err := res.Read(annoModel); err != nil {
		return annoModel,
			fmt.Errorf(
				"error in retrieving tag %s in ontology %s %s",
				tag, ontology, err,
			)
	}

	return annoModel, nil
}

func (ar *arangorepository) existAnno(
	attr *annotation.NewTaggedAnnotationAttributes,
	tag string,
) error {
	count, err := ar.database.CountWithParams(annExistQ, map[string]interface{}{
		"@anno_collection":  ar.anno.annot.Name(),
		"@cv_collection":    ar.onto.Cv.Name(),
		"anno_cvterm_graph": ar.anno.annotg.Name(),
		"entry_id":          attr.EntryId,
		"rank":              attr.Rank,
		"ontology":          attr.Ontology,
		"tag":               tag,
	})
	if err != nil {
		return fmt.Errorf("error in count query %s", err)
	}
	if count > 0 {
		return errors.New("error in creating, annotation already exists")
	}

	return nil
}

func (ar *arangorepository) groupID2Annotations(
	groupID string,
) ([]*model.AnnoDoc, error) {
	var annoModel []*model.AnnoDoc
	// check if the group exists
	isOk, err := ar.anno.annog.DocumentExists(context.Background(), groupID)
	if err != nil {
		return annoModel,
			fmt.Errorf(
				"error in checking for existence of group identifier %s %s",
				groupID,
				err,
			)
	}
	if !isOk {
		return annoModel, &repository.GroupNotFoundError{Id: groupID}
	}
	// retrieve group object
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupID, dbg,
	)
	if err != nil {
		return annoModel, fmt.Errorf("error in retrieving the group %s", err)
	}
	// retrieve the model objects for the existing annotations
	return ar.getAllAnnotations(dbg.Group...)
}

func (ar *arangorepository) getAllAnnotations(
	ids ...string,
) ([]*model.AnnoDoc, error) {
	annoModel := make([]*model.AnnoDoc, 0)
	for _, k := range ids {
		res, err := ar.database.Get(
			fmt.Sprintf(
				annGetQ, ar.anno.annot.Name(),
				ar.anno.annotg.Name(), ar.onto.Cv.Name(), k,
			),
		)
		if err != nil {
			return annoModel, fmt.Errorf("error in fetching id %s", err)
		}
		amodel := &model.AnnoDoc{}
		if err := res.Read(amodel); err != nil {
			return annoModel, fmt.Errorf(
				"error in reading data to structure %s",
				err,
			)
		}
		annoModel = append(annoModel, amodel)
	}

	return annoModel, nil
}

func getListAnnoStatement(filter string, cursor int64) string {
	var stmt string
	switch {
	case len(filter) > 0 && cursor == 0:
		stmt = fmt.Sprintf(annListFilterQ, filter)
	case len(filter) > 0 && cursor != 0:
		stmt = fmt.Sprintf(annListFilterWithCursorQ, filter)
	case len(filter) == 0 && cursor == 0:
		stmt = annListQ
	case len(filter) == 0 && cursor != 0:
		stmt = annListWithCursorQ
	}

	return stmt
}
