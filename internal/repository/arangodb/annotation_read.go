package arangodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (ar *arangorepository) GetAnnotationByID(id string) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	r, err := ar.database.Get(
		fmt.Sprintf(
			annGetQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.Cv.Name(),
			id,
		),
	)
	if err != nil {
		return m, err
	}
	if r.IsEmpty() {
		m.NotFound = true
		return m, &repository.AnnoNotFound{Id: id}
	}
	if err := r.Read(m); err != nil {
		return m, err
	}
	return m, nil
}

func (ar *arangorepository) GetAnnotationByEntry(req *annotation.EntryAnnotationRequest) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	r, err := ar.database.Get(
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
		return m, err
	}
	if r.IsEmpty() {
		m.NotFound = true
		return m, &repository.AnnoNotFound{Id: req.EntryId}
	}
	if err := r.Read(m); err != nil {
		return m, err
	}
	return m, nil
}

func (ar *arangorepository) ListAnnotations(cursor int64, limit int64, filter string) ([]*model.AnnoDoc, error) {
	var am []*model.AnnoDoc
	var stmt string
	bindVars := map[string]interface{}{
		"@cvt_collection":   ar.onto.Term.Name(),
		"@cv_collection":    ar.onto.Cv.Name(),
		"anno_cvterm_graph": ar.anno.annotg.Name(),
		"limit":             limit + 1,
	}
	if len(filter) > 0 { // filter string is present
		if cursor == 0 { // no cursor, return first set of result
			stmt = fmt.Sprintf(annListFilterQ, filter)
		} else {
			stmt = fmt.Sprintf(annListFilterWithCursorQ, filter)
			bindVars["cursor"] = cursor
		}
	} else {
		if cursor == 0 {
			stmt = annListQ
		} else {
			stmt = annListWithCursorQ
			bindVars["cursor"] = cursor
		}
	}
	rs, err := ar.database.SearchRows(stmt, bindVars)
	if err != nil {
		return am, err
	}
	if rs.IsEmpty() {
		return am, &repository.AnnoListNotFound{}
	}
	for rs.Scan() {
		m := &model.AnnoDoc{}
		if err := rs.Read(m); err != nil {
			return am, err
		}
		am = append(am, m)
	}
	return am, nil
}

// Retrieves an annotation group
func (ar *arangorepository) GetAnnotationGroup(groupId string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	ml, err := ar.groupID2Annotations(groupId)
	if err != nil {
		return g, err
	}
	// retrieve group information
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupId,
		dbg,
	)
	if err != nil {
		return g, fmt.Errorf("error in retrieving the group %s", err)
	}
	g.CreatedAt = dbg.CreatedAt
	g.UpdatedAt = dbg.UpdatedAt
	g.GroupId = dbg.GroupId
	g.AnnoDocs = ml
	return g, nil
}

// ListAnnotationGroup provides a paginated list of annotation groups along
// with optional filtering
func (ar *arangorepository) ListAnnotationGroup(cursor, limit int64, filter string) ([]*model.AnnoGroup, error) {
	var gm []*model.AnnoGroup
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
	rs, err := ar.database.Search(stmt)
	if err != nil {
		return gm, err
	}
	if rs.IsEmpty() {
		return gm, &repository.AnnoGroupListNotFound{}
	}
	for rs.Scan() {
		m := &model.AnnoGroup{}
		if err := rs.Read(m); err != nil {
			return gm, err
		}
		gm = append(gm, m)
	}
	return gm, nil
}

// GetAnnotationTag retrieves tag information
func (ar *arangorepository) GetAnnotationTag(tag, ontology string) (*model.AnnoTag, error) {
	m := new(model.AnnoTag)
	r, err := ar.database.GetRow(
		tagGetQ,
		map[string]interface{}{
			"@cvterm_collection": ar.onto.Term.Name(),
			"@cv_collection":     ar.onto.Cv.Name(),
			"ontology":           ontology,
			"tag":                tag,
		})
	if err != nil {
		return m, fmt.Errorf("error in running tag query %s", err)
	}
	if r.IsEmpty() {
		return m, &repository.AnnoTagNotFound{Tag: tag}
	}
	if err := r.Read(m); err != nil {
		return m,
			fmt.Errorf(
				"error in retrieving tag %s in ontology %s %s",
				tag, ontology, err,
			)
	}
	return m, nil
}

func (ar *arangorepository) existAnno(attr *annotation.NewTaggedAnnotationAttributes, tag string) error {
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

func (ar *arangorepository) groupID2Annotations(groupID string) ([]*model.AnnoDoc, error) {
	var ml []*model.AnnoDoc
	// check if the group exists
	ok, err := ar.anno.annog.DocumentExists(context.Background(), groupID)
	if err != nil {
		return ml,
			fmt.Errorf("error in checking for existence of group identifier %s %s",
				groupID, err,
			)
	}
	if !ok {
		return ml, &repository.GroupNotFound{Id: groupID}
	}
	// retrieve group object
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupID, dbg,
	)
	if err != nil {
		return ml, fmt.Errorf("error in retrieving the group %s", err)
	}
	// retrieve the model objects for the existing annotations
	return ar.getAllAnnotations(dbg.Group...)
}

func (ar *arangorepository) getAllAnnotations(ids ...string) ([]*model.AnnoDoc, error) {
	var ml []*model.AnnoDoc
	for _, k := range ids {
		r, err := ar.database.Get(
			fmt.Sprintf(
				annGetQ, ar.anno.annot.Name(),
				ar.anno.annotg.Name(), ar.onto.Cv.Name(), k,
			),
		)
		if err != nil {
			return ml, err
		}
		m := &model.AnnoDoc{}
		if err := r.Read(m); err != nil {
			return ml, err
		}
		ml = append(ml, m)
	}
	return ml, nil
}
