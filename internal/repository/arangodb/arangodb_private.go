package arangodb

import (
	"context"
	"fmt"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (ar *arangorepository) createAnno(params *createParams) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	attr := params.attr
	bindVarsc := map[string]interface{}{
		"@anno_collection":    ar.anno.annot.Name(),
		"@anno_cv_collection": ar.anno.term.Name(),
		"value":               attr.Value,
		"editable_value":      attr.EditableValue,
		"created_by":          attr.CreatedBy,
		"entry_id":            attr.EntryId,
		"rank":                attr.Rank,
		"version":             1,
		"to":                  params.id,
	}
	rins, err := ar.database.DoRun(annInst, bindVarsc)
	if err != nil {
		return m, err
	}
	if rins.IsEmpty() {
		return m, fmt.Errorf("error in returning newly created document")
	}
	if err := rins.Read(m); err != nil {
		return m, err
	}
	m.Tag = params.tag
	m.Ontology = attr.Ontology
	return m, nil
}

func (ar *arangorepository) existAnno(attr *annotation.NewTaggedAnnotationAttributes, tag string) error {
	bindVars := map[string]interface{}{
		"@anno_collection":  ar.anno.annot.Name(),
		"@cv_collection":    ar.onto.cv.Name(),
		"anno_cvterm_graph": ar.anno.annotg.Name(),
		"entry_id":          attr.EntryId,
		"rank":              attr.Rank,
		"ontology":          attr.Ontology,
		"tag":               tag,
	}
	count, err := ar.database.CountWithParams(annExistQ, bindVars)
	if err != nil {
		return fmt.Errorf("error in count query %s", err)
	}
	if count > 0 {
		return fmt.Errorf("error in creating, annotation already exists")
	}
	return nil
}

func (ar *arangorepository) termID(onto, term string) (string, error) {
	var id string
	bindVars := map[string]interface{}{
		"@cv_collection":     ar.onto.cv.Name(),
		"@cvterm_collection": ar.onto.term.Name(),
		"ontology":           onto,
		"tag":                term,
	}
	r, err := ar.database.GetRow(annExistTagQ, bindVars)
	if err != nil {
		return id, fmt.Errorf("error in running obograph retrieving query %s", err)
	}
	if r.IsEmpty() {
		return id, fmt.Errorf("ontology %s and tag %s does not exist", onto, term)
	}
	if err := r.Read(&id); err != nil {
		return id, fmt.Errorf("error in retrieving obograph id %s", err)
	}
	return id, nil
}

func (ar *arangorepository) groupID2Annotations(groupId string) ([]*model.AnnoDoc, error) {
	var ml []*model.AnnoDoc
	// check if the group exists
	ok, err := ar.anno.annog.DocumentExists(
		context.Background(),
		groupId,
	)
	if err != nil {
		return ml, fmt.Errorf("error in checking for existence of group identifier %s %s", groupId, err)
	}
	if !ok {
		return ml, &repository.GroupNotFound{Id: groupId}
	}
	// retrieve group object
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupId,
		dbg,
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
				annGetQ,
				ar.anno.annot.Name(),
				ar.anno.annotg.Name(),
				ar.onto.cv.Name(),
				k,
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

func (ar *arangorepository) termName(id string) (string, error) {
	var name string
	cvtr, err := ar.database.GetRow(
		cvtID2LblQ,
		map[string]interface{}{
			"@cvterm_collection": ar.onto.term.Name(),
			"id":                 id,
		},
	)
	if err != nil {
		return name, fmt.Errorf("error in running tag retrieving query %s", err)
	}
	if cvtr.IsEmpty() {
		return name, fmt.Errorf("cvterm id %s does not exist", id)
	}
	if err := cvtr.Read(&name); err != nil {
		return name, fmt.Errorf("error in retrieving tag %s", err)
	}
	return name, nil
}
