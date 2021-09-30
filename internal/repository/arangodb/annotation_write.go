package arangodb

import (
	"context"
	"errors"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

func (ar *arangorepository) AddAnnotation(na *annotation.NewTaggedAnnotation) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	attr := na.Data.Attributes
	// check if the tag and ontology exist
	cvtid, err := ar.termID(attr.Ontology, attr.Tag)
	if err != nil {
		return m, err
	}
	// get the tag from database
	tag, err := ar.termName(cvtid)
	if err != nil {
		return m, err
	}
	// check if the annotation exist
	if err := ar.existAnno(attr, tag); err != nil {
		return m, err
	}
	return ar.createAnno(
		&createParams{
			attr: attr,
			id:   cvtid,
			tag:  tag,
		},
	)
}

func (ar *arangorepository) EditAnnotation(ua *annotation.TaggedAnnotationUpdate) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	attr := ua.Data.Attributes
	r, err := ar.database.Get(
		fmt.Sprintf(
			annGetQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.Cv.Name(),
			ua.Data.Id,
		),
	)
	if err != nil {
		return m, err
	}
	if r.IsEmpty() {
		m.NotFound = true
		return m, &repository.AnnoNotFound{Id: ua.Data.Id}
	}
	if err := r.Read(m); err != nil {
		return m, err
	}
	// create annotation document
	bindParams := []interface{}{
		ar.anno.annot.Name(),
		ar.anno.term.Name(),
		ar.anno.ver.Name(),
		attr.Value,
		attr.EditableValue,
		attr.CreatedBy,
		m.EnrtyId,
		m.Rank,
		m.Version + 1,
		m.CvtId,
		m.ID.String(),
	}
	dbh := ar.database.Handler()
	i, err := dbh.Transaction(
		context.Background(),
		annVerInstFn,
		&driver.TransactionOptions{
			WriteCollections: []string{
				ar.anno.annot.Name(),
				ar.anno.term.Name(),
				ar.anno.ver.Name(),
			},
			Params:             bindParams,
			MaxTransactionSize: 10000,
		})
	if err != nil {
		return m, err
	}
	um, err := model.ConvToModel(i)
	if err != nil {
		return um, err
	}
	um.Ontology = m.Ontology
	um.Tag = m.Tag
	return um, nil
}

// Creates a new annotation group
func (ar *arangorepository) AddAnnotationGroup(idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// check if the annotations exists
	if err := DocumentsExists(ar.anno.annot, idslice...); err != nil {
		return g, err
	}
	// retrieve all annotations objects
	ml, err := ar.getAllAnnotations(idslice...)
	if err != nil {
		return g, err
	}
	dbg := &model.DbGroup{}
	r, err := ar.database.DoRun(
		annGroupInst,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"group":                  idslice,
		},
	)
	if err != nil {
		return g, fmt.Errorf("error in creating group %s", err)
	}
	if err := r.Read(dbg); err != nil {
		return g, err
	}
	g.CreatedAt = dbg.CreatedAt
	g.UpdatedAt = dbg.UpdatedAt
	g.GroupId = dbg.GroupId
	g.AnnoDocs = ml
	return g, nil
}

// Delete an annotation group
func (ar *arangorepository) RemoveAnnotationGroup(groupID string) error {
	_, err := ar.anno.annog.RemoveDocument(
		context.Background(),
		groupID,
	)
	if err != nil {
		return fmt.Errorf("error in removing group with id %s %s", groupID, err)
	}
	return nil
}

// Add a new annotations to an existing group
func (ar *arangorepository) AppendToAnnotationGroup(groupID string, idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// retrieve annotation objects for existing group
	gml, err := ar.groupID2Annotations(groupID)
	if err != nil {
		return g, err
	}
	// retrieve annotation objects for given identifiers
	ml, err := ar.getAllAnnotations(idslice...)
	if err != nil {
		return g, err
	}
	// remove duplicates
	aml := model.UniqueModel(append(gml, ml...))
	// update the new group
	r, err := ar.database.DoRun(
		annGroupUpd,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"key":                    groupID,
			"group":                  model.DocToIds(aml),
		},
	)
	if err != nil {
		return g, fmt.Errorf("error in updating group with id %s %s", groupID, err)
	}
	dbg := &model.DbGroup{}
	if err := r.Read(dbg); err != nil {
		return g, err
	}
	g.CreatedAt = dbg.CreatedAt
	g.UpdatedAt = dbg.UpdatedAt
	g.GroupId = dbg.GroupId
	g.AnnoDocs = aml
	return g, nil
}

func (ar *arangorepository) createAnno(params *createParams) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	attr := params.attr
	rins, err := ar.database.DoRun(
		annInst, map[string]interface{}{
			"@anno_collection":    ar.anno.annot.Name(),
			"@anno_cv_collection": ar.anno.term.Name(),
			"editable_value":      attr.EditableValue,
			"created_by":          attr.CreatedBy,
			"entry_id":            attr.EntryId,
			"rank":                attr.Rank,
			"value":               attr.Value,
			"to":                  params.id,
			"version":             1,
		})
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
