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

const maxTransactionSize = 10000

func (ar *arangorepository) AddAnnotation(na *annotation.NewTaggedAnnotation) (*model.AnnoDoc, error) {
	mann := &model.AnnoDoc{}
	attr := na.Data.Attributes
	// check if the tag and ontology exist
	cvtid, err := ar.termID(attr.Ontology, attr.Tag)
	if err != nil {
		return mann, err
	}
	// get the tag from database
	tag, err := ar.termName(cvtid)
	if err != nil {
		return mann, err
	}
	// check if the annotation exist
	if err := ar.existAnno(attr, tag); err != nil {
		return mann, err
	}

	return ar.createAnno(
		&createParams{
			attr: attr,
			id:   cvtid,
			tag:  tag,
		},
	)
}

func (ar *arangorepository) EditAnnotation(uat *annotation.TaggedAnnotationUpdate) (*model.AnnoDoc, error) {
	mann := &model.AnnoDoc{}
	attr := uat.Data.Attributes
	rgt, err := ar.database.Get(
		fmt.Sprintf(
			annGetQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.Cv.Name(),
			uat.Data.Id,
		),
	)
	if err != nil {
		return mann, fmt.Errorf("error in fetching id %s", err)
	}
	if rgt.IsEmpty() {
		mann.NotFound = true

		return mann, &repository.AnnoNotFoundError{Id: uat.Data.Id}
	}
	if err := rgt.Read(mann); err != nil {
		return mann, fmt.Errorf("error in reading to struct %s", err)
	}
	// create annotation document
	bindParams := []interface{}{
		ar.anno.annot.Name(),
		ar.anno.term.Name(),
		ar.anno.ver.Name(),
		attr.Value,
		attr.EditableValue,
		attr.CreatedBy,
		mann.EnrtyId,
		mann.Rank,
		mann.Version + 1,
		mann.CvtId,
		mann.ID.String(),
	}
	dbh := ar.database.Handler()
	idt, err := dbh.Transaction(
		context.Background(),
		annVerInstFn,
		&driver.TransactionOptions{
			WriteCollections: []string{
				ar.anno.annot.Name(),
				ar.anno.term.Name(),
				ar.anno.ver.Name(),
			},
			Params:             bindParams,
			MaxTransactionSize: maxTransactionSize,
		})
	if err != nil {
		return mann, fmt.Errorf("error in running transaction %s", err)
	}
	umd, err := model.ConvToModel(idt)
	if err != nil {
		return umd, fmt.Errorf("error in converting model struct %s", err)
	}
	umd.Ontology = mann.Ontology
	umd.Tag = mann.Tag

	return umd, nil
}

// Creates a new annotation group.
func (ar *arangorepository) AddAnnotationGroup(idslice ...string) (*model.AnnoGroup, error) {
	grp := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return grp, errors.New("need at least more than one entry to form a group")
	}
	// check if the annotations exists
	if err := DocumentsExists(ar.anno.annot, idslice...); err != nil {
		return grp, err
	}
	// retrieve all annotations objects
	mla, err := ar.getAllAnnotations(idslice...)
	if err != nil {
		return grp, err
	}
	dbg := &model.DbGroup{}
	rdn, err := ar.database.DoRun(
		annGroupInst,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"group":                  idslice,
		},
	)
	if err != nil {
		return grp, fmt.Errorf("error in creating group %s", err)
	}
	if err := rdn.Read(dbg); err != nil {
		return grp, fmt.Errorf("error in reading to struct %s", err)
	}
	grp.CreatedAt = dbg.CreatedAt
	grp.UpdatedAt = dbg.UpdatedAt
	grp.GroupId = dbg.GroupId
	grp.AnnoDocs = mla

	return grp, nil
}

// Delete an annotation group.
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

// Add a new annotations to an existing group.
func (ar *arangorepository) AppendToAnnotationGroup(groupID string, idslice ...string) (*model.AnnoGroup, error) {
	grp := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return grp, errors.New("need at least more than one entry to form a group")
	}
	// retrieve annotation objects for existing group
	gml, err := ar.groupID2Annotations(groupID)
	if err != nil {
		return grp, err
	}
	// retrieve annotation objects for given identifiers
	ml, err := ar.getAllAnnotations(idslice...)
	if err != nil {
		return grp, err
	}
	// remove duplicates
	aml := model.UniqueModel(append(gml, ml...))
	// update the new group
	rdn, err := ar.database.DoRun(
		annGroupUpd,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"key":                    groupID,
			"group":                  model.DocToIds(aml),
		},
	)
	if err != nil {
		return grp, fmt.Errorf("error in updating group with id %s %s", groupID, err)
	}
	dbg := &model.DbGroup{}
	if err := rdn.Read(dbg); err != nil {
		return grp, fmt.Errorf("error in reading to struct %s", err)
	}
	grp.CreatedAt = dbg.CreatedAt
	grp.UpdatedAt = dbg.UpdatedAt
	grp.GroupId = dbg.GroupId
	grp.AnnoDocs = aml

	return grp, nil
}

func (ar *arangorepository) createAnno(params *createParams) (*model.AnnoDoc, error) {
	mann := &model.AnnoDoc{}
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
		return mann, fmt.Errorf("error in running query %s", err)
	}
	if rins.IsEmpty() {
		return mann, fmt.Errorf("error in returning newly created document")
	}
	if err := rins.Read(mann); err != nil {
		return mann, fmt.Errorf("error in reading to struct %s", err)
	}
	mann.Tag = params.tag
	mann.Ontology = attr.Ontology

	return mann, nil
}
