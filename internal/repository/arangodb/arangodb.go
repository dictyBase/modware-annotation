package arangodb

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/go-playground/validator/v10"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type arangorepository struct {
	sess     *manager.Session
	database *manager.Database
	anno     *annoc
	onto     *ontoc
}

func NewTaggedAnnotationRepo(connP *manager.ConnectParams, collP *CollectionParams) (repo.TaggedAnnotationRepository, error) {
	ar := &arangorepository{}
	if err := validator.New().Struct(collP); err != nil {
		return ar, err
	}
	sess, db, err := manager.NewSessionDb(connP)
	if err != nil {
		return ar, err
	}
	ontoc, err := setOntologyCollection(db, collP)
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

func (ar *arangorepository) GetAnnotationById(id string) (*model.AnnoDoc, error) {
	m := &model.AnnoDoc{}
	r, err := ar.database.Get(
		fmt.Sprintf(
			annGetQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.cv.Name(),
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
			ar.onto.cv.Name(),
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
			ar.onto.cv.Name(),
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
	um, err := convToModel(i)
	if err != nil {
		return um, err
	}
	um.Ontology = m.Ontology
	um.Tag = m.Tag
	return um, nil
}

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
		map[string]interface{}{
			"is_obsolete": true,
		},
	)
	if err != nil {
		return fmt.Errorf("unable to remove annotation with id %s %s", m.Key, err)
	}
	return nil
}

func (ar *arangorepository) ListAnnotations(cursor int64, limit int64, filter string) ([]*model.AnnoDoc, error) {
	var am []*model.AnnoDoc
	var stmt string
	bindVars := map[string]interface{}{
		"@cvt_collection":   ar.onto.term.Name(),
		"@cv_collection":    ar.onto.cv.Name(),
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
	log.Printf("stmt is %s", stmt)
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

// Add a new annotations to an existing group
func (ar *arangorepository) AppendToAnnotationGroup(groupId string, idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// retrieve annotation objects for existing group
	gml, err := ar.groupID2Annotations(groupId)
	if err != nil {
		return g, err
	}
	// retrieve annotation objects for given identifiers
	ml, err := ar.getAllAnnotations(idslice...)
	if err != nil {
		return g, err
	}
	// remove duplicates
	aml := uniqueAnno(append(gml, ml...))
	// update the new group
	r, err := ar.database.DoRun(
		annGroupUpd,
		map[string]interface{}{
			"@anno_group_collection": ar.anno.annog.Name(),
			"key":                    groupId,
			"group":                  docToIds(aml),
		},
	)
	if err != nil {
		return g, fmt.Errorf("error in updating group with id %s %s", groupId, err)
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

// Delete an annotation group
func (ar *arangorepository) RemoveAnnotationGroup(groupId string) error {
	_, err := ar.anno.annog.RemoveDocument(
		context.Background(),
		groupId,
	)
	if err != nil {
		return fmt.Errorf("error in removing group with id %s %s", groupId, err)
	}
	return nil
}

// Creates a new annotation group
func (ar *arangorepository) AddAnnotationGroup(idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// check if the annotations exists
	if err := documentsExists(ar.anno.annot, idslice...); err != nil {
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

// RemoveFromAnnotationGroup remove annotations from an existing group
func (ar *arangorepository) RemoveFromAnnotationGroup(groupId string, idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// check if the group exists
	ok, err := ar.anno.annog.DocumentExists(
		context.Background(),
		groupId,
	)
	if err != nil {
		return g, fmt.Errorf("error in checking for existence of group identifier %s %s", groupId, err)
	}
	if !ok {
		return g, &repository.GroupNotFound{Id: groupId}
	}
	// retrieve all annotations ids for the group
	dbg := &model.DbGroup{}
	_, err = ar.anno.annog.ReadDocument(
		context.Background(),
		groupId,
		dbg,
	)
	if err != nil {
		return g, fmt.Errorf("error in retrieving the group %s", err)
	}
	nids := removeStringItems(dbg.Group, idslice...)
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
			"key":                    groupId,
			"group":                  nids,
		},
	)
	if err != nil {
		return g, fmt.Errorf("error in removing group members with id %s %s", groupId, err)
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

// ListAnnotationGroup provides a paginated list of annotation groups along
// with optional filtering
func (ar *arangorepository) ListAnnotationGroup(cursor, limit int64, filter string) ([]*model.AnnoGroup, error) {
	var gm []*model.AnnoGroup
	var stmt string
	if len(filter) > 0 { // filter
		// no cursor
		stmt = fmt.Sprintf(annGroupListFilterQ,
			ar.anno.annot.Name(), ar.anno.annotg.Name(), ar.onto.cv.Name(),
			filter, ar.anno.annog.Name(), ar.anno.annot.Name(),
			ar.anno.annotg.Name(), ar.onto.cv.Name(),
			limit,
		)
		if cursor != 0 { // with cursor
			stmt = fmt.Sprintf(annGroupListFilterWithCursorQ,
				ar.anno.annot.Name(), ar.anno.annotg.Name(),
				ar.onto.cv.Name(), filter,
				ar.anno.annog.Name(), ar.anno.annot.Name(),
				ar.anno.annotg.Name(), ar.onto.cv.Name(),
				cursor, limit,
			)
		}
	} else { // no filter
		// no cursor
		stmt = fmt.Sprintf(annGroupListQ,
			ar.anno.annog.Name(), ar.anno.annot.Name(),
			ar.anno.annotg.Name(), ar.onto.cv.Name(),
			limit,
		)
		if cursor != 0 { // with cursor
			stmt = fmt.Sprintf(annGroupListWithCursorQ,
				ar.anno.annog.Name(), ar.anno.annot.Name(),
				ar.anno.annotg.Name(), ar.onto.cv.Name(),
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
			"@cvterm_collection": ar.onto.term.Name(),
			"@cv_collection":     ar.onto.cv.Name(),
			"ontology":           ontology,
			"tag":                tag,
		},
	)
	if err != nil {
		return m, fmt.Errorf("error in running tag query %s", err)
	}
	if r.IsEmpty() {
		return m, &repository.AnnoTagNotFound{Tag: tag}
	}
	if err := r.Read(m); err != nil {
		return m, fmt.Errorf("error in retrieving tag %s in ontology %s %s", tag, ontology, err)
	}
	return m, nil
}

// Clear clears all annotations and related ontologies from the repository
// datasource
func (ar *arangorepository) Clear() error {
	if err := ar.anno.annot.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.ver.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.term.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.verg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.annotg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.term.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.cv.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.rel.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.onto.obog.Remove(context.Background()); err != nil {
		return err
	}
	return nil
}

// ClearAnnotations clears all annotations from the repository datasource
func (ar *arangorepository) ClearAnnotations() error {
	if err := ar.anno.annot.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.ver.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.term.Truncate(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.verg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.annotg.Remove(context.Background()); err != nil {
		return err
	}
	if err := ar.anno.annog.Remove(context.Background()); err != nil {
		return err
	}
	return nil
}
