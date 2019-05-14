package arangodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	validator "gopkg.in/go-playground/validator.v9"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/apihelpers/aphcollection"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

// CollectionParams are the arangodb collections required for storing
// annotations
type CollectionParams struct {
	// Term is the collection for storing ontology term
	Term string `validate:"required"`
	// Relationship is the edge collection for storing term relationship
	Relationship string `validate:"required"`
	// GraphInfo is the collection for storing ontology metadata
	GraphInfo string `validate:"required"`
	// OboGraph is the named graph for connecting term and relationship collections
	OboGraph string `validate:"required"`
	// Annotation is the collection for storing annotation
	Annotation string `validate:"required"`
	// AnnoGroup is the collection for grouping annotations
	AnnoGroup string `validate:"required"`
	// AnnoTerm is the edge collection annotation with a named tag(ontology
	// term)
	AnnoTerm string `validate:"required"`
	// AnnoVersion is the edge collection for connecting different versions of
	// annotations
	AnnoVersion string `validate:"required"`
	// AnnoTagGraph is the named graph for connecting annotation
	// with the ontology
	AnnoTagGraph string `validate:"required"`
	// AnnoVerGraph is the named graph for connecting different
	// version of annotations
	AnnoVerGraph string `validate:"required"`
}

type annoc struct {
	annot  driver.Collection
	term   driver.Collection
	ver    driver.Collection
	annog  driver.Collection
	verg   driver.Graph
	annotg driver.Graph
}

type ontoc struct {
	term driver.Collection
	rel  driver.Collection
	cv   driver.Collection
	obog driver.Graph
}

type arangorepository struct {
	sess     *manager.Session
	database *manager.Database
	anno     *annoc
	onto     *ontoc
}

func NewTaggedAnnotationRepo(connP *manager.ConnectParams, collP *CollectionParams) (repository.TaggedAnnotationRepository, error) {
	ar := &arangorepository{}
	validate := validator.New()
	if err := validate.Struct(collP); err != nil {
		return ar, err
	}
	sess, db, err := manager.NewSessionDb(connP)
	if err != nil {
		return ar, err
	}
	ar.sess = sess
	ar.database = db
	termc, err := db.FindOrCreateCollection(collP.Term, &driver.CreateCollectionOptions{})
	if err != nil {
		return ar, err
	}
	relc, err := db.FindOrCreateCollection(
		collP.Relationship,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	if err != nil {
		return ar, err
	}
	graphc, err := db.FindOrCreateCollection(
		collP.GraphInfo,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return ar, err
	}
	obog, err := db.FindOrCreateGraph(
		collP.OboGraph,
		[]driver.EdgeDefinition{
			driver.EdgeDefinition{
				Collection: relc.Name(),
				From:       []string{termc.Name()},
				To:         []string{termc.Name()},
			},
		},
	)
	if err != nil {
		return ar, err
	}
	ar.onto = &ontoc{
		term: termc,
		rel:  relc,
		cv:   graphc,
		obog: obog,
	}
	anno, err := db.FindOrCreateCollection(
		collP.Annotation,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return ar, err
	}
	annogrp, err := db.FindOrCreateCollection(
		collP.AnnoGroup,
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		return ar, err
	}
	annocvt, err := db.FindOrCreateCollection(
		collP.AnnoTerm,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	if err != nil {
		return ar, err
	}
	annov, err := db.FindOrCreateCollection(
		collP.AnnoVersion,
		&driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge},
	)
	if err != nil {
		return ar, err
	}
	verg, err := db.FindOrCreateGraph(
		collP.AnnoVerGraph,
		[]driver.EdgeDefinition{
			driver.EdgeDefinition{
				Collection: annov.Name(),
				From:       []string{anno.Name()},
				To:         []string{anno.Name()},
			},
		},
	)
	if err != nil {
		return ar, err
	}
	annotg, err := db.FindOrCreateGraph(
		collP.AnnoTagGraph,
		[]driver.EdgeDefinition{
			driver.EdgeDefinition{
				Collection: annocvt.Name(),
				From:       []string{anno.Name()},
				To:         []string{termc.Name()},
			},
		},
	)
	if err != nil {
		return ar, err
	}
	ar.anno = &annoc{
		annot:  anno,
		term:   annocvt,
		ver:    annov,
		verg:   verg,
		annotg: annotg,
		annog:  annogrp,
	}
	return ar, nil
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
		return m, &repository.AnnoNotFound{id}
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
		return m, &repository.AnnoNotFound{req.EntryId}
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
	bindVars := map[string]interface{}{
		"@cv_collection":     ar.onto.cv.Name(),
		"@cvterm_collection": ar.onto.term.Name(),
		"ontology":           attr.Ontology,
		"tag":                attr.Tag,
	}
	r, err := ar.database.GetRow(annExistTagQ, bindVars)
	if err != nil {
		return m, fmt.Errorf("error in running obograph retrieving query %s", err)
	}
	if r.IsEmpty() {
		m.NotFound = true
		return m, fmt.Errorf("ontology %s and tag %s does not exist %s", attr.Ontology, attr.Tag, err)
	}
	var cvtid string
	if err := r.Read(&cvtid); err != nil {
		return m, fmt.Errorf("error in retrieving obograph id %s", err)
	}
	// check if the annotation exist
	count, err := ar.database.Count(
		fmt.Sprintf(
			annExistQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.cv.Name(),
			attr.EntryId,
			attr.Rank,
			attr.Tag,
			attr.Ontology,
		),
	)
	if err != nil {
		return m, fmt.Errorf("error in count query %s", err)
	}
	if count > 0 {
		return m, fmt.Errorf("error in creating, annotation already exists")
	}
	// create annotation document
	bindVarsc := map[string]interface{}{
		"@anno_collection":    ar.anno.annot.Name(),
		"@anno_cv_collection": ar.anno.term.Name(),
		"value":               attr.Value,
		"editable_value":      attr.EditableValue,
		"created_by":          attr.CreatedBy,
		"entry_id":            attr.EntryId,
		"rank":                attr.Rank,
		"version":             1,
		"to":                  cvtid,
	}
	rins, err := ar.database.DoRun(annInst, bindVarsc)
	if err != nil {
		return m, err
	}
	if r.IsEmpty() {
		m.NotFound = true
		return m, fmt.Errorf("error in returning newly created document")
	}
	if err := rins.Read(m); err != nil {
		return m, err
	}
	m.Tag = attr.Tag
	m.Ontology = attr.Ontology
	return m, nil
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
		return m, &repository.AnnoNotFound{ua.Data.Id}
	}
	if err := r.Read(m); err != nil {
		return m, err
	}
	// create annotation document
	bindVars := map[string]interface{}{
		"@anno_collection":     ar.anno.annot.Name(),
		"@anno_cv_collection":  ar.anno.term.Name(),
		"@anno_ver_collection": ar.anno.ver.Name(),
		"value":                attr.Value,
		"editable_value":       attr.EditableValue,
		"created_by":           attr.CreatedBy,
		"entry_id":             m.EnrtyId,
		"rank":                 m.Rank,
		"version":              m.Version + 1,
		"to":                   m.CvtId,
		"prev":                 m.ID.String(),
	}
	um := &model.AnnoDoc{}
	rupd, err := ar.database.DoRun(annVerInst, bindVars)
	if err != nil {
		return um, err
	}
	if err := rupd.Read(um); err != nil {
		return um, err
	}
	um.Ontology = m.Ontology
	um.Tag = m.Tag
	return um, nil
}

func (ar *arangorepository) RemoveAnnotation(id string) error {
	m := &model.AnnoDoc{}
	_, err := ar.anno.annot.ReadDocument(context.Background(), id, m)
	if err != nil {
		if driver.IsNotFound(err) {
			return fmt.Errorf("annotation record with id %s does not exist %s", id, err)
		}
		return err
	}
	if m.IsObsolete {
		return fmt.Errorf("annotation with id %s has already been obsolete", m.Key)
	}
	_, err = ar.anno.annot.UpdateDocument(
		context.Background(),
		m.Key,
		map[string]interface{}{
			"is_obsolete": true,
		},
	)
	if err != nil {
		return fmt.Errorf("unable to remove annotation with id %s", m.Key)
	}
	return nil
}

func (ar *arangorepository) ListAnnotations(cursor int64, limit int64) ([]*model.AnnoDoc, error) {
	var am []*model.AnnoDoc
	var stmt string
	if cursor == 0 { // no cursor so return first set of result
		stmt = fmt.Sprintf(
			annListQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.cv.Name(),
			limit+1,
		)
	} else {
		stmt = fmt.Sprintf(
			annListWithCursorQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.cv.Name(),
			cursor,
			limit+1,
		)
	}
	rs, err := ar.database.Search(stmt)
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
	ml, err := ar.groupId2Annotations(groupId)
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

// GetAnnotationGroupByEntry retrieves an annotation group associated with an entry
func (ar *arangorepository) GetAnnotationGroupByEntry(req *annotation.EntryAnnotationRequest) (*model.AnnoGroup, error) {
	rg := &model.ReaderGroup{}
	rs, err := ar.database.Search(
		fmt.Sprintf(
			annGetGroupByEntryQ,
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.cv.Name(),
			req.EntryId,
			req.Rank,
			req.IsObsolete,
			req.Tag,
			req.Ontology,
			ar.anno.annog.Name(),
			ar.anno.annot.Name(),
			ar.anno.annot.Name(),
			ar.anno.annotg.Name(),
			ar.onto.cv.Name(),
		),
	)
	if err != nil {
		return rg.EmptyExchangeModel(), err
	}
	if rs.IsEmpty() {
		return rg.EmptyExchangeModel(), &repository.GroupNotFound{}
	}
	for rs.Scan() {
		m := rg.EmptyExchangeModel()
		if err := rs.Read(m); err != nil {
			return g, err
		}
		rg.AddMembers(m)
	}
	return rg.ToExchangeModel(), nil
}

// Add a new annotations to an existing group
func (ar *arangorepository) AppendToAnnotationGroup(groupId string, idslice ...string) (*model.AnnoGroup, error) {
	g := &model.AnnoGroup{}
	if len(idslice) <= 1 {
		return g, errors.New("need at least more than one entry to form a group")
	}
	// retrieve annotation objects for existing group
	eml, err := ar.groupId2Anno(groupId)
	if err != nil {
		return g, err
	}
	// check if the annotations exists
	if err := documentsExists(ar.anno.annot, idslice); err != nil {
		return g, err
	}
	// retrieve annotation objects for given identifiers
	// remove duplicates
	rg.AddMemberIds(aphcollection.UniqueString(append(rg.MemberIds(), idslice...))...)
	rg.AddMembers(uniqueAnno(rg.Members())...)
	// update the new group
	_, err = ar.anno.annog.UpdateDocument(
		context.Background(),
		groupId,
		rg.ToStorageModel(),
	)
	if err != nil {
		return rg.EmptyExchangeModel(), fmt.Errorf("error in updating the group id %s %s", groupId, err)
	}
	return rg.ToExchangeModel(), nil
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
		return m, errors.New("need at least more than one entry to form a group")
	}
	// check if the annotations exists
	if err := documentsExists(ar.anno.annot, idslice); err != nil {
		return g, err
	}
	// retrieve all annotations objects
	ml, err := ar.getAllAnnotations(idslice)
	if err != nil {
		return g, err
	}
	dbg := &model.DbGroup{}
	_, err := ar.anno.annog.CreateDocument(
		driver.WithReturnNew(context.Background(), dbg),
		&DbGroup{
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Group:     idslice,
		},
	)
	if err != nil {
		return g, fmt.Errorf("error in creating group %s", err)
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
		return g, &repository.GroupNotFound{groupId}
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
	dbg.Group = aphcollection.Remove(dbg.Group, idslice...)
	// retrieve the annotation objects
	ml, err := ar.getAllAnnotations(dbg.Group...)
	if err != nil {
		return g, err
	}
	// update the new group
	dbg.UpdatedAt = time.Now()
	_, err = ar.anno.annog.UpdateDocument(
		context.Background(),
		groupId,
		dbg,
	)
	if err != nil {
		return g, fmt.Errorf("error in updating the group id %s %s", groupId, err)
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
	if len(filter) > 0 { // filter string is present
		if cursor == 0 { // no cursor, return first set of result
			stmt = fmt.Sprintf(
				annGroupListFilterQ,
				ar.anno.annot.Name(),
				ar.anno.annot.Name(),
				ar.onto.cv.Name(),
				filter,
				ar.anno.annog.Name(),
				ar.anno.annot.Name(),
				ar.anno.annot.Name(),
				ar.onto.cv.Name(),
				limit,
			)
		} else {
			stmt = fmt.Sprintf(
				annGroupListFilterWithCursorQ,
				ar.anno.annot.Name(),
				ar.anno.annot.Name(),
				ar.onto.cv.Name(),
				filter,
				ar.anno.annog.Name(),
				ar.anno.annot.Name(),
				ar.anno.annot.Name(),
				ar.onto.cv.Name(),
				cursor,
				limit,
			)
		}
	} else { // no filter
		if cursor == 0 { // no cursor
			stmt = fmt.Sprintf(
				annGroupListQ,
				ar.anno.annog.Name(),
				ar.anno.annot.Name(),
				ar.anno.annotg.Name(),
				ar.onto.cv.Name(),
				limit,
			)

		} else { // with cursor
			stmt = fmt.Sprintf(
				annGroupListWithCursorQ,
				ar.anno.annog.Name(),
				ar.anno.annot.Name(),
				ar.anno.annotg.Name(),
				ar.onto.cv.Name(),
				cursor,
				limit,
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

func (ar *arangorepository) groupId2Annotations(groupId string) ([]*model.AnnoDoc, error) {
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
		return ml, &repository.GroupNotFound{groupId}
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
			return false, fmt.Errorf("error in checking for existence of identifier %s %s", k, err)
		}
		if !ok {
			return &repository.AnnoNotFound{k}
		}
	}
	return nil
}
