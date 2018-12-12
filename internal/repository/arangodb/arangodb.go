package arangodb

import (
	"context"
	"fmt"

	validator "gopkg.in/go-playground/validator.v9"

	driver "github.com/arangodb/go-driver"
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
	// AnnoTerm is the edge collection annotation with a named tag(ontology
	// term)
	AnnoTerm string `validate:"required"`
	// AnnoVersion is the edge collection for connecting different versions of
	// annotations
	AnnoVersion string `validate:"required"`
	// AnnoTagGraph is the named graph for connecting annotation
	// with the ontology
	AnnoTagGraph string `validate:"required"`
	// AnnoVerGraph is the name graph for connecting different
	// version of annotations
	AnnoVerGraph string `validate:"required"`
}

type annoc struct {
	annot  driver.Collection
	term   driver.Collection
	ver    driver.Collection
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
		return m, nil
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
		return m, nil
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
		return m, nil
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
	bindVars := map[string]interface{}{
		"@anno_collection": ar.anno.term.Name(),
		"@cv_collection":   ar.onto.cv.Name(),
		"graph":            ar.anno.annotg.Name(),
		"limit":            limit + 1,
	}
	if cursor == 0 { // no cursor so return first set of result
		stmt = annListQ
	} else {
		bindVars["next_cursor"] = cursor
		stmt = annListWithCursorQ
	}
	rs, err := ar.database.SearchRows(stmt, bindVars)
	if err != nil {
		return am, err
	}
	if rs.IsEmpty() {
		return am, nil
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
	return nil
}
