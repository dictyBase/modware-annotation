package arangodb

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/dictyBase/arangomanager/testarango"
	ontostorage "github.com/dictyBase/go-obograph/storage"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/assert"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-obograph/graph"
	araobo "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/model"

	manager "github.com/dictyBase/arangomanager"
)

var ahost, aport, auser, apass, adb string
var tags = []string{
	"private note",
	"name description",
	"name",
	"curator note",
	"description",
	"public note",
	"status",
	"curation",
	"product",
	"gene product",
	"curation status",
	"curator",
	"note",
}

var ddbg = []string{"DDB_G0286429", "DDB_G0294491"}

func toTimestamp(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func getOntoParams() *ontoarango.CollectionParams {
	return &ontoarango.CollectionParams{
		GraphInfo:    "cv",
		OboGraph:     "obograph",
		Relationship: "cvterm_relationship",
		Term:         "cvterm",
	}
}

func getConnectParams() *manager.ConnectParams {
	arPort, _ := strconv.Atoi(aport)
	return &manager.ConnectParams{
		User:     auser,
		Pass:     apass,
		Database: adb,
		Host:     ahost,
		Port:     arPort,
		Istls:    false,
	}
}

func getCollectionParams() *CollectionParams {
	return &CollectionParams{
		Annotation:   "annotation",
		AnnoTerm:     "annotation_cvterm",
		AnnoVersion:  "annotation_version",
		AnnoTagGraph: "annotation_tag",
		AnnoVerGraph: "annotation_history",
		AnnoGroup:    "annotation_group",
		AnnoIndexes:  []string{"entry_id"},
	}
}

func loadAnnotationObo() error {
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("unable to get current dir %s", err)
	}
	r, err := os.Open(
		filepath.Join(
			filepath.Dir(dir), "testdata", "dicty_annotation.json",
		),
	)
	if err != nil {
		return err
	}
	defer r.Close()
	g, err := graph.BuildGraph(r)
	if err != nil {
		return fmt.Errorf("error in building graph %s", err)
	}
	connP := getConnectParams()
	ds, err := araobo.NewDataSource(
		&araobo.ConnectParams{
			User:     connP.User,
			Pass:     connP.Pass,
			Host:     connP.Host,
			Database: connP.Database,
			Port:     connP.Port,
			Istls:    connP.Istls,
		}, getOntoParams(),
	)
	if err != nil {
		return err
	}
	if ds.ExistsOboGraph(g) {
		return errors.New("dicty_annotation already exist, needs a cleanp!!!!")
	}
	return saveExistentTestGraph(ds, g)
}

func saveExistentTestGraph(ds ontostorage.DataSource, g graph.OboGraph) error {
	if err := ds.SaveOboGraphInfo(g); err != nil {
		return fmt.Errorf("error in saving graph %s", err)
	}
	if _, err := ds.SaveTerms(g); err != nil {
		return fmt.Errorf("error in saving terms %s", err)
	}
	if _, err := ds.SaveRelationships(g); err != nil {
		return fmt.Errorf("error in saving relationships %s", err)
	}
	return nil
}

func newTestAnnoWithTagAndOnto(onto, tag string) *annotation.NewTaggedAnnotation {
	return &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:         "developmentally regulated gene",
				EditableValue: "developmentally regulated gene",
				CreatedBy:     "siddbasu@gmail.com",
				Tag:           tag,
				Ontology:      onto,
				EntryId:       "DDB_G0267474",
				Rank:          0,
			},
		},
	}
}

func newTestTaggedAnnotationWithParams(tag, entryId string) *annotation.NewTaggedAnnotation {
	return &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:         "developmentally regulated gene",
				EditableValue: "developmentally regulated gene",
				CreatedBy:     "siddbasu@gmail.com",
				Tag:           tag,
				Ontology:      "dicty_annotation",
				EntryId:       entryId,
				Rank:          0,
			},
		},
	}
}

func newTestTaggedAnnotation() *annotation.NewTaggedAnnotation {
	return &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:         "developmentally regulated gene",
				EditableValue: "developmentally regulated gene",
				CreatedBy:     "siddbasu@gmail.com",
				Tag:           "description",
				Ontology:      "dicty_annotation",
				EntryId:       "DDB_G0267474",
				Rank:          0,
			},
		},
	}
}

func newTestTaggedAnnotationsListForFiltering(num int) []*annotation.NewTaggedAnnotation {
	var nal []*annotation.NewTaggedAnnotation
	value := fmt.Sprintf("cool gene %s", tags[0])
	for z := 0; z < num/2; z++ {
		nal = append(nal, &annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         value,
					EditableValue: value,
					CreatedBy:     "sidd@gmail.com",
					Tag:           tags[0],
					Ontology:      "dicty_annotation",
					EntryId:       ddbg[0],
					Rank:          int64(z),
				},
			},
		})
	}
	value = fmt.Sprintf("cool gene %s", tags[1])
	for y := num / 2; y < num; y++ {
		nal = append(nal, &annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         value,
					EditableValue: value,
					CreatedBy:     "basu@gmail.com",
					Tag:           tags[1],
					Ontology:      "dicty_annotation",
					EntryId:       ddbg[1],
					Rank:          int64(y),
				},
			},
		})
	}
	return nal
}

func newTestTaggedAnnotationsList(num int) []*annotation.NewTaggedAnnotation {
	var nal []*annotation.NewTaggedAnnotation
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	max := 800000
	min := 300000
	for i := 0; i < num; i++ {
		value := fmt.Sprintf("cool gene %s", tags[r.Intn(len(tags)-1)])
		nal = append(nal, &annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         value,
					EditableValue: value,
					CreatedBy:     "siddbasu@gmail.com",
					Tag:           tags[r.Intn(len(tags)-1)],
					Ontology:      "dicty_annotation",
					EntryId:       fmt.Sprintf("DDB_G0%d", r.Intn(max-min)+min),
					Rank:          0,
				},
			},
		})
	}
	return nal
}

func TestMain(m *testing.M) {
	ta, err := testarango.NewTestArangoFromEnv(true)
	if err != nil {
		log.Fatalf("unable to construct new TestArango instance %s", err)
	}
	dbh, err := ta.DB(ta.Database)
	if err != nil {
		log.Fatalf("unable to get database %s", err)
	}
	auser = ta.User
	apass = ta.Pass
	ahost = ta.Host
	aport = strconv.Itoa(ta.Port)
	adb = ta.Database
	if err := loadAnnotationObo(); err != nil {
		log.Fatalf("error in loading test annotation obograph %s", err)
	}
	code := m.Run()
	if err := dbh.Drop(); err != nil {
		log.Printf("error in dropping database %s", err)
	}
	os.Exit(code)
}

func TestAddAnnotation(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	nta := newTestAnnoWithTagAndOnto("dicty_annotation", "curator")
	m, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.False(m.IsObsolete, "new tagged annotation should not be obsolete")
	assert.Equal(m.Value, nta.Data.Attributes.Value, "should match the value")
	assert.Equal(m.CreatedBy, nta.Data.Attributes.CreatedBy, "should match created_by")
	assert.Equal(m.EnrtyId, nta.Data.Attributes.EntryId, "should match entry identifier")
	assert.Equal(m.Rank, nta.Data.Attributes.Rank, "should match the rank")
	assert.Equal(m.Ontology, nta.Data.Attributes.Ontology, "should match ontology name")
	assert.Equal(m.Tag, nta.Data.Attributes.Tag, "should match the ontology tag")

	// error in case of existing record
	_, err = anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error for existing annotation")
	assert.Regexp(
		regexp.MustCompile("already exists"),
		err.Error(),
		"error should have existence of annotation",
	)

	nta.Data.Attributes.Tag = "respiration"
	_, err = anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error in case of non-existent ontology and tag")
	assert.Regexp(
		regexp.MustCompile("respiration"),
		err.Error(),
		"error should contain the non-existent tag name",
	)
	nta = newTestAnnoWithTagAndOnto("caboose", "description")
	_, err = anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error in case of non-existent ontology and tag")
	assert.Regexp(
		regexp.MustCompile("caboose"),
		err.Error(),
		"error should contain the non-existent ontology",
	)

	// use synonym for tag name
	nta = newTestAnnoWithTagAndOnto("dicty_annotation", "summary")
	m2, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.False(m2.IsObsolete, "new tagged annotation should not be obsolete")
	assert.Equal(m2.Value, nta.Data.Attributes.Value, "should match the value")
	assert.Equal(m2.CreatedBy, nta.Data.Attributes.CreatedBy, "should match created_by")
	assert.Equal(m2.EnrtyId, nta.Data.Attributes.EntryId, "should match entry identifier")
	assert.Equal(m2.Rank, nta.Data.Attributes.Rank, "should match the rank")
	assert.Equal(m2.Ontology, nta.Data.Attributes.Ontology, "should match ontology name")
	assert.Equal(m2.Tag, "description", "should match the ontology tag")

	nta = newTestAnnoWithTagAndOnto("dicty_annotation", "decreased 3',5'-cyclic-GMP phosphodiesterase activity")
	m3, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m3.Ontology, nta.Data.Attributes.Ontology, "should match ontology name")
	assert.Equal(m3.Tag, nta.Data.Attributes.Tag, "should match the tag")
}

func TestGetAnnotationByEntry(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	nta := newTestTaggedAnnotation()
	_, err = anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nta2 := newTestTaggedAnnotationWithParams("curation", "DDB_G0287317")
	_, err = anrepo.AddAnnotation(nta2)
	assert.NoErrorf(err, "expect no error, received %s", err)
	m, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta.Data.Attributes.Tag,
		EntryId:  nta.Data.Attributes.EntryId,
		Ontology: nta.Data.Attributes.Ontology,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m.Rank, int64(0), "should match rank 0")
	assert.Equal(m.EnrtyId, nta.Data.Attributes.EntryId, "should match the entry id")

	m2, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta2.Data.Attributes.Tag,
		EntryId:  nta2.Data.Attributes.EntryId,
		Ontology: nta2.Data.Attributes.Ontology,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m2.EnrtyId, nta2.Data.Attributes.EntryId, "should match the entry id")
	assert.Equal(m2.Tag, nta2.Data.Attributes.Tag, "should match the tag")

	em, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta2.Data.Attributes.Tag,
		Ontology: nta2.Data.Attributes.Ontology,
		EntryId:  "DDB_G0277853",
	})
	assert.Errorf(err, "expect %s error, received nothing", err)
	assert.True(
		repository.IsAnnotationNotFound(err),
		"the entry should not exist",
	)
	assert.True(em.NotFound, "the entry should not exist")
}

func TestGetAnnotationById(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	nta := newTestTaggedAnnotation()
	m, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nta2 := newTestTaggedAnnotationWithParams("curation", "DDB_G0287317")
	m2, err := anrepo.AddAnnotation(nta2)
	assert.NoErrorf(err, "expect no error, received %s", err)
	em, err := anrepo.GetAnnotationById(m.Key)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m.EnrtyId, em.EnrtyId, "should match entry identifier")
	assert.Equal(m.Ontology, em.Ontology, "should match ontology")
	assert.Equal(m.Tag, em.Tag, "should match tag")
	assert.Equal(m.Key, em.Key, "should match the identifier")
	assert.Equal(m.Value, em.Value, "should match the value")
	assert.True(m.CreatedAt.Equal(em.CreatedAt), "should match created time of annotation")
	assert.Equal(m.Rank, em.Rank, "should match rank")

	em2, err := anrepo.GetAnnotationById(m2.Key)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m2.EnrtyId, em2.EnrtyId, "should match entry identifier")

	ne, err := anrepo.GetAnnotationById("9999999")
	assert.Errorf(err, "expected %s error, received nothing", err)
	assert.True(
		repository.IsAnnotationNotFound(err),
		"entry should not exist",
	)
	assert.True(ne.NotFound, "entry should not exist")
}

func TestRemoveAnnotation(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	nta := newTestTaggedAnnotation()
	m, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nta2 := newTestTaggedAnnotationWithParams("curation", "DDB_G0287317")
	m2, err := anrepo.AddAnnotation(nta2)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotation(m.Key, true)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotation(m2.Key, false)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotation(m2.Key, false)
	assert.True(assert.Error(err), "should return error")
	assert.Contains(err.Error(), "obsolete", "should contain obsolete message")
}

func TestEditAnnotation(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	nta := newTestTaggedAnnotation()
	m, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	ua := &annotation.TaggedAnnotationUpdate{
		Data: &annotation.TaggedAnnotationUpdate_Data{
			Type: "annotations",
			Id:   m.Key,
			Attributes: &annotation.TaggedAnnotationUpdateAttributes{
				Value:         "updated gene description",
				EditableValue: "updated gene description",
				CreatedBy:     "basu@gmail.com",
			},
		},
	}
	um, err := anrepo.EditAnnotation(ua)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m.Version+1, um.Version, "version should be incremented by 1")
	assert.NotEqual(ua.Data.Id, um.Key, "identifier should not match")
	assert.Equal(ua.Data.Attributes.Value, um.Value, "should matches the value")
	assert.Equal(ua.Data.Attributes.CreatedBy, um.CreatedBy, "should matches created by")
}

func TestListAnnoFilter(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsListForFiltering(20)
	for _, anno := range tal {
		_, err := anrepo.AddAnnotation(anno)
		assert.NoErrorf(err, "expect no error, received %s", err)
	}
	filterOne := `FILTER ann.entry_id == 'DDB_G0286429'
				  AND cvt.label == 'private note'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	ml, err := anrepo.ListAnnotations(0, 4, filterOne)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml, 5, "should have 5 annotations")
	for _, m := range ml {
		assert.Equal(m.CreatedBy, "sidd@gmail.com", "should match created by")
		assert.Equal(m.Tag, tags[0], "should match the tag")
		assert.Equal(m.EnrtyId, ddbg[0], "should match the entry id")
	}
	ml2, err := anrepo.ListAnnotations(
		toTimestamp(ml[len(ml)-1].CreatedAt),
		4,
		filterOne,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml2, 5, "should have five annotations")
	assert.Exactly(ml[len(ml)-1], ml2[0], "should have identical model objects")

	ml3, err := anrepo.ListAnnotations(
		toTimestamp(ml2[len(ml2)-1].CreatedAt),
		4,
		filterOne,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml3, 2, "should have two annotations")
	assert.Exactly(ml2[len(ml2)-1], ml3[0], "should have identical model objects")

	filterTwo := `FILTER ann.entry_id == 'DDB_G0294491'
				  AND cvt.label == 'name description'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	ml4, err := anrepo.ListAnnotations(0, 6, filterTwo)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml4, 7, "should have 7 annotations")
	for _, m := range ml4 {
		assert.Equal(m.CreatedBy, "basu@gmail.com", "should match created by")
		assert.Equal(m.Tag, tags[1], "should match the tag")
		assert.Equal(m.EnrtyId, ddbg[1], "should match the entry id")
	}
	ml5, err := anrepo.ListAnnotations(
		toTimestamp(ml4[len(ml4)-1].CreatedAt),
		4,
		filterTwo,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml5, 4, "should have four annotations")
	assert.Exactly(ml4[len(ml4)-1], ml5[0], "should have identical model objects")

	testModelListSort(ml, t)
	testModelListSort(ml2, t)
	testModelListSort(ml3, t)
	testModelListSort(ml4, t)
	testModelListSort(ml5, t)

	_, err = anrepo.ListAnnotations(0, 4, "FILTER ann.entry_id == 'jumbo'")
	assert.Error(err, "expect error")
	assert.True(repository.IsAnnotationListNotFound(err), "expect no annotation list found")
}

func TestListAnnotations(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(15)
	for _, anno := range tal {
		_, err := anrepo.AddAnnotation(anno)
		assert.NoErrorf(err, "expect no error, received %s", err)
	}
	ml, err := anrepo.ListAnnotations(0, 4, "")
	if err != nil {
		assert.NoErrorf(err, "expect no error, received %s", err)
	}
	assert.Len(ml, 5, "should have 5 annotations")
	for i, m := range ml {
		assert.Contains(m.Value, "cool gene", "should contain the phrase cool gene")
		assert.Equal(tal[i].Data.Attributes.CreatedBy, m.CreatedBy, "should match created by")
		assert.Subset(tags, []string{m.Tag}, "should contain the tag in the slice")
		assert.Equal(tal[i].Data.Attributes.Ontology, m.Ontology, "should match the ontology")
		assert.Contains(m.EnrtyId, "DDB_G0", "should contain the DDB_G0 in entry id")
		assert.Equal(int(m.Rank), 0, "should match the zero rank")
	}
	ml2, err := anrepo.ListAnnotations(
		toTimestamp(ml[len(ml)-1].CreatedAt),
		4,
		"",
	)
	if err != nil {
		t.Fatalf("error in fetching annotation list %s", err)
	}
	assert.Len(ml2, 5, "should have five annotations")
	assert.Exactly(ml[len(ml)-1], ml2[0], "should have identical model objects")

	ml3, err := anrepo.ListAnnotations(
		toTimestamp(ml2[len(ml2)-1].CreatedAt),
		4,
		"",
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml3, 5, "should have five annotations")
	assert.Exactly(ml2[len(ml2)-1], ml3[0], "should have identical model objects")

	ml4, err := anrepo.ListAnnotations(
		toTimestamp(ml3[len(ml3)-1].CreatedAt),
		4,
		"",
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml4, 3, "should have three annotations")
	assert.Exactly(ml3[len(ml3)-1], ml4[0], "should have identical model objects")
	testModelListSort(ml, t)
	testModelListSort(ml2, t)
	testModelListSort(ml3, t)
	testModelListSort(ml4, t)
}

func TestAddAnnotationGroup(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(8)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	ids := testModelMaptoID(ml, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Lenf(g.AnnoDocs, len(ids), "should have %d annotations", len(ids))
}

func TestGetAnnotationGroup(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(4)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	ids := testModelMaptoID(ml, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	eg, err := anrepo.GetAnnotationGroup(g.GroupId)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.ElementsMatch(
		testModelMaptoID(g.AnnoDocs, model2IdCallback),
		testModelMaptoID(eg.AnnoDocs, model2IdCallback),
		"expected identical annotation identifiers in the list",
	)
}

func TestAppendToAnntationGroup(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(7)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	ids := testModelMaptoID(ml[:4], model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nids := testModelMaptoID(ml[4:], model2IdCallback)
	eg, err := anrepo.AppendToAnnotationGroup(g.GroupId, nids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.ElementsMatch(
		testModelMaptoID(eg.AnnoDocs, model2IdCallback),
		append(ids, nids...),
		"expected identical annotation identifiers after appending to the group",
	)
}

func TestRemoveAnnotationGroup(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(7)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	ids := testModelMaptoID(ml, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotationGroup(g.GroupId)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotationGroup(g.GroupId)
	assert.True(assert.Error(err), "should return error")
	assert.Contains(
		err.Error(),
		"removing group",
		"should contain removing group phrase",
	)
}

func TestRemoveFromAnnotationGroup(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(9)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	ids := testModelMaptoID(ml, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	eg, err := anrepo.RemoveFromAnnotationGroup(g.GroupId, ids[:5]...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.ElementsMatch(
		testModelMaptoID(g.AnnoDocs, model2IdCallback),
		ids,
		"should match no of documents",
	)
	assert.ElementsMatch(
		ids[5:],
		testModelMaptoID(eg.AnnoDocs, model2IdCallback),
		"expected identical annotation identifiers after removing from the group",
	)
}

func TestListAnnGrFilter(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsListForFiltering(20)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	j := 5
	for i := 0; j <= len(ml); i += 5 {
		ids := testModelMaptoID(ml[i:j], model2IdCallback)
		_, err := anrepo.AddAnnotationGroup(ids...)
		assert.NoErrorf(err, "expect no error, received %s", err)
		j += 5
	}
	assert.NoErrorf(err, "expect no error, received %s", err)
	filterOne := `FILTER ann.entry_id == 'DDB_G0286429'
				  AND cvt.label == 'private note'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	egl, err := anrepo.ListAnnotationGroup(0, 10, filterOne)
	assert.NoErrorf(err, "expect no error, received %s", err)
	testGroupMember(egl, 2, 0, "sidd@gmail.com", t)
	filterTwo := `FILTER ann.entry_id == 'DDB_G0294491'
				  AND cvt.label == 'name description'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	egl2, err := anrepo.ListAnnotationGroup(0, 10, filterTwo)
	assert.NoErrorf(err, "expect no error, received %s", err)
	testGroupMember(egl2, 2, 1, "basu@gmail.com", t)
	filterThree := `FILTER cv.metadata.namespace == 'dicty_annotation'`
	egl3, err := anrepo.ListAnnotationGroup(0, 2, filterThree)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(egl3, 2, "should have two groups")
	for _, g := range egl3 {
		assert.Len(g.AnnoDocs, 5, "should have 5 annotations in each group")
	}
	egl4, err := anrepo.ListAnnotationGroup(
		toTimestamp(egl3[len(egl3)-1].CreatedAt),
		4,
		filterThree,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(egl4, 3, "should have three groups")
	for _, g := range egl4 {
		assert.Len(g.AnnoDocs, 5, "should have 5 annotations in each group")
	}
	_, err = anrepo.ListAnnotationGroup(0, 4, "FILTER ann.entry_id == 'jumbo'")
	assert.Error(err, "expect error")
	assert.True(repository.IsAnnotationGroupListNotFound(err), "expect no annotation group to be found")
}

func TestListAnnotationGroup(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	tal := newTestTaggedAnnotationsList(60)
	var ml []*model.AnnoDoc
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		ml = append(ml, m)
	}
	j := 5
	for i := 0; j <= len(ml); i += 5 {
		ids := testModelMaptoID(ml[i:j], model2IdCallback)
		_, err := anrepo.AddAnnotationGroup(ids...)
		assert.NoErrorf(err, "expect no error, received %s", err)
		j += 5
	}
	egl, err := anrepo.ListAnnotationGroup(0, 4, "")
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(egl, 4, "should have 4 groups")
	for _, g := range egl {
		assert.Len(g.AnnoDocs, 5, "should have 5 annotations in each group")
	}
	egl2, err := anrepo.ListAnnotationGroup(
		toTimestamp(egl[len(egl)-1].CreatedAt),
		6,
		"",
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(egl2, 6, "should have 6 groups")
	for _, g := range egl2 {
		assert.Len(g.AnnoDocs, 5, "should have 5 annotations in each group")
	}
	assert.Exactly(
		egl[len(egl)-1],
		egl2[0],
		"should have identical model objects",
	)
	egl3, err := anrepo.ListAnnotationGroup(
		toTimestamp(egl2[len(egl2)-1].CreatedAt),
		6,
		"",
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(egl3, 4, "should have 4 groups")
	for _, g := range egl3 {
		assert.Len(g.AnnoDocs, 5, "should have 5 annotations in each group")
	}
	assert.Exactly(
		egl2[len(egl2)-1],
		egl3[0],
		"should have identical model objects",
	)
}

func TestGetAnnotationTag(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	for _, tag := range tags[:6] {
		m, err := anrepo.GetAnnotationTag(tag, "dicty_annotation")
		assert.NoErrorf(err, "expect no error from fetching %s tag", tag)
		assert.Equal(m.Name, tag, "should match tag name")
		assert.Equal(m.Ontology, "dicty_annotation", "should match ontology")
		assert.Falsef(m.IsObsolete, "tag %s should not be obsolete", tag)
	}
	_, err = anrepo.GetAnnotationTag("yadayada", "dicty_annotation")
	assert.Error(err, "expect error from non-existent tag")
	assert.True(repository.IsAnnoTagNotFound(err), "should be an error for non-existent tag")
}

func TestCollectionIndexErrors(t *testing.T) {
	assert := assert.New(t)
	_, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		&CollectionParams{
			Annotation:   "annotation",
			AnnoTerm:     "annotation_cvterm",
			AnnoVersion:  "annotation_version",
			AnnoTagGraph: "annotation_tag",
			AnnoVerGraph: "annotation_history",
			AnnoGroup:    "annotation_group",
			AnnoIndexes:  []string{},
		},
		getOntoParams(),
	)
	assert.Error(err, "should receive error if creating repo with no indexes")
}

func testModelListSort(m []*model.AnnoDoc, t *testing.T) {
	assert := assert.New(t)
	it, err := NewModelAnnoDocPairWiseIterator(m)
	assert.NoErrorf(err, "expect no error, received %s", err)
	for it.NextModelAnnoDocPair() {
		cm, nm := it.ModelAnnoDocPair()
		assert.Truef(
			nm.CreatedAt.Before(cm.CreatedAt),
			"date %s should be before %s",
			nm.CreatedAt.String(),
			cm.CreatedAt.String(),
		)
	}
}

func testGroupMember(gl []*model.AnnoGroup, count, idx int, email string, t *testing.T) {
	assert := assert.New(t)
	assert.Lenf(gl, count, "should have %d groups", count)
	for _, g := range gl {
		assert.Len(g.AnnoDocs, 5, "should have 5 annotations in each group")
		for _, d := range g.AnnoDocs {
			assert.Equalf(d.Tag, tags[idx], "should have %d as the tag", idx)
			assert.Equalf(d.CreatedBy, email, "should be created by %s", email)
			assert.Equal(d.Ontology, "dicty_annotation", "should have dicty_annotation ontology")
			assert.Equalf(d.EnrtyId, ddbg[idx], "should have %d as entry id", idx)
		}
	}
}

func testModelMaptoID(am []*model.AnnoDoc, fn func(m *model.AnnoDoc) string) []string {
	var s []string
	for _, m := range am {
		s = append(s, fn(m))
	}
	return s
}

func model2IdCallback(m *model.AnnoDoc) string {
	return m.Key
}

func annoCleanUp(anrepo repository.TaggedAnnotationRepository, t *testing.T) {
	assert := assert.New(t)
	if err := anrepo.ClearAnnotations(); err != nil {
		assert.FailNow(err.Error(), "error in pruning test annotations")
	}
}
