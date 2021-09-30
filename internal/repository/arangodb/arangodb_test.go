package arangodb

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dictyBase/arangomanager/testarango"
	ontostorage "github.com/dictyBase/go-obograph/storage"
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

func getOntoParams() *araobo.CollectionParams {
	return &araobo.CollectionParams{
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

func newTestTaggedAnnotationWithParams(tag, entryID string) *annotation.NewTaggedAnnotation {
	return &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:         "developmentally regulated gene",
				EditableValue: "developmentally regulated gene",
				CreatedBy:     "siddbasu@gmail.com",
				Tag:           tag,
				Ontology:      "dicty_annotation",
				EntryId:       entryID,
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

func TestLoadOboJSON(t *testing.T) {
	assert := assert.New(t)
	anrepo, err := NewTaggedAnnotationRepo(
		getConnectParams(),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer annoCleanUp(anrepo, t)
	fh, err := oboReader()
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer fh.Close()
	m, err := anrepo.LoadOboJSON(bufio.NewReader(fh))
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m, model.Created, "should match created upload status")
}

func oboReader() (*os.File, error) {
	dir, err := os.Getwd()
	if err != nil {
		return &os.File{}, fmt.Errorf("unable to get current dir %s", err)
	}
	return os.Open(
		filepath.Join(
			filepath.Dir(dir), "testdata", "dicty_phenotypes.json",
		),
	)
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
