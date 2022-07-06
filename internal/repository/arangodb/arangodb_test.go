package arangodb

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/arangomanager/testarango"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-obograph/graph"
	ontostorage "github.com/dictyBase/go-obograph/storage"
	araobo "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func getConnectParamsFromDb(tra *testarango.TestArango) *manager.ConnectParams {
	return &manager.ConnectParams{
		User:     tra.User,
		Pass:     tra.Pass,
		Database: tra.Database,
		Host:     tra.Host,
		Port:     tra.Port,
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

func loadData(tra *testarango.TestArango) error {
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("unable to get current dir %s", err)
	}
	res, err := os.Open(
		filepath.Join(
			filepath.Dir(dir), "testdata", "dicty_annotation.json",
		),
	)
	if err != nil {
		return fmt.Errorf("error in open file %s", err)
	}
	defer res.Close()
	gra, err := graph.BuildGraph(res)
	if err != nil {
		return fmt.Errorf("error in building graph %s", err)
	}
	dsr, err := araobo.NewDataSource(
		&araobo.ConnectParams{
			User:     tra.User,
			Pass:     tra.Pass,
			Host:     tra.Host,
			Database: tra.Database,
			Port:     tra.Port,
			Istls:    tra.Istls,
		}, getOntoParams(),
	)
	if err != nil {
		return fmt.Errorf("error in creating datasource %s", err)
	}
	if dsr.ExistsOboGraph(gra) {
		return errors.New("dicty_annotation already exist, needs a cleanp")
	}

	return saveExistentTestGraph(dsr, gra)
}

func saveExistentTestGraph(dsr ontostorage.DataSource, gra graph.OboGraph) error {
	if err := dsr.SaveOboGraphInfo(gra); err != nil {
		return fmt.Errorf("error in saving graph %s", err)
	}
	if _, err := dsr.SaveTerms(gra); err != nil {
		return fmt.Errorf("error in saving terms %s", err)
	}
	if _, err := dsr.SaveRelationships(gra); err != nil {
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
	for zcount := 0; zcount < num/2; zcount++ {
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
					Rank:          int64(zcount),
				},
			},
		})
	}
	value = fmt.Sprintf("cool gene %s", tags[1])
	for ycount := num / 2; ycount < num; ycount++ {
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
					Rank:          int64(ycount),
				},
			},
		})
	}

	return nal
}

func newTestTaggedAnnotationsList(num int) []*annotation.NewTaggedAnnotation {
	nal := make([]*annotation.NewTaggedAnnotation, 0)
	rsrc := rand.New(rand.NewSource(time.Now().UnixNano()))
	max := 800000
	min := 300000
	for i := 0; i < num; i++ {
		value := fmt.Sprintf("cool gene %s", tags[rsrc.Intn(len(tags)-1)])
		nal = append(nal, &annotation.NewTaggedAnnotation{
			Data: &annotation.NewTaggedAnnotation_Data{
				Type: "annotations",
				Attributes: &annotation.NewTaggedAnnotationAttributes{
					Value:         value,
					EditableValue: value,
					CreatedBy:     "siddbasu@gmail.com",
					Tag:           tags[rsrc.Intn(len(tags)-1)],
					Ontology:      "dicty_annotation",
					EntryId:       fmt.Sprintf("DDB_G0%d", rsrc.Intn(max-min)+min),
					Rank:          0,
				},
			},
		})
	}

	return nal
}

func setUp(t *testing.T) (*require.Assertions, repository.TaggedAnnotationRepository) {
	t.Helper()
	tra, err := testarango.NewTestArangoFromEnv(true)
	if err != nil {
		t.Fatalf("unable to construct new TestArango instance %s", err)
	}
	assert := require.New(t)
	repo, err := NewTaggedAnnotationRepo(
		getConnectParamsFromDb(tra),
		getCollectionParams(),
		getOntoParams(),
	)
	assert.NoErrorf(err, "expect no error connecting to annotation repository, received %s", err)
	err = loadData(tra)
	assert.NoError(err, "expect no error from loading ontology")

	return assert, repo
}

func tearDown(repo repository.TaggedAnnotationRepository) {
	_ = repo.Dbh().Drop()
}

func TestLoadOboJSON(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	fh, err := oboReader()
	assert.NoErrorf(err, "expect no error, received %s", err)
	defer fh.Close()
	info, err := anrepo.LoadOboJSON(bufio.NewReader(fh))
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.True(info.IsCreated, "should match created status")
}

func oboReader() (*os.File, error) {
	dir, err := os.Getwd()
	if err != nil {
		return &os.File{}, fmt.Errorf("unable to get current dir %s", err)
	}

	fhr, err := os.Open(
		filepath.Join(
			filepath.Dir(dir), "testdata", "dicty_phenotypes.json",
		),
	)
	if err != nil {
		return fhr, fmt.Errorf("error in opening file %s", err)
	}

	return fhr, nil
}

func testModelListSort(t *testing.T, m []*model.AnnoDoc) {
	t.Helper()
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

func testGroupMember(t *testing.T, gl []*model.AnnoGroup, count, idx int, email string) {
	t.Helper()
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
	str := make([]string, 0)
	for _, m := range am {
		str = append(str, fn(m))
	}

	return str
}

func model2IdCallback(mod *model.AnnoDoc) string {
	return mod.Key
}
