package arangodb

import (
	"regexp"
	"testing"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

const (
	filterOne = `FILTER ann.entry_id == 'DDB_G0286429'
				  AND cvt.label == 'private note'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	filterTwo = `FILTER ann.entry_id == 'DDB_G0294491'
				  AND cvt.label == 'name description'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	filterThree = `FILTER ann.entry_id == 'jumbo'`
)

func TestListAnnotations(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsList(15)
	for _, anno := range tal {
		_, err := anrepo.AddAnnotation(anno)
		assert.NoErrorf(err, "expect no error, received %s", err)
	}
	mla, err := anrepo.ListAnnotations(0, 4, "")
	if err != nil {
		assert.NoErrorf(err, "expect no error, received %s", err)
	}
	assert.Len(mla, 5, "should have 5 annotations")
	for ict, manno := range mla {
		assert.Contains(manno.Value, "cool gene", "should contain the phrase cool gene")
		assert.Equal(tal[ict].Data.Attributes.CreatedBy, manno.CreatedBy, "should match created by")
		assert.Subset(tags, []string{manno.Tag}, "should contain the tag in the slice")
		assert.Equal(tal[ict].Data.Attributes.Ontology, manno.Ontology, "should match the ontology")
		assert.Contains(manno.EnrtyId, "DDB_G0", "should contain the DDB_G0 in entry id")
		assert.Equal(int(manno.Rank), 0, "should match the zero rank")
	}
	ml2, err := anrepo.ListAnnotations(
		toTimestamp(mla[len(mla)-1].CreatedAt),
		4,
		"",
	)
	if err != nil {
		t.Fatalf("error in fetching annotation list %s", err)
	}
	assert.Len(ml2, 5, "should have five annotations")
	assert.Exactly(mla[len(mla)-1], ml2[0], "should have identical model objects")

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
	testModelListSort(t, mla)
	testModelListSort(t, ml2)
	testModelListSort(t, ml3)
	testModelListSort(t, ml4)
}

func TestListAnnoFilter(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsListForFiltering(20)
	for _, anno := range tal {
		_, err := anrepo.AddAnnotation(anno)
		assert.NoErrorf(err, "expect no error, received %s", err)
	}
	mla, err := anrepo.ListAnnotations(0, 4, filterOne)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(mla, 5, "should have 5 annotations")
	for _, m := range mla {
		assert.Equal(m.CreatedBy, "sidd@gmail.com", "should match created by")
		assert.Equal(m.Tag, tags[0], "should match the tag")
		assert.Equal(m.EnrtyId, ddbg[0], "should match the entry id")
	}
	ml2, err := anrepo.ListAnnotations(
		toTimestamp(mla[len(mla)-1].CreatedAt),
		4, filterOne,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml2, 5, "should have five annotations")
	assert.Exactly(mla[len(mla)-1], ml2[0], "should have identical model objects")
	ml3, err := anrepo.ListAnnotations(
		toTimestamp(ml2[len(ml2)-1].CreatedAt),
		4, filterOne,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml3, 2, "should have two annotations")
	assert.Exactly(ml2[len(ml2)-1], ml3[0], "should have identical model objects")
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
		4, filterTwo,
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml5, 4, "should have four annotations")
	assert.Exactly(ml4[len(ml4)-1], ml5[0], "should have identical model objects")
	for _, sml := range [][]*model.AnnoDoc{mla, ml2, ml3, ml4, ml5} {
		testModelListSort(t, sml)
	}
	_, err = anrepo.ListAnnotations(0, 4, filterThree)
	assert.Error(err, "expect error")
	assert.True(repository.IsAnnotationListNotFound(err), "expect no annotation list found")
}

func TestGetAnnotationByID(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	nta := newTestTaggedAnnotation()
	mann, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nta2 := newTestTaggedAnnotationWithParams("curation", "DDB_G0287317")
	ml2, err := anrepo.AddAnnotation(nta2)
	assert.NoErrorf(err, "expect no error, received %s", err)
	eim, err := anrepo.GetAnnotationByID(mann.Key)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(mann.EnrtyId, eim.EnrtyId, "should match entry identifier")
	assert.Equal(mann.Ontology, eim.Ontology, "should match ontology")
	assert.Equal(mann.Tag, eim.Tag, "should match tag")
	assert.Equal(mann.Key, eim.Key, "should match the identifier")
	assert.Equal(mann.Value, eim.Value, "should match the value")
	assert.True(mann.CreatedAt.Equal(eim.CreatedAt), "should match created time of annotation")
	assert.Equal(mann.Rank, eim.Rank, "should match rank")

	em2, err := anrepo.GetAnnotationByID(ml2.Key)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(ml2.EnrtyId, em2.EnrtyId, "should match entry identifier")

	nie, err := anrepo.GetAnnotationByID("9999999")
	assert.Errorf(err, "expected %s error, received nothing", err)
	assert.True(
		repository.IsAnnotationNotFound(err),
		"entry should not exist",
	)
	assert.True(nie.NotFound, "entry should not exist")
}

func TestGetAnnotationByEntry(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	nta := newTestTaggedAnnotation()
	_, err := anrepo.AddAnnotation(nta)
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

	ml2, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta2.Data.Attributes.Tag,
		EntryId:  nta2.Data.Attributes.EntryId,
		Ontology: nta2.Data.Attributes.Ontology,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(ml2.EnrtyId, nta2.Data.Attributes.EntryId, "should match the entry id")
	assert.Equal(ml2.Tag, nta2.Data.Attributes.Tag, "should match the tag")

	emt, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta2.Data.Attributes.Tag,
		Ontology: nta2.Data.Attributes.Ontology,
		EntryId:  "DDB_G0277853",
	})
	assert.Errorf(err, "expect %s error, received nothing", err)
	assert.True(
		repository.IsAnnotationNotFound(err),
		"the entry should not exist",
	)
	assert.True(emt.NotFound, "the entry should not exist")
}

func TestAddAnnotation(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	nta := newTestAnnoWithTagAndOnto("dicty_annotation", "curator")
	mann, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.False(mann.IsObsolete, "new tagged annotation should not be obsolete")
	assert.Equal(mann.Value, nta.Data.Attributes.Value, "should match the value")
	assert.Equal(mann.CreatedBy, nta.Data.Attributes.CreatedBy, "should match created_by")
	assert.Equal(mann.EnrtyId, nta.Data.Attributes.EntryId, "should match entry identifier")
	assert.Equal(mann.Rank, nta.Data.Attributes.Rank, "should match the rank")
	assert.Equal(mann.Ontology, nta.Data.Attributes.Ontology, "should match ontology name")
	assert.Equal(mann.Tag, nta.Data.Attributes.Tag, "should match the ontology tag")
	_, err = anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error for existing annotation")
	assert.Regexp(
		regexp.MustCompile("already exists"),
		err.Error(), "error should have existence of annotation",
	)
	nta.Data.Attributes.Tag = "respiration"
	_, err = anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error in case of non-existent ontology and tag")
	assert.Regexp(
		regexp.MustCompile("respiration"),
		err.Error(), "error should contain the non-existent tag name",
	)
	nta = newTestAnnoWithTagAndOnto("caboose", "description")
	_, err = anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error in case of non-existent ontology and tag")
	assert.Regexp(
		regexp.MustCompile("caboose"),
		err.Error(), "error should contain the non-existent ontology",
	)
	nta = newTestAnnoWithTagAndOnto("dicty_annotation", "summary")
	mann2, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.False(mann2.IsObsolete, "new tagged annotation should not be obsolete")
	assert.Equal(mann2.Value, nta.Data.Attributes.Value, "should match the value")
	assert.Equal(mann2.CreatedBy, nta.Data.Attributes.CreatedBy, "should match created_by")
	assert.Equal(mann2.EnrtyId, nta.Data.Attributes.EntryId, "should match entry identifier")
	assert.Equal(mann2.Rank, nta.Data.Attributes.Rank, "should match the rank")
	assert.Equal(mann2.Ontology, nta.Data.Attributes.Ontology, "should match ontology name")
	assert.Equal(mann2.Tag, "description", "should match the ontology tag")
	nta = newTestAnnoWithTagAndOnto("dicty_annotation", "decreased 3',5'-cyclic-GMP phosphodiesterase activity")
	m3, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(m3.Ontology, nta.Data.Attributes.Ontology, "should match ontology name")
	assert.Equal(m3.Tag, nta.Data.Attributes.Tag, "should match the tag")
}

func TestGetAnnotationGroup(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsList(4)
	mla := make([]*model.AnnoDoc, 0)
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		mla = append(mla, m)
	}
	ids := testModelMaptoID(mla, model2IdCallback)
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

func TestListAnnGrFilter(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsListForFiltering(20)
	mla := make([]*model.AnnoDoc, 0)
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		mla = append(mla, m)
	}
	j := 5
	for i := 0; j <= len(mla); i += 5 {
		ids := testModelMaptoID(mla[i:j], model2IdCallback)
		_, err := anrepo.AddAnnotationGroup(ids...)
		assert.NoErrorf(err, "expect no error, received %s", err)
		j += 5
	}
	filterOne := `FILTER ann.entry_id == 'DDB_G0286429'
				  AND cvt.label == 'private note'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	egl, err := anrepo.ListAnnotationGroup(0, 10, filterOne)
	assert.NoErrorf(err, "expect no error, received %s", err)
	testGroupMember(t, egl, 2, 0, "sidd@gmail.com")
	filterTwo := `FILTER ann.entry_id == 'DDB_G0294491'
				  AND cvt.label == 'name description'
				  AND cv.metadata.namespace == 'dicty_annotation'
	`
	egl2, err := anrepo.ListAnnotationGroup(0, 10, filterTwo)
	assert.NoErrorf(err, "expect no error, received %s", err)
	testGroupMember(t, egl2, 2, 1, "basu@gmail.com")
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
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsList(60)
	mla := make([]*model.AnnoDoc, 0)
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		mla = append(mla, m)
	}
	j := 5
	for i := 0; j <= len(mla); i += 5 {
		ids := testModelMaptoID(mla[i:j], model2IdCallback)
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
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	for _, tag := range tags[:6] {
		m, err := anrepo.GetAnnotationTag(tag, "dicty_annotation")
		assert.NoErrorf(err, "expect no error from fetching %s tag", tag)
		assert.Equal(m.Name, tag, "should match tag name")
		assert.Equal(m.Ontology, "dicty_annotation", "should match ontology")
		assert.Falsef(m.IsObsolete, "tag %s should not be obsolete", tag)
	}
	_, err := anrepo.GetAnnotationTag("yadayada", "dicty_annotation")
	assert.Error(err, "expect error from non-existent tag")
	assert.True(repository.IsAnnoTagNotFound(err), "should be an error for non-existent tag")
}
