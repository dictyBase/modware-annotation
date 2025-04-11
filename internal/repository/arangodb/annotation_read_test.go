package arangodb

import (
	"testing"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
)

const (
	filterOne   = `entry_id==DDB_G0286429;tag==private note;ontology==dicty_annotation`
	filterTwo   = `entry_id==DDB_G0294491;tag==name description;ontology==dicty_annotation`
	filterThree = `entry_id==jumbo`
)

//nolint:tparallel
func TestListAnnoFilter(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo) // Teardown might be needed if tests interfere

	// Setup: Create test data once
	tal := newTestTaggedAnnotationsListForFiltering(20)
	for _, anno := range tal {
		_, err := anrepo.AddAnnotation(anno)
		assert.NoErrorf(
			err,
			"setup: expect no error adding annotation, received %s",
			err,
		)
	}

	var mla, ml2, ml4 []*model.AnnoDoc // Store results for subsequent tests

	t.Run("FilterOneFirstPage", func(t *testing.T) {
		mla = testListAnnoFilterOneFirstPage(t, assert, anrepo)
	})

	t.Run("FilterOneSecondPage", func(t *testing.T) {
		ml2 = testListAnnoFilterOneSecondPage(t, assert, anrepo, mla)
	})

	t.Run("FilterOneThirdPage", func(t *testing.T) {
		testListAnnoFilterOneThirdPage(t, assert, anrepo, ml2)
	})

	t.Run("FilterTwoFirstPage", func(t *testing.T) {
		ml4 = testListAnnoFilterTwoFirstPage(t, assert, anrepo)
	})

	t.Run("FilterTwoSecondPage", func(t *testing.T) {
		testListAnnoFilterTwoSecondPage(t, assert, anrepo, ml4)
	})

	t.Run("FilterNotFound", func(t *testing.T) {
		testListAnnoFilterNotFound(t, assert, anrepo)
	})
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
	assert.True(
		mann.CreatedAt.Equal(eim.CreatedAt),
		"should match created time of annotation",
	)
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
	mae, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta.Data.Attributes.Tag,
		EntryId:  nta.Data.Attributes.EntryId,
		Ontology: nta.Data.Attributes.Ontology,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(int64(0), mae.Rank, "should match rank 0")
	assert.Equal(
		mae.EnrtyId,
		nta.Data.Attributes.EntryId,
		"should match the entry id",
	)

	ml2, err := anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      nta2.Data.Attributes.Tag,
		EntryId:  nta2.Data.Attributes.EntryId,
		Ontology: nta2.Data.Attributes.Ontology,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(
		ml2.EnrtyId,
		nta2.Data.Attributes.EntryId,
		"should match the entry id",
	)
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

//nolint:tparallel
func TestAddAnnotation(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)

	firstAnno := newTestAnnoWithTagAndOnto("dicty_annotation", "curator")
	t.Run("SuccessFirst", func(t *testing.T) {
		testAddAnnotationSuccess(t, assert, anrepo, firstAnno)
	})
	t.Run("Duplicate", func(t *testing.T) {
		testAddAnnotationDuplicate(t, assert, anrepo, firstAnno)
	})
	t.Run("NonExistentTag", func(t *testing.T) {
		testAddAnnotationNonExistentTag(t, assert, anrepo, firstAnno)
	})
	t.Run("NonExistentOntology", func(t *testing.T) {
		testAddAnnotationNonExistentOntology(t, assert, anrepo)
	})
	t.Run("SuccessSecond", func(t *testing.T) {
		testAddAnnotationSuccessSecond(t, assert, anrepo)
	})

	t.Run("SuccessThird", func(t *testing.T) {
		testAddAnnotationSuccessThird(t, assert, anrepo)
	})
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
	assert.True(
		repository.IsAnnotationGroupListNotFound(err),
		"expect no annotation group to be found",
	)
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
		assert.Equal("dicty_annotation", m.Ontology, "should match ontology")
		assert.Falsef(m.IsObsolete, "tag %s should not be obsolete", tag)
	}
	_, err := anrepo.GetAnnotationTag("yadayada", "dicty_annotation")
	assert.Error(err, "expect error from non-existent tag")
	assert.True(
		repository.IsAnnoTagNotFound(err),
		"should be an error for non-existent tag",
	)
}

func testListAnnoFilterOneFirstPage(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
) []*model.AnnoDoc {
	t.Helper()
	mla, err := anrepo.ListAnnotations(
		&repository.ListAnnotationsParams{Limit: 4, Filter: filterOne},
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(mla, 5, "should have 5 annotations")
	for _, m := range mla {
		assert.Equal("sidd@gmail.com", m.CreatedBy, "should match created by")
		assert.Equal(m.Tag, tags[0], "should match the tag")
		assert.Equal(m.EnrtyId, ddbg[0], "should match the entry id")
	}
	testModelListSort(t, mla)

	return mla
}

func testListAnnoFilterOneSecondPage(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
	prevResult []*model.AnnoDoc,
) []*model.AnnoDoc {
	t.Helper()
	assert.NotEmpty(prevResult, "previous result should not be empty")
	ml2, err := anrepo.ListAnnotations(&repository.ListAnnotationsParams{
		Cursor: toTimestamp(prevResult[len(prevResult)-1].CreatedAt),
		Limit:  4, Filter: filterOne,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml2, 5, "should have five annotations")
	assert.Exactly(
		prevResult[len(prevResult)-1],
		ml2[0],
		"should have identical model objects",
	)
	testModelListSort(t, ml2)

	return ml2
}

func testListAnnoFilterOneThirdPage(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
	prevResult []*model.AnnoDoc,
) {
	t.Helper()
	assert.NotEmpty(prevResult, "previous result should not be empty")
	ml3, err := anrepo.ListAnnotations(&repository.ListAnnotationsParams{
		Cursor: toTimestamp(prevResult[len(prevResult)-1].CreatedAt),
		Limit:  4, Filter: filterOne,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml3, 2, "should have two annotations")
	assert.Exactly(
		prevResult[len(prevResult)-1],
		ml3[0],
		"should have identical model objects",
	)
	testModelListSort(t, ml3)
}

func testListAnnoFilterTwoFirstPage(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
) []*model.AnnoDoc {
	t.Helper()
	ml4, err := anrepo.ListAnnotations(
		&repository.ListAnnotationsParams{Limit: 6, Filter: filterTwo},
	)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml4, 7, "should have 7 annotations")
	for _, m := range ml4 {
		assert.Equal("basu@gmail.com", m.CreatedBy, "should match created by")
		assert.Equal(m.Tag, tags[1], "should match the tag")
		assert.Equal(m.EnrtyId, ddbg[1], "should match the entry id")
	}
	testModelListSort(t, ml4)

	return ml4
}

func testListAnnoFilterTwoSecondPage(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
	prevResult []*model.AnnoDoc,
) {
	t.Helper()
	assert.NotEmpty(prevResult, "previous result should not be empty")
	ml5, err := anrepo.ListAnnotations(&repository.ListAnnotationsParams{
		Cursor: toTimestamp(prevResult[len(prevResult)-1].CreatedAt),
		Limit:  4, Filter: filterTwo,
	})
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Len(ml5, 4, "should have four annotations")
	assert.Exactly(
		prevResult[len(prevResult)-1],
		ml5[0],
		"should have identical model objects",
	)
	testModelListSort(t, ml5)
}

func testListAnnoFilterNotFound(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	_, err := anrepo.ListAnnotations(
		&repository.ListAnnotationsParams{Limit: 4, Filter: filterThree},
	)
	assert.Error(err, "expect error")
	assert.True(
		repository.IsAnnotationListNotFound(err),
		"expect no annotation list found",
	)
}

// Helper functions for TestAddAnnotation subtests.
func testAddAnnotationSuccess(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
	nta *annotation.NewTaggedAnnotation,
) {
	t.Helper()
	mann, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.False(
		mann.IsObsolete,
		"new tagged annotation should not be obsolete",
	)
	assert.Equal(
		nta.Data.Attributes.Value,
		mann.Value,
		"should match the value",
	)
	assert.Equal(
		nta.Data.Attributes.CreatedBy,
		mann.CreatedBy,
		"should match created_by",
	)
	assert.Equal(
		nta.Data.Attributes.EntryId,
		mann.EnrtyId,
		"should match entry identifier",
	)
	assert.Equal(nta.Data.Attributes.Rank, mann.Rank, "should match the rank")
	assert.Equal(
		nta.Data.Attributes.Ontology,
		mann.Ontology,
		"should match ontology name",
	)
	assert.Equal(
		nta.Data.Attributes.Tag,
		mann.Tag,
		"should match the ontology tag",
	)
}

func testAddAnnotationDuplicate(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
	nta *annotation.NewTaggedAnnotation,
) {
	t.Helper()
	_, err := anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error for existing annotation")
	assert.Regexp(
		"already exists",
		err.Error(), "error should have existence of annotation",
	)
}

func testAddAnnotationNonExistentTag(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
	nta *annotation.NewTaggedAnnotation,
) {
	t.Helper()
	// Create a copy to avoid modifying the original nta used in other tests
	ntaCopy := &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:     nta.Data.Attributes.Value,
				CreatedBy: nta.Data.Attributes.CreatedBy,
				EntryId:   nta.Data.Attributes.EntryId,
				Rank:      nta.Data.Attributes.Rank,
				Ontology:  nta.Data.Attributes.Ontology,
				Tag:       "respiration", // Non-existent tag
			},
		},
	}
	_, err := anrepo.AddAnnotation(ntaCopy)
	assert.Error(err, "expect error in case of non-existent ontology and tag")
	assert.Regexp(
		"respiration",
		err.Error(), "error should contain the non-existent tag name",
	)
}

func testAddAnnotationNonExistentOntology(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	nta := newTestAnnoWithTagAndOnto("caboose", "description")
	_, err := anrepo.AddAnnotation(nta)
	assert.Error(err, "expect error in case of non-existent ontology and tag")
	assert.Regexp(
		"caboose",
		err.Error(), "error should contain the non-existent ontology",
	)
}

func testAddAnnotationSuccessSecond(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	nta := newTestAnnoWithTagAndOnto("dicty_annotation", "summary")
	mann2, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.False(
		mann2.IsObsolete,
		"new tagged annotation should not be obsolete",
	)
	assert.Equal(
		nta.Data.Attributes.Value,
		mann2.Value,
		"should match the value",
	)
	assert.Equal(
		nta.Data.Attributes.CreatedBy,
		mann2.CreatedBy,
		"should match created_by",
	)
	assert.Equal(
		nta.Data.Attributes.EntryId,
		mann2.EnrtyId,
		"should match entry identifier",
	)
	assert.Equal(nta.Data.Attributes.Rank, mann2.Rank, "should match the rank")
	assert.Equal(
		nta.Data.Attributes.Ontology,
		mann2.Ontology,
		"should match ontology name",
	)
	// The tag "summary" maps to "description" in the test setup ontology
	assert.Equal("description", mann2.Tag, "should match the ontology tag")
}

func testAddAnnotationSuccessThird(
	t *testing.T,
	assert *require.Assertions,
	anrepo repository.TaggedAnnotationRepository,
) {
	t.Helper()
	nta := newTestAnnoWithTagAndOnto(
		"dicty_annotation",
		"decreased 3',5'-cyclic-GMP phosphodiesterase activity",
	)
	annm, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(
		nta.Data.Attributes.Ontology,
		annm.Ontology,
		"should match ontology name",
	)
	assert.Equal(nta.Data.Attributes.Tag, annm.Tag, "should match the tag")
}
