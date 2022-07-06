package arangodb

import (
	"testing"

	"github.com/dictyBase/modware-annotation/internal/model"
)

func TestRemoveFromAnnotationGroup(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsList(9)
	mla := make([]*model.AnnoDoc, 0)
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		mla = append(mla, m)
	}
	ids := testModelMaptoID(mla, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	ega, err := anrepo.RemoveFromAnnotationGroup(g.GroupId, ids[:5]...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.ElementsMatch(
		testModelMaptoID(g.AnnoDocs, model2IdCallback),
		ids,
		"should match no of documents",
	)
	assert.ElementsMatch(
		ids[5:],
		testModelMaptoID(ega.AnnoDocs, model2IdCallback),
		"expected identical annotation identifiers after removing from the group",
	)
}

func TestRemoveAnnotationGroup(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsList(7)
	mla := make([]*model.AnnoDoc, 0)
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		mla = append(mla, m)
	}
	ids := testModelMaptoID(mla, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotationGroup(g.GroupId)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotationGroup(g.GroupId)
	assert.Errorf(err, "should return error")
	assert.Contains(
		err.Error(),
		"removing group",
		"should contain removing group phrase",
	)
}

func TestRemoveAnnotation(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	nta := newTestTaggedAnnotation()
	m, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nta2 := newTestTaggedAnnotationWithParams("curation", "DDB_G0287317")
	mt2, err := anrepo.AddAnnotation(nta2)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotation(m.Key, true)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotation(mt2.Key, false)
	assert.NoErrorf(err, "expect no error, received %s", err)
	err = anrepo.RemoveAnnotation(mt2.Key, false)
	assert.Errorf(err, "should return error")
	assert.Contains(err.Error(), "obsolete", "should contain obsolete message")
}
