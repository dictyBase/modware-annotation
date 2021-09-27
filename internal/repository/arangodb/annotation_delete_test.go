package arangodb

import (
	"testing"

	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/stretchr/testify/assert"
)

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
