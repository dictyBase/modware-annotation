package arangodb

import (
	"testing"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
)

func TestEditAnnotation(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	nta := newTestTaggedAnnotation()
	mda, err := anrepo.AddAnnotation(nta)
	assert.NoErrorf(err, "expect no error, received %s", err)
	uan := &annotation.TaggedAnnotationUpdate{
		Data: &annotation.TaggedAnnotationUpdate_Data{
			Type: "annotations",
			Id:   mda.Key,
			Attributes: &annotation.TaggedAnnotationUpdateAttributes{
				Value:         "updated gene description",
				EditableValue: "updated gene description",
				CreatedBy:     "basu@gmail.com",
			},
		},
	}
	um, err := anrepo.EditAnnotation(uan)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Equal(mda.Version+1, um.Version, "version should be incremented by 1")
	assert.NotEqual(uan.Data.Id, um.Key, "identifier should not match")
	assert.Equal(uan.Data.Attributes.Value, um.Value, "should matches the value")
	assert.Equal(uan.Data.Attributes.CreatedBy, um.CreatedBy, "should matches created by")
}

func TestAddAnnotationGroup(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUp(t)
	defer tearDown(anrepo)
	tal := newTestTaggedAnnotationsList(8)
	mla := make([]*model.AnnoDoc, 0)
	for _, ann := range tal {
		m, err := anrepo.AddAnnotation(ann)
		assert.NoErrorf(err, "expect no error, received %s", err)
		mla = append(mla, m)
	}
	ids := testModelMaptoID(mla, model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.Lenf(g.AnnoDocs, len(ids), "should have %d annotations", len(ids))
}

func TestAppendToAnntationGroup(t *testing.T) {
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
	ids := testModelMaptoID(mla[:4], model2IdCallback)
	g, err := anrepo.AddAnnotationGroup(ids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	nids := testModelMaptoID(mla[4:], model2IdCallback)
	eg, err := anrepo.AppendToAnnotationGroup(g.GroupId, nids...)
	assert.NoErrorf(err, "expect no error, received %s", err)
	assert.ElementsMatch(
		testModelMaptoID(eg.AnnoDocs, model2IdCallback),
		append(ids, nids...),
		"expected identical annotation identifiers after appending to the group",
	)
}
