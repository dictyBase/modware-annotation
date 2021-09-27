package arangodb

import (
	"testing"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/stretchr/testify/assert"
)

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
