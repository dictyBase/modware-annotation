package arangodb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	E "github.com/IBM/fp-go/v2/either"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
)

const (
	tagLookupOntology = "tag_lookup"
	tagLookupEntry    = "DBS0000001"
)

// setUpTagLookup loads the tag_lookup test ontology next to dicty_annotation.
func setUpTagLookup(
	t *testing.T,
) (*require.Assertions, repository.TaggedAnnotationRepository) {
	t.Helper()
	assert, anrepo := setUp(t)
	dir, err := os.Getwd()
	assert.NoError(err)
	fh, err := os.Open(
		filepath.Join(filepath.Dir(dir), "testdata", "tag_lookup.json"),
	)
	assert.NoError(err)
	defer func() { _ = fh.Close() }()
	_, err = anrepo.LoadOboJSON(fh)
	assert.NoError(err, "expect no error from loading tag_lookup ontology")

	return assert, anrepo
}

// forceTermID runs a term id lookup and returns its result as a Go pair.
func forceTermID(lookup IOE.IOEither[error, string]) (string, error) {
	return E.UnwrapError(lookup())
}

func newTagLookupAnno(tag string, rank int64) *annotation.NewTaggedAnnotation {
	return &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:     "novalue",
				CreatedBy: "curator@example.org",
				Tag:       tag,
				Ontology:  tagLookupOntology,
				EntryId:   tagLookupEntry,
				Rank:      rank,
			},
		},
	}
}

func TestGetAnnotationTagLookup(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUpTagLookup(t)
	defer tearDown(anrepo)

	found := []struct {
		name, tag, wantName, wantID string
		obsolete                    bool
	}{
		{"id with colon", "TGLK:0000001", "alpha", "TGLK_0000001", false},
		{"id with underscore", "TGLK_0000001", "alpha", "TGLK_0000001", false},
		{"label wins over synonym", "alpha", "alpha", "TGLK_0000001", false},
		{"unique synonym", "alpha alias", "alpha", "TGLK_0000001", false},
		{"obsolete id", "TGLK:0000003", "obsolete gamma", "TGLK_0000003", true},
	}
	for _, tc := range found {
		tag, err := anrepo.GetAnnotationTag(tc.tag, tagLookupOntology)
		assert.NoErrorf(err, "%s: expect no error for tag %s", tc.name, tc.tag)
		assert.Equalf(tc.wantName, tag.Name, "%s: tag name", tc.name)
		assert.Equalf(tc.wantID, tag.ID, "%s: tag id", tc.name)
		assert.Equalf(tagLookupOntology, tag.Ontology, "%s: ontology", tc.name)
		assert.Equalf(tc.obsolete, tag.IsObsolete, "%s: obsolete flag", tc.name)
	}

	notFound := []struct{ name, tag, ontology string }{
		{"unknown id", "TGLK:9999999", tagLookupOntology},
		{"id in wrong ontology", "TGLK:0000001", "dicty_annotation"},
		{"id-like label is not matched", "XYZ_0000001", tagLookupOntology},
	}
	for _, tc := range notFound {
		_, err := anrepo.GetAnnotationTag(tc.tag, tc.ontology)
		assert.Errorf(err, "%s: expect error", tc.name)
		assert.Truef(
			repository.IsAnnoTagNotFound(err),
			"%s: expect tag not found error, received %s", tc.name, err,
		)
	}

	_, err := anrepo.GetAnnotationTag("shared synonym", tagLookupOntology)
	assert.Error(err, "expect error for a synonym shared by two terms")
	assert.Truef(
		repository.IsAnnoTagAmbiguous(err),
		"expect ambiguous tag error, received %s", err,
	)
}

func TestAddAnnotationTagLookup(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUpTagLookup(t)
	defer tearDown(anrepo)

	_, err := anrepo.AddAnnotation(newTagLookupAnno("shared synonym", 0))
	assert.Error(err, "expect error for a synonym shared by two terms")
	assert.Truef(
		repository.IsAnnoTagAmbiguous(err),
		"expect ambiguous tag error, received %s", err,
	)
	_, err = anrepo.AddAnnotation(newTagLookupAnno("TGLK:0000003", 0))
	assert.Error(err, "expect error for an obsolete term")
	assert.Truef(
		repository.IsAnnoTagNotFound(err),
		"expect tag not found error for obsolete term, received %s", err,
	)
	_, err = anrepo.GetAnnotationByEntry(&annotation.EntryAnnotationRequest{
		Tag:      "alpha",
		Ontology: tagLookupOntology,
		EntryId:  tagLookupEntry,
		Rank:     0,
	})
	assert.Error(err, "expect no annotation after rejected creates")
	assert.True(repository.IsAnnotationNotFound(err))

	m, err := anrepo.AddAnnotation(newTagLookupAnno("TGLK:0000001", 0))
	assert.NoError(err, "expect no error when creating with a term id")
	assert.Equal("alpha", m.Tag, "expect stored tag to be the term label")
	assert.Equal(tagLookupOntology, m.Ontology)

	for _, tag := range []string{"alpha", "TGLK_0000001", "TGLK:0000001", "alpha alias"} {
		_, err = anrepo.AddAnnotation(newTagLookupAnno(tag, 0))
		assert.Errorf(err, "expect duplicate error for tag form %s", tag)
		assert.Contains(err.Error(), "already exists")
	}

	m, err = anrepo.AddAnnotation(newTagLookupAnno("alpha alias", 1))
	assert.NoError(err, "expect no error when creating with a unique synonym")
	assert.Equal("alpha", m.Tag)

	_, err = anrepo.AddAnnotation(newTagLookupAnno("delta", 2))
	assert.Truef(
		repository.IsAnnoTagAmbiguous(err),
		"expect ambiguous error for a label that two terms share, received %s",
		err,
	)
	for _, tag := range []string{"TGLK:0000005", "TGLK:0000006"} {
		m, err = anrepo.AddAnnotation(newTagLookupAnno(tag, 2))
		assert.NoErrorf(
			err,
			"expect distinct terms with one label not to be duplicates: %s",
			tag,
		)
		assert.Equal("delta", m.Tag)
	}
}

func TestLookupTermIDWithContext(t *testing.T) {
	t.Parallel()
	assert, anrepo := setUpTagLookup(t)
	defer tearDown(anrepo)
	arepo, ok := anrepo.(*arangorepository)
	assert.True(ok, "expect the arangodb repository implementation")
	ctx := context.Background()

	key, err := forceTermID(
		arepo.lookupTermID(ctx, tagLookupOntology, "TGLK:0000001"),
	)
	assert.NoError(err)
	label, err := arepo.termName(key)
	assert.NoError(err)
	assert.Equal("alpha", label, "expect the cvterm _id of the term")

	_, err = forceTermID(
		arepo.lookupTermID(ctx, tagLookupOntology, "shared synonym"),
	)
	assert.True(repository.IsAnnoTagAmbiguous(err))

	_, err = forceTermID(
		arepo.lookupTermID(ctx, tagLookupOntology, "TGLK:9999999"),
	)
	assert.True(repository.IsAnnoTagNotFound(err))

	_, err = forceTermID(
		arepo.lookupTermID(ctx, tagLookupOntology, "TGLK:0000003"),
	)
	assert.True(
		repository.IsAnnoTagNotFound(err),
		"expect an obsolete term to be absent for annotation creation",
	)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = forceTermID(
		arepo.lookupTermID(cancelled, tagLookupOntology, "TGLK:0000001"),
	)
	assert.Error(err, "expect error for a cancelled context")
	assert.ErrorIs(err, context.Canceled)
}
