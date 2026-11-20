package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const tagLookupServiceOntology = "tag_lookup"

func tagLookupServiceAnno(tag string) *annotation.NewTaggedAnnotation {
	return &annotation.NewTaggedAnnotation{
		Data: &annotation.NewTaggedAnnotation_Data{
			Type: "annotations",
			Attributes: &annotation.NewTaggedAnnotationAttributes{
				Value:     "novalue",
				CreatedBy: "curator@example.org",
				Tag:       tag,
				Ontology:  tagLookupServiceOntology,
				EntryId:   "DBS0000002",
				Rank:      0,
			},
		},
	}
}

// TestTagLookupByTermIDOverGRPC verifies term id lookup through the gRPC
// CreateAnnotation and GetAnnotationTag methods on a real ArangoDB.
func TestTagLookupByTermIDOverGRPC(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	client, repo := autocompleteEndToEndSetup(t)

	dir, err := os.Getwd()
	assert.NoError(err)
	reader, err := os.Open(filepath.Join(
		filepath.Dir(filepath.Dir(dir)),
		"repository",
		"testdata",
		"tag_lookup.json",
	))
	assert.NoError(err)
	defer func() { _ = reader.Close() }()
	_, err = repo.LoadOboJSON(reader)
	assert.NoError(err, "expect the tag_lookup ontology to load")

	ctx := context.Background()
	tag, err := client.GetAnnotationTag(ctx, &annotation.TagRequest{
		Name:     "TGLK:0000001",
		Ontology: tagLookupServiceOntology,
	})
	assert.NoError(err)
	assert.Equal("alpha", tag.Name)
	assert.Equal("TGLK_0000001", tag.Id)

	_, err = client.GetAnnotationTag(ctx, &annotation.TagRequest{
		Name:     "TGLK:9999999",
		Ontology: tagLookupServiceOntology,
	})
	assert.Equal(codes.NotFound, status.Code(err))

	_, err = client.GetAnnotationTag(ctx, &annotation.TagRequest{
		Name:     "shared synonym",
		Ontology: tagLookupServiceOntology,
	})
	assert.Equal(codes.InvalidArgument, status.Code(err))

	created, err := client.CreateAnnotation(
		ctx,
		tagLookupServiceAnno("TGLK:0000001"),
	)
	assert.NoError(err, "expect create with a term id to succeed")
	assert.Equal("alpha", created.Data.Attributes.Tag)
	assert.Equal(tagLookupServiceOntology, created.Data.Attributes.Ontology)

	_, err = client.CreateAnnotation(ctx, tagLookupServiceAnno("alpha"))
	assert.Error(err, "expect duplicate error with the label of the same term")

	_, err = client.CreateAnnotation(
		ctx,
		tagLookupServiceAnno("shared synonym"),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
}
