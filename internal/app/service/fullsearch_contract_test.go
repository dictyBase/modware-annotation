package service

import (
	"testing"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/stretchr/testify/require"
)

func TestAnnotationSearchProtoContract(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	validReq := &annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationSearchAttributes{
				Query: "cs",
				Limit: 50,
			},
		},
	}
	assert.NoError(protovalidate.Validate(validReq))

	// Nil data and nil attributes are rejected.
	assert.Error(protovalidate.Validate(&annotation.AnnotationSearchParameters{}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{Type: "annotation"},
	}))

	// Query shorter than 2 characters is rejected.
	assert.Error(protovalidate.Validate(&annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{
			Attributes: &annotation.AnnotationSearchAttributes{Query: "a", Limit: 10},
		},
	}))

	// Limit below 0 or above 50 is rejected.
	assert.Error(protovalidate.Validate(&annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{
			Attributes: &annotation.AnnotationSearchAttributes{Query: "cs", Limit: -1},
		},
	}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{
			Attributes: &annotation.AnnotationSearchAttributes{Query: "cs", Limit: 51},
		},
	}))

	// All 10 search field enum values exist with their contract numbers.
	assert.Equal(int32(1), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM))
	assert.Equal(int32(2), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE))
	assert.Equal(int32(3), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYSTEMATIC_NAME))
	assert.Equal(int32(4), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENETIC_MODIFICATION))
	assert.Equal(int32(5), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_MUTAGENESIS_METHOD))
	assert.Equal(int32(6), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PHENOTYPE))
	assert.Equal(int32(7), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_ENVIRONMENT))
	assert.Equal(int32(8), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_ASSAY))
	assert.Equal(int32(9), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_CHARACTERISTIC))
	assert.Equal(int32(10), int32(annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PUBLIC_NOTE))

	resp := &annotation.AnnotationSearchResultCollection{
		Data: []*annotation.AnnotationSearchResult{
			{
				EntryId:     "DBS0236126",
				Field:       annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PHENOTYPE,
				DisplayText: "aberrant fruiting body morphogenesis",
				Score:       504.2,
			},
		},
		Meta: &annotation.Meta{Limit: 50, Total: 1},
	}
	assert.Equal("DBS0236126", resp.GetData()[0].GetEntryId())
	assert.Equal(int64(1), resp.GetMeta().GetTotal())
}
