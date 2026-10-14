package service

import (
	"testing"

	"github.com/bufbuild/protovalidate-go"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/stretchr/testify/require"
)

func TestAnnotationAutocompleteProtoContract(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	validReq := &annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationAutocompleteAttributes{
				Query: "axe",
				Limit: 5,
			},
		},
	}
	assert.NoError(protovalidate.Validate(validReq))

	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{Type: "annotation"},
	}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Attributes: &annotation.AnnotationAutocompleteAttributes{Query: "ax", Limit: 5},
		},
	}))
	assert.Error(protovalidate.Validate(&annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Attributes: &annotation.AnnotationAutocompleteAttributes{Query: "axe", Limit: 51},
		},
	}))

	col := &annotation.AnnotationSuggestionCollection{
		Data: []*annotation.AnnotationSuggestion{
			{
				EntryId:     "DBS0236126",
				Field:       annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE,
				DisplayText: "axeA1, axeB1",
				Score:       1000,
			},
		},
		Meta: &annotation.Meta{NextCursor: 0, Limit: 5, Total: 1},
	}
	assert.Equal(int64(1), col.GetMeta().GetTotal())
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM,
		annotation.AnnotationSearchField(1),
	)
}
