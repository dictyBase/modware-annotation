package service

import (
	"context"
	"errors"
	"testing"

	IOE "github.com/IBM/fp-go/v2/ioeither"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubFullSearchRepo records the full search request it received and returns a
// canned result, so the handler contract is observable without a database.
type stubFullSearchRepo struct {
	repository.TaggedAnnotationRepository
	params *repository.FullSearchQuery
	rows   []*repository.FullSearchResult
	err    error
	calls  int
}

// SearchAnnotation implements the repository method under test.
func (s *stubFullSearchRepo) SearchAnnotation(
	params *repository.FullSearchQuery,
) IOE.IOEither[error, []*repository.FullSearchResult] {
	s.calls++
	s.params = params
	if s.err != nil {
		return IOE.Left[[]*repository.FullSearchResult](s.err)
	}

	return IOE.Of[error](s.rows)
}

// fullSearchParamsWith builds a full search request around a query and a limit.
func fullSearchParamsWith(
	query string,
	limit int64,
) *annotation.AnnotationSearchParameters {
	return &annotation.AnnotationSearchParameters{
		Data: &annotation.AnnotationSearchParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationSearchAttributes{
				Query: query,
				Limit: limit,
			},
		},
	}
}

func TestSearchAnnotationHandlerMapsResults(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{
		rows: []*repository.FullSearchResult{
			{
				EntryID:     "DBS0236126",
				Field:       "phenotype",
				DisplayText: "aberrant fruiting body morphogenesis",
				Score:       504.2,
			},
			{
				EntryID:     "DBS0236125",
				Field:       "synonym",
				DisplayText: "gammaS13",
				Score:       1012.5,
			},
		},
	}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("fruiting", 25),
	)
	assert.NoError(err)
	assert.Len(col.GetData(), 2)
	assert.Equal("DBS0236126", col.GetData()[0].GetEntryId())
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PHENOTYPE,
		col.GetData()[0].GetField(),
	)
	assert.Equal(
		"aberrant fruiting body morphogenesis",
		col.GetData()[0].GetDisplayText(),
	)
	assert.Equal(504.2, col.GetData()[0].GetScore())
	assert.Equal("DBS0236125", col.GetData()[1].GetEntryId())
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM,
		col.GetData()[1].GetField(),
	)
	assert.Equal("gammaS13", col.GetData()[1].GetDisplayText())
	assert.Equal(1012.5, col.GetData()[1].GetScore())
	assert.Equal(int64(2), col.GetMeta().GetTotal())
	assert.Equal(int64(25), col.GetMeta().GetLimit())
}

func TestSearchAnnotationHandlerPassesThroughQueryAndLimit(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("  AX4  ", 15),
	)
	assert.NoError(err)
	assert.NotNil(repo.params)
	assert.Equal(
		"ax4",
		repo.params.Query,
		"expect the query to be normalized",
	)
	assert.Equal(15, repo.params.Limit)
}

func TestSearchAnnotationHandlerDefaultsLimitToFifty(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("axe", 0),
	)
	assert.NoError(err)
	assert.NotNil(repo.params)
	assert.Equal(defaultFullSearchLimit, repo.params.Limit)
	assert.Equal(int64(defaultFullSearchLimit), col.GetMeta().GetLimit())
}

func TestSearchAnnotationHandlerRejectsNilData(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		&annotation.AnnotationSearchParameters{},
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsNilAttributes(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		&annotation.AnnotationSearchParameters{
			Data: &annotation.AnnotationSearchParameters_Data{
				Type: "annotation",
			},
		},
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsShortQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("a", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsPaddedShortQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	// " a " passes the raw min_len = 2 check, but only one searchable
	// character remains after normalization.
	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith(" a ", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsMarkOnlyQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	// "a\u0301" has two raw runes, but strips down to one rune after
	// combining-mark removal.
	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("a\u0301", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsWhitespaceQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("   ", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsStopwordOnlyQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("the and", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerRejectsLimitAboveCap(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("axe", 51),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestSearchAnnotationHandlerReturnsEmptyCollection(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("axe", 0),
	)
	assert.NoError(err)
	assert.NotNil(col.GetMeta())
	assert.Equal(int64(0), col.GetMeta().GetTotal())
	assert.Equal(int64(defaultFullSearchLimit), col.GetMeta().GetLimit())
	assert.Empty(col.GetData())
}

func TestSearchAnnotationHandlerMapsRepositoryError(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{err: errors.New("search index is down")}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchAnnotation(
		context.Background(),
		fullSearchParamsWith("axe", 10),
	)
	assert.Equal(codes.Internal, status.Code(err))
	assert.Contains(status.Convert(err).Message(), "search index is down")
}

func TestFullSearchFieldMapping(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM,
		toAnnotationSearchField("synonym"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE,
		toAnnotationSearchField("genotype"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYSTEMATIC_NAME,
		toAnnotationSearchField("systematic_name"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENETIC_MODIFICATION,
		toAnnotationSearchField("genetic_modification"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_MUTAGENESIS_METHOD,
		toAnnotationSearchField("mutagenesis_method"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PHENOTYPE,
		toAnnotationSearchField("phenotype"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_ENVIRONMENT,
		toAnnotationSearchField("environment"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_ASSAY,
		toAnnotationSearchField("assay"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_CHARACTERISTIC,
		toAnnotationSearchField("characteristic"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_PUBLIC_NOTE,
		toAnnotationSearchField("public_note"),
	)
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_UNSPECIFIED,
		toAnnotationSearchField("description"),
	)
}
