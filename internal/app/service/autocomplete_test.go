package service

import (
	"context"
	"errors"
	"testing"

	IOE "github.com/IBM/fp-go/v2/ioeither"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubAutocompleteRepo records the autocomplete request it received and
// returns a canned result, so the handler contract is observable without a
// database.
type stubAutocompleteRepo struct {
	repository.TaggedAnnotationRepository
	params *repository.AutocompleteQuery
	rows   []*repository.Suggestion
	err    error
	calls  int
}

// AutocompleteAnnotation implements the repository method under test.
func (s *stubAutocompleteRepo) AutocompleteAnnotation(
	params *repository.AutocompleteQuery,
) IOE.IOEither[error, []*repository.Suggestion] {
	s.calls++
	s.params = params
	if s.err != nil {
		return IOE.Left[[]*repository.Suggestion](s.err)
	}

	return IOE.Of[error](s.rows)
}

// newAutocompleteTestService builds an annotation service over the given
// repository stub.
func newAutocompleteTestService(
	t *testing.T,
	repo repository.TaggedAnnotationRepository,
) *AnnotationService {
	t.Helper()
	svc, err := NewAnnotationService(&Params{
		Repository: repo,
		Publisher:  &MockTaggedMessage{},
		Options: []aphgrpc.Option{
			aphgrpc.TopicsOption(map[string]string{
				"annotationCreate": "annotation_create_test",
				"annotationUpdate": "annotation_update_test",
			}),
		},
		Group: "annotation_group_test",
	})
	require.NoError(t, err, "expect no error creating annotation service")

	return svc
}

// autocompleteParamsWith builds a request around a query and a limit.
func autocompleteParamsWith(
	query string,
	limit int64,
) *annotation.AnnotationAutocompleteParameters {
	return &annotation.AnnotationAutocompleteParameters{
		Data: &annotation.AnnotationAutocompleteParameters_Data{
			Type: "annotation",
			Attributes: &annotation.AnnotationAutocompleteAttributes{
				Query: query,
				Limit: limit,
			},
		},
	}
}

func TestAutocompleteAnnotationHandlerMapsSuggestions(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{
		rows: []*repository.Suggestion{
			{
				EntryID:     "DBS0236126",
				Field:       "genotype",
				DisplayText: "axeA1, axeB1",
				Score:       1000,
			},
		},
	}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("axe", 5),
	)
	assert.NoError(err)
	assert.Len(col.GetData(), 1)
	assert.Equal("DBS0236126", col.GetData()[0].GetEntryId())
	assert.Equal(
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE,
		col.GetData()[0].GetField(),
	)
	assert.Equal("axeA1, axeB1", col.GetData()[0].GetDisplayText())
	assert.Equal(float64(1000), col.GetData()[0].GetScore())
	assert.Equal(int64(1), col.GetMeta().GetTotal())
	assert.Equal(int64(5), col.GetMeta().GetLimit())
}

func TestAutocompleteAnnotationHandlerPassesThroughQueryAndLimit(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("  axe  ", 7),
	)
	assert.NoError(err)
	assert.NotNil(repo.params)
	assert.Equal("axe", repo.params.Query, "expect the query to be trimmed")
	assert.Equal(7, repo.params.Limit)
}

func TestAutocompleteAnnotationHandlerDefaultsLimitToFive(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("axe", 0),
	)
	assert.NoError(err)
	assert.NotNil(repo.params)
	assert.Equal(defaultAutocompleteLimit, repo.params.Limit)
	assert.Equal(int64(defaultAutocompleteLimit), col.GetMeta().GetLimit())
}

func TestAutocompleteAnnotationHandlerRejectsNilData(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		&annotation.AnnotationAutocompleteParameters{},
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestAutocompleteAnnotationHandlerRejectsNilAttributes(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		&annotation.AnnotationAutocompleteParameters{
			Data: &annotation.AnnotationAutocompleteParameters_Data{
				Type: "annotation",
			},
		},
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestAutocompleteAnnotationHandlerRejectsShortQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("ax", 5),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestAutocompleteAnnotationHandlerRejectsWhitespaceQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("   ", 5),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestAutocompleteAnnotationHandlerRejectsLimitAboveCap(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("axe", 51),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestAutocompleteAnnotationHandlerReturnsEmptyCollection(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("axe", 0),
	)
	assert.NoError(err)
	assert.NotNil(col.GetMeta())
	assert.Equal(int64(0), col.GetMeta().GetTotal())
	assert.Equal(int64(defaultAutocompleteLimit), col.GetMeta().GetLimit())
	assert.Empty(col.GetData())
}

func TestAutocompleteAnnotationHandlerMapsRepositoryError(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{err: errors.New("search index is down")}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("axe", 5),
	)
	assert.Equal(codes.Internal, status.Code(err))
	assert.Contains(status.Convert(err).Message(), "search index is down")
}

func TestAutocompleteFieldMapping(t *testing.T) {
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
		annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_UNSPECIFIED,
		toAnnotationSearchField("description"),
	)
}

func TestAutocompleteAnnotationHandlerRejectsPaddedShortQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	// " a " passes the protobuf minimum of three characters, but only one
	// searchable character remains after normalization.
	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith(" a ", 5),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestAutocompleteAnnotationHandlerRejectsMarkOnlyQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubAutocompleteRepo{}
	svc := newAutocompleteTestService(t, repo)

	// Three combining marks are three characters to the protobuf validator and
	// none after accent folding, so the request must fail as a client fault
	// rather than reach the repository.
	_, err := svc.AutocompleteAnnotation(
		context.Background(),
		autocompleteParamsWith("\u0301\u0301\u0301", 5),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}
