package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	IOE "github.com/IBM/fp-go/v2/ioeither"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubFullSearchRepo records the full search request it received and returns a
// canned result, so the handler contract is observable without a database.
type stubFullSearchRepo struct {
	repository.TaggedAnnotationRepository
	params          *repository.FullSearchQuery
	rows            []*repository.FullSearchResult
	phenotypeParams *repository.PhenotypeSearchQuery
	phenotypeRows   []*repository.PhenotypeSearchResult
	docs            []*model.AnnoDoc
	groups          []*model.AnnoGroup
	err             error
	calls           int
	gotCtx          context.Context
}

// SearchAnnotation implements the repository method under test.
func (s *stubFullSearchRepo) SearchAnnotation(
	ctx context.Context,
	params *repository.FullSearchQuery,
) IOE.IOEither[error, []*repository.FullSearchResult] {
	s.calls++
	s.gotCtx = ctx
	s.params = params
	if s.err != nil {
		return IOE.Left[[]*repository.FullSearchResult](s.err)
	}

	return IOE.Of[error](s.rows)
}

// SearchPhenotypes implements the repository method under test.
func (s *stubFullSearchRepo) SearchPhenotypes(
	ctx context.Context,
	params *repository.PhenotypeSearchQuery,
) IOE.IOEither[error, []*repository.PhenotypeSearchResult] {
	s.calls++
	s.gotCtx = ctx
	s.phenotypeParams = params
	if s.err != nil {
		return IOE.Left[[]*repository.PhenotypeSearchResult](s.err)
	}

	return IOE.Of[error](s.phenotypeRows)
}

func (s *stubFullSearchRepo) ListAnnotations(
	ctx context.Context,
	_ *repository.ListAnnotationsParams,
) ([]*model.AnnoDoc, error) {
	s.gotCtx = ctx

	return s.docs, s.err
}

func (s *stubFullSearchRepo) ListAnnotationGroup(
	ctx context.Context,
	_, _ int64,
	_ string,
) ([]*model.AnnoGroup, error) {
	s.gotCtx = ctx

	return s.groups, s.err
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

func TestSearchAnnotationHandlerForwardsContextToRepository(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)
	ctx := t.Context()

	_, err := svc.SearchAnnotation(ctx, fullSearchParamsWith("fruiting", 10))
	assert.NoError(err)
	assert.Equal(ctx, repo.gotCtx, "expect handler to forward caller context to SearchAnnotation")

	canceledSvc := newAutocompleteTestService(t, &stubFullSearchRepo{err: fmt.Errorf("wrapped: %w", context.Canceled)})
	_, err = canceledSvc.SearchAnnotation(ctx, fullSearchParamsWith("fruiting", 10))
	assert.Equal(codes.Canceled, status.Code(err), "expect Canceled status code for context.Canceled")

	deadlineSvc := newAutocompleteTestService(t, &stubFullSearchRepo{err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded)})
	_, err = deadlineSvc.SearchAnnotation(ctx, fullSearchParamsWith("fruiting", 10))
	assert.Equal(codes.DeadlineExceeded, status.Code(err), "expect DeadlineExceeded status code for context.DeadlineExceeded")
}

func TestListAnnotationHandlersForwardContextToRepository(t *testing.T) {
	t.Parallel()
	doc := &model.AnnoDoc{Value: "val", EnrtyID: "DBS0236126"}
	repo := &stubFullSearchRepo{
		docs:   []*model.AnnoDoc{doc},
		groups: []*model.AnnoGroup{{GroupID: "g1", AnnoDocs: []*model.AnnoDoc{doc}}},
	}
	svc := newAutocompleteTestService(t, repo)

	t.Run("ListAnnotations", func(t *testing.T) {
		ctx := t.Context()
		_, err := svc.ListAnnotations(ctx, &annotation.ListParameters{
			Limit:  10,
			Filter: "tag==synonym;ontology==dicty_annotation",
		})
		require.NoError(t, err)
		require.Equal(t, ctx, repo.gotCtx, "expect ListAnnotations to forward caller context")
	})

	t.Run("ListAnnotationGroups", func(t *testing.T) {
		ctx := t.Context()
		_, err := svc.ListAnnotationGroups(ctx, &annotation.ListGroupParameters{Limit: 10})
		require.NoError(t, err)
		require.Equal(t, ctx, repo.gotCtx, "expect ListAnnotationGroups to forward caller context")
	})
}
