package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// phenotypeSearchParamsWith builds a phenotype search request around a query
// and a limit.
func phenotypeSearchParamsWith(
	query string,
	limit int64,
) *annotation.PhenotypeSearchParameters {
	return &annotation.PhenotypeSearchParameters{
		Data: &annotation.PhenotypeSearchParameters_Data{
			Type: "phenotype",
			Attributes: &annotation.PhenotypeSearchAttributes{
				Query: query,
				Limit: limit,
			},
		},
	}
}

func TestPhenotypeSearchHandlerMapsResults(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{
		phenotypeRows: []*repository.PhenotypeSearchResult{
			{
				EntryID:    "DBS0236201",
				Field:      "label",
				Name:       "culminant mound formation",
				Definition: "The developmental process that forms the culminant mound.",
				Score:      1012.5,
			},
			{
				EntryID:    "DBS0236202",
				Field:      "definition",
				Name:       "in suspension",
				Definition: "Growth of cells in a shaking liquid culture.",
				Score:      504.2,
			},
		},
	}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("culminant", 25),
	)
	assert.NoError(err)
	assert.Len(col.GetData(), 2)
	assert.Equal("DBS0236201", col.GetData()[0].GetEntryId())
	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
		col.GetData()[0].GetField(),
	)
	assert.Equal("culminant mound formation", col.GetData()[0].GetName())
	assert.Equal(
		"The developmental process that forms the culminant mound.",
		col.GetData()[0].GetDefinition(),
	)
	assert.Equal(1012.5, col.GetData()[0].GetScore())
	assert.Equal("DBS0236202", col.GetData()[1].GetEntryId())
	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_DEFINITION,
		col.GetData()[1].GetField(),
	)
	assert.Equal("in suspension", col.GetData()[1].GetName())
	assert.Equal(
		"Growth of cells in a shaking liquid culture.",
		col.GetData()[1].GetDefinition(),
	)
	assert.Equal(504.2, col.GetData()[1].GetScore())
	assert.Equal(int64(2), col.GetMeta().GetTotal())
	assert.Equal(int64(25), col.GetMeta().GetLimit())
}

func TestPhenotypeSearchHandlerPassesThroughQueryAndLimit(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("  AX4  ", 15),
	)
	assert.NoError(err)
	assert.NotNil(repo.phenotypeParams)
	assert.Equal(
		"ax4",
		repo.phenotypeParams.Query,
		"expect the query to be normalized",
	)
	assert.Equal(15, repo.phenotypeParams.Limit)
}

func TestPhenotypeSearchHandlerDefaultsLimitToFifty(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("axe", 0),
	)
	assert.NoError(err)
	assert.NotNil(repo.phenotypeParams)
	assert.Equal(defaultFullSearchLimit, repo.phenotypeParams.Limit)
	assert.Equal(int64(defaultFullSearchLimit), col.GetMeta().GetLimit())
}

func TestPhenotypeSearchHandlerRejectsNilData(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchPhenotypes(
		context.Background(),
		&annotation.PhenotypeSearchParameters{},
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestPhenotypeSearchHandlerRejectsNilAttributes(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchPhenotypes(
		context.Background(),
		&annotation.PhenotypeSearchParameters{
			Data: &annotation.PhenotypeSearchParameters_Data{
				Type: "phenotype",
			},
		},
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestPhenotypeSearchHandlerRejectsShortQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("a", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestPhenotypeSearchHandlerRejectsPaddedShortQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	// " a " passes the raw min_len = 2 check, but only one searchable
	// character remains after normalization.
	_, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith(" a ", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestPhenotypeSearchHandlerRejectsMarkOnlyQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	// "a\u0301" has two raw runes, but strips down to one rune after
	// combining-mark removal.
	_, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("a\u0301", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestPhenotypeSearchHandlerRejectsStopwordOnlyQuery(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("the and", 10),
	)
	assert.Equal(codes.InvalidArgument, status.Code(err))
	assert.Zero(repo.calls, "expect no repository call")
}

func TestPhenotypeSearchHandlerReturnsEmptyCollection(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)

	col, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("axe", 0),
	)
	assert.NoError(err)
	assert.NotNil(col.GetMeta())
	assert.Equal(int64(0), col.GetMeta().GetTotal())
	assert.Equal(int64(defaultFullSearchLimit), col.GetMeta().GetLimit())
	assert.Empty(col.GetData())
}

func TestPhenotypeSearchHandlerMapsRepositoryError(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{err: errors.New("search index is down")}
	svc := newAutocompleteTestService(t, repo)

	_, err := svc.SearchPhenotypes(
		context.Background(),
		phenotypeSearchParamsWith("axe", 10),
	)
	assert.Equal(codes.Internal, status.Code(err))
	assert.Contains(status.Convert(err).Message(), "search index is down")
}

func TestPhenotypeSearchFieldMapping(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
		toPhenotypeSearchField("label"),
	)
	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_SYNONYM,
		toPhenotypeSearchField("synonym"),
	)
	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_DEFINITION,
		toPhenotypeSearchField("definition"),
	)
	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_COMMENT,
		toPhenotypeSearchField("comment"),
	)
	assert.Equal(
		annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_UNSPECIFIED,
		toPhenotypeSearchField("description"),
	)
}

func TestPhenotypeSearchHandlerForwardsContextToRepository(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	repo := &stubFullSearchRepo{}
	svc := newAutocompleteTestService(t, repo)
	ctx := t.Context()

	_, err := svc.SearchPhenotypes(ctx, phenotypeSearchParamsWith("culminant", 10))
	assert.NoError(err)
	assert.Equal(ctx, repo.gotCtx, "expect handler to forward caller context to SearchPhenotypes")

	canceledSvc := newAutocompleteTestService(
		t,
		&stubFullSearchRepo{err: fmt.Errorf("wrapped: %w", context.Canceled)},
	)
	_, err = canceledSvc.SearchPhenotypes(ctx, phenotypeSearchParamsWith("culminant", 10))
	assert.Equal(
		codes.Canceled,
		status.Code(err),
		"expect Canceled status code for context.Canceled",
	)

	deadlineSvc := newAutocompleteTestService(
		t,
		&stubFullSearchRepo{err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded)},
	)
	_, err = deadlineSvc.SearchPhenotypes(
		ctx,
		phenotypeSearchParamsWith("culminant", 10),
	)
	assert.Equal(
		codes.DeadlineExceeded,
		status.Code(err),
		"expect DeadlineExceeded status code for context.DeadlineExceeded",
	)
}
