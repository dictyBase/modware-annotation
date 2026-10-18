package service

import (
	"context"

	A "github.com/IBM/fp-go/v2/array"
	E "github.com/IBM/fp-go/v2/either"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	O "github.com/IBM/fp-go/v2/option"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
	"github.com/bufbuild/protovalidate-go"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

const (
	// msgEmptyFullSearchQuery is the client-facing message for a query that
	// holds no searchable text once it is normalized.
	msgEmptyFullSearchQuery = "expect a non-empty search query"
	// msgShortFullSearchQuery is the client-facing message for a query that
	// keeps fewer than 2 searchable characters after normalization.
	msgShortFullSearchQuery = "expect a search query of at least 2 characters"
	// msgStopwordFullSearchQuery is the client-facing message for a query
	// consisting solely of English stopwords.
	msgStopwordFullSearchQuery = "expect a search query with at least one non-stopword"
	// minFullSearchQueryLength is the documented minimum length, in
	// characters, of the normalized query.
	minFullSearchQueryLength = 2
	// defaultFullSearchLimit is the list length a non-positive request gets.
	defaultFullSearchLimit = 50
)

type (
	// fullSearchInput is the boundary record of the handler: the caller
	// context, the request and the repository travel as data rather than being
	// captured in a closure.
	fullSearchInput struct {
		ctx     context.Context
		request *annotation.AnnotationSearchParameters
		repo    repository.TaggedAnnotationRepository
	}

	// fullSearchParams is the validated and normalized search input.
	fullSearchParams struct {
		query string
		limit int
	}

	// fullSearchState is the state of one handler run, once the query is
	// normalized and the repository call has been made.
	fullSearchState struct {
		ctx   context.Context
		repo  repository.TaggedAnnotationRepository
		query *repository.FullSearchQuery
		rows  []*repository.FullSearchResult
	}

	// fullSearchResult is the boundary record the handler folds into.
	fullSearchResult struct {
		collection *annotation.AnnotationSearchResultCollection
		err        error
	}
)

var (
	// fullSearchRowsLens focuses the search results of a handler run, so
	// IOE.Bind can attach the repository result to the state.
	fullSearchRowsLens = L.MakeLens(
		func(s fullSearchState) []*repository.FullSearchResult { return s.rows },
		func(
			s fullSearchState,
			rows []*repository.FullSearchResult,
		) fullSearchState {
			s.rows = rows
			return s
		},
	)

	// hasFullSearchQueryText reports whether the normalized query still holds
	// text after whitespace and combining-mark stripping.
	hasFullSearchQueryText = F.Pipe1(
		predord.MinStrLen(1),
		P.ContraMap(fullSearchQueryText),
	)

	// hasLongEnoughFullSearchQuery reports whether the normalized query reaches
	// the documented minimum length in characters.
	hasLongEnoughFullSearchQuery = F.Pipe2(
		predarrays.MinLen[rune](minFullSearchQueryLength),
		P.ContraMap(S.ToRunes),
		P.ContraMap(fullSearchQueryText),
	)

	// hasNonStopwordFullSearchQuery reports whether the normalized query
	// retains at least one non-stopword word.
	hasNonStopwordFullSearchQuery = F.Pipe2(
		repository.IsAllStopwords,
		P.Not,
		P.ContraMap(fullSearchQueryText),
	)
)

// SearchAnnotation returns at most 50 ranked strain annotation matches for a
// search query of at least 2 normalized characters.
func (srv *AnnotationService) SearchAnnotation(
	ctx context.Context,
	r *annotation.AnnotationSearchParameters,
) (*annotation.AnnotationSearchResultCollection, error) {
	result := F.Pipe5(
		IOE.Of[error](fullSearchInput{ctx: ctx, request: r, repo: srv.repo}),
		IOE.ChainEitherK(validatedFullSearch),
		IOE.Bind(fullSearchRowsLens.Set, searchAnnotationHits),
		IOE.Map[error](toSearchResultCollection),
		ioeutils.ToEither[error, *annotation.AnnotationSearchResultCollection],
		E.Fold(failedFullSearchResult, successfulFullSearchResult),
	)

	return result.collection, result.err
}

// validatedFullSearch is the pure guard chain of the full search handler.
func validatedFullSearch(
	in fullSearchInput,
) E.Either[error, fullSearchState] {
	valErr := protovalidate.Validate(in.request)

	return F.Pipe6(
		E.TryCatchError(in.request, valErr),
		E.Map[error](newFullSearchParams),
		E.Chain(E.FromPredicate(
			hasFullSearchQueryText,
			invalidParamStatusOf[fullSearchParams](
				msgEmptyFullSearchQuery,
			),
		)),
		E.Chain(E.FromPredicate(
			hasLongEnoughFullSearchQuery,
			invalidParamStatusOf[fullSearchParams](
				msgShortFullSearchQuery,
			),
		)),
		E.Chain(E.FromPredicate(
			hasNonStopwordFullSearchQuery,
			invalidParamStatusOf[fullSearchParams](
				msgStopwordFullSearchQuery,
			),
		)),
		E.Map[error](F.Bind1st(newFullSearchState, in)),
		E.MapLeft[fullSearchState](invalidParamStatus),
	)
}

// newFullSearchParams normalizes the request query and turns a non-positive
// limit into the service default.
func newFullSearchParams(
	r *annotation.AnnotationSearchParameters,
) fullSearchParams {
	attrs := r.GetData().GetAttributes()
	requested := int(attrs.GetLimit())
	effective := F.Pipe2(
		requested,
		O.FromPredicate(isPositiveLimit),
		O.GetOrElse(F.Constant(defaultFullSearchLimit)),
	)
	rawQuery := attrs.GetQuery()
	query := repository.NormalizeAutocompleteQuery(rawQuery)

	return fullSearchParams{
		query: query,
		limit: effective,
	}
}

// fullSearchQueryText extracts the query text of normalized parameters.
func fullSearchQueryText(params fullSearchParams) string {
	return params.query
}

// newFullSearchState starts one full search handler run against a repository.
func newFullSearchState(
	in fullSearchInput,
	params fullSearchParams,
) fullSearchState {
	return fullSearchState{
		ctx:  in.ctx,
		repo: in.repo,
		query: &repository.FullSearchQuery{
			Query: params.query,
			Limit: params.limit,
		},
	}
}

// searchAnnotationHits runs the repository full search and maps any repository
// fault onto codes.Internal.
func searchAnnotationHits(
	s fullSearchState,
) IOE.IOEither[error, []*repository.FullSearchResult] {
	found := s.repo.SearchAnnotation(s.ctx, s.query)

	return F.Pipe1(
		found,
		IOE.MapLeft[[]*repository.FullSearchResult](serverErrorStatus),
	)
}

// toAnnotationSearchResult maps one repository full search result onto its
// protobuf wire representation.
func toAnnotationSearchResult(
	row *repository.FullSearchResult,
) *annotation.AnnotationSearchResult {
	field := toAnnotationSearchField(row.Field)

	return &annotation.AnnotationSearchResult{
		EntryId:     row.EntryID,
		Field:       field,
		DisplayText: row.DisplayText,
		Score:       row.Score,
	}
}

// toSearchResultCollection projects a completed handler run onto the protobuf
// response collection.
func toSearchResultCollection(
	s fullSearchState,
) *annotation.AnnotationSearchResultCollection {
	data := F.Pipe1(s.rows, A.Map(toAnnotationSearchResult))
	limit := int64(s.query.Limit)
	total := int64(len(data))

	return &annotation.AnnotationSearchResultCollection{
		Data: data,
		Meta: &annotation.Meta{
			Limit: limit,
			Total: total,
		},
	}
}

// failedFullSearchResult is the boundary projection of a failed handler run.
func failedFullSearchResult(err error) fullSearchResult {
	return fullSearchResult{err: err}
}

// successfulFullSearchResult is the boundary projection of a completed handler
// run.
func successfulFullSearchResult(
	collection *annotation.AnnotationSearchResultCollection,
) fullSearchResult {
	return fullSearchResult{collection: collection}
}
