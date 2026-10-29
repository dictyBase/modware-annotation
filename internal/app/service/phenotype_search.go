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
	R "github.com/IBM/fp-go/v2/record"
	S "github.com/IBM/fp-go/v2/string"
	"github.com/bufbuild/protovalidate-go"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

const (
	fieldPhenotypeLabel      = "label"
	fieldPhenotypeDefinition = "definition"
	fieldPhenotypeComment    = "comment"
)

type (
	// phenotypeSearchInput is the boundary record of the handler: the caller
	// context, the request, and the repository travel as data rather than
	// being captured in a closure.
	phenotypeSearchInput struct {
		ctx     context.Context
		request *annotation.PhenotypeSearchParameters
		repo    repository.TaggedAnnotationRepository
	}

	// phenotypeSearchParams is the validated and normalized search input.
	phenotypeSearchParams struct {
		query string
		limit int
	}

	// phenotypeSearchState is the state of one handler run, once the query is
	// normalized and the repository call has been made.
	phenotypeSearchState struct {
		ctx   context.Context
		repo  repository.TaggedAnnotationRepository
		query *repository.PhenotypeSearchQuery
		rows  []*repository.PhenotypeSearchResult
	}

	// phenotypeSearchOutcome is the boundary record the handler folds into.
	phenotypeSearchOutcome struct {
		collection *annotation.PhenotypeSearchResultCollection
		err        error
	}
)

var (
	// phenotypeSearchRowsLens focuses the search results of a handler run, so
	// IOE.Bind can attach the repository result to the state.
	phenotypeSearchRowsLens = L.MakeLens(
		func(s phenotypeSearchState) []*repository.PhenotypeSearchResult { return s.rows },
		func(
			s phenotypeSearchState,
			rows []*repository.PhenotypeSearchResult,
		) phenotypeSearchState {
			s.rows = rows
			return s
		},
	)

	// hasPhenotypeSearchQueryText reports whether the normalized query still
	// holds text after whitespace and combining-mark stripping.
	hasPhenotypeSearchQueryText = F.Pipe1(
		predord.MinStrLen(1),
		P.ContraMap(phenotypeSearchQueryText),
	)

	// hasLongEnoughPhenotypeSearchQuery reports whether the normalized query
	// reaches the documented minimum length in characters.
	hasLongEnoughPhenotypeSearchQuery = F.Pipe2(
		predarrays.MinLen[rune](minFullSearchQueryLength),
		P.ContraMap(S.ToRunes),
		P.ContraMap(phenotypeSearchQueryText),
	)

	// hasNonStopwordPhenotypeSearchQuery reports whether the normalized query
	// retains at least one non-stopword word.
	hasNonStopwordPhenotypeSearchQuery = F.Pipe2(
		repository.IsAllStopwords,
		P.Not,
		P.ContraMap(phenotypeSearchQueryText),
	)

	emptyPhenotypeQueryErr    = invalidParamStatusOf[phenotypeSearchParams](msgEmptyFullSearchQuery)
	shortPhenotypeQueryErr    = invalidParamStatusOf[phenotypeSearchParams](msgShortFullSearchQuery)
	stopwordPhenotypeQueryErr = invalidParamStatusOf[phenotypeSearchParams](msgStopwordFullSearchQuery)

	checkPhenotypeQueryText     = E.FromPredicate(hasPhenotypeSearchQueryText, emptyPhenotypeQueryErr)
	checkPhenotypeQueryLength   = E.FromPredicate(hasLongEnoughPhenotypeSearchQuery, shortPhenotypeQueryErr)
	checkPhenotypeQueryStopword = E.FromPredicate(hasNonStopwordPhenotypeSearchQuery, stopwordPhenotypeQueryErr)

	searchPhenotypeHits = F.Flow2(
		rawSearchPhenotypeHits,
		IOE.MapLeft[[]*repository.PhenotypeSearchResult](serverErrorStatus),
	)

	phenotypeSearchFieldMap = map[string]annotation.PhenotypeSearchField{
		fieldPhenotypeLabel:      annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_LABEL,
		fieldSynonym:             annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_SYNONYM,
		fieldPhenotypeDefinition: annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_DEFINITION,
		fieldPhenotypeComment:    annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_COMMENT,
	}

	// lookupPhenotypeSearchField awaits the canonical field label after the
	// enum map, because record.Lookup is curried key-first.
	lookupPhenotypeSearchField = F.Pipe1(
		phenotypeSearchFieldMap,
		F.Flip(R.Lookup[annotation.PhenotypeSearchField, string]),
	)

	toPhenotypeSearchField = F.Flow2(
		lookupPhenotypeSearchField,
		O.GetOrElse(F.Constant(
			annotation.PhenotypeSearchField_PHENOTYPE_SEARCH_FIELD_UNSPECIFIED,
		)),
	)
)

// SearchPhenotypes returns at most 50 ranked strain phenotype matches
// for a search query of at least 2 normalized characters.
func (srv *AnnotationService) SearchPhenotypes(
	ctx context.Context,
	r *annotation.PhenotypeSearchParameters,
) (*annotation.PhenotypeSearchResultCollection, error) {
	seed := phenotypeSearchInput{ctx: ctx, request: r, repo: srv.repo}
	result := F.Pipe5(
		IOE.Of[error](seed),
		IOE.ChainEitherK(validatedPhenotypeSearch),
		IOE.Bind(phenotypeSearchRowsLens.Set, searchPhenotypeHits),
		IOE.Map[error](toPhenotypeSearchResultCollection),
		ioeutils.ToEither[error, *annotation.PhenotypeSearchResultCollection],
		E.Fold(failedPhenotypeSearchOutcome, successfulPhenotypeSearchOutcome),
	)

	return result.collection, result.err
}

// validatedPhenotypeSearch is the pure guard chain of the phenotype search handler.
func validatedPhenotypeSearch(
	in phenotypeSearchInput,
) E.Either[error, phenotypeSearchState] {
	valErr := protovalidate.Validate(in.request)
	toState := F.Bind1st(newPhenotypeSearchState, in)

	return F.Pipe6(
		E.TryCatchError(in.request, valErr),
		E.Map[error](newPhenotypeSearchParams),
		E.Chain(checkPhenotypeQueryText),
		E.Chain(checkPhenotypeQueryLength),
		E.Chain(checkPhenotypeQueryStopword),
		E.Map[error](toState),
		E.MapLeft[phenotypeSearchState](invalidParamStatus),
	)
}

// newPhenotypeSearchParams normalizes the request query and turns a non-positive
// limit into the service default.
func newPhenotypeSearchParams(
	r *annotation.PhenotypeSearchParameters,
) phenotypeSearchParams {
	attrs := r.GetData().GetAttributes()
	requested := int(attrs.GetLimit())
	effective := F.Pipe2(
		requested,
		O.FromPredicate(isPositiveLimit),
		O.GetOrElse(F.Constant(defaultFullSearchLimit)),
	)
	rawQuery := attrs.GetQuery()
	query := repository.NormalizeAutocompleteQuery(rawQuery)

	return phenotypeSearchParams{
		query: query,
		limit: effective,
	}
}

// phenotypeSearchQueryText extracts the query text of normalized parameters.
func phenotypeSearchQueryText(params phenotypeSearchParams) string {
	return params.query
}

// newPhenotypeSearchState starts one phenotype search handler run against a repository.
func newPhenotypeSearchState(
	in phenotypeSearchInput,
	params phenotypeSearchParams,
) phenotypeSearchState {
	return phenotypeSearchState{
		ctx:  in.ctx,
		repo: in.repo,
		query: &repository.PhenotypeSearchQuery{
			Query: params.query,
			Limit: params.limit,
		},
	}
}

// rawSearchPhenotypeHits invokes the repository phenotype search.
func rawSearchPhenotypeHits(
	s phenotypeSearchState,
) IOE.IOEither[error, []*repository.PhenotypeSearchResult] {
	return s.repo.SearchPhenotypes(s.ctx, s.query)
}

// toProtoPhenotypeSearchResult maps one repository phenotype search result onto
// its protobuf wire representation.
func toProtoPhenotypeSearchResult(
	row *repository.PhenotypeSearchResult,
) *annotation.PhenotypeSearchResult {
	field := toPhenotypeSearchField(row.Field)

	return &annotation.PhenotypeSearchResult{
		EntryId:    row.EntryID,
		Field:      field,
		Name:       row.Name,
		Definition: row.Definition,
		Score:      row.Score,
	}
}

// toPhenotypeSearchResultCollection projects a completed handler run onto the
// protobuf response collection.
func toPhenotypeSearchResultCollection(
	s phenotypeSearchState,
) *annotation.PhenotypeSearchResultCollection {
	data := F.Pipe1(s.rows, A.Map(toProtoPhenotypeSearchResult))
	count := len(data)
	limit := int64(s.query.Limit)
	total := int64(count)

	return &annotation.PhenotypeSearchResultCollection{
		Data: data,
		Meta: &annotation.Meta{
			Limit: limit,
			Total: total,
		},
	}
}

// failedPhenotypeSearchOutcome is the boundary projection of a failed handler run.
func failedPhenotypeSearchOutcome(err error) phenotypeSearchOutcome {
	return phenotypeSearchOutcome{err: err}
}

// successfulPhenotypeSearchOutcome is the boundary projection of a completed
// handler run.
func successfulPhenotypeSearchOutcome(
	collection *annotation.PhenotypeSearchResultCollection,
) phenotypeSearchOutcome {
	return phenotypeSearchOutcome{collection: collection}
}
