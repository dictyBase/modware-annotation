package service

import (
	"context"
	"strings"

	E "github.com/IBM/fp-go/v2/either"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	N "github.com/IBM/fp-go/v2/number"
	L "github.com/IBM/fp-go/v2/optics/lens"
	O "github.com/IBM/fp-go/v2/option"
	P "github.com/IBM/fp-go/v2/predicate"
	"github.com/bufbuild/protovalidate-go"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// msgEmptyAutocompleteQuery is the client-facing message for a query that
	// holds no searchable text once it is trimmed.
	msgEmptyAutocompleteQuery = "expect a non-empty autocomplete query"
	// defaultAutocompleteLimit is the list length a non-positive request gets.
	defaultAutocompleteLimit = 5
	// fieldSynonym and fieldGenotype are the tag labels the search reports.
	fieldSynonym  = "synonym"
	fieldGenotype = "genotype"
)

type (
	// autocompleteInput is the boundary record of the handler: the request and
	// the repository travel as data rather than being captured in a closure.
	autocompleteInput struct {
		request *annotation.AnnotationAutocompleteParameters
		repo    repository.TaggedAnnotationRepository
	}

	// autocompleteParams is the validated and normalized search input.
	autocompleteParams struct {
		query string
		limit int
	}

	// autocompleteSearch is the state of one handler run, once the query is
	// normalized and the repository call has been made.
	autocompleteSearch struct {
		repo  repository.TaggedAnnotationRepository
		query *repository.AutocompleteQuery
		rows  []*repository.Suggestion
	}

	// autocompleteResult is the boundary record the handler folds into.
	autocompleteResult struct {
		collection *annotation.AnnotationSuggestionCollection
		err        error
	}
)

var (
	// rowsLens focuses the suggestions of a handler run, so IOE.Bind can
	// attach the repository result to the state.
	rowsLens = L.MakeLens(
		func(s autocompleteSearch) []*repository.Suggestion { return s.rows },
		func(
			s autocompleteSearch,
			rows []*repository.Suggestion,
		) autocompleteSearch {
			s.rows = rows
			return s
		},
	)

	// isPositiveLimit guards the limit default: a non-positive request becomes
	// the service default.
	isPositiveLimit = N.MoreThan(0)

	// hasAutocompleteQueryText reports whether the trimmed query still holds
	// text, which is the one guard the request validator cannot express.
	hasAutocompleteQueryText = F.Pipe1(
		predord.MinStrLen(1),
		P.ContraMap(autocompleteQueryText),
	)
)

// AutocompleteAnnotation returns annotation suggestions for a partial strain
// synonym or genotype value. Guards are pure Either, the repository call is the
// only effect, and the whole program is forced once at the boundary.
func (srv *AnnotationService) AutocompleteAnnotation(
	_ context.Context,
	r *annotation.AnnotationAutocompleteParameters,
) (*annotation.AnnotationSuggestionCollection, error) {
	build := F.Pipe4(
		IOE.Of[error](autocompleteInput{request: r, repo: srv.repo}),
		IOE.ChainEitherK(validatedAutocompleteSearch),
		IOE.Bind(rowsLens.Set, searchSuggestions),
		IOE.Map[error](toSuggestionCollection),
		ioeutils.ToEither[error, *annotation.AnnotationSuggestionCollection],
	)
	result := F.Pipe1(build, E.Fold(
		failedAutocompleteResult,
		successfulAutocompleteResult,
	))

	return result.collection, result.err
}

// validatedAutocompleteSearch is the pure guard chain of the handler. It stays
// an Either: every fault in it is a client fault, so one mapper at the end
// stamps the status class and no caller has to force it.
func validatedAutocompleteSearch(
	in autocompleteInput,
) E.Either[error, autocompleteSearch] {
	return F.Pipe4(
		E.TryCatchError(in.request, protovalidate.Validate(in.request)),
		E.Map[error](newAutocompleteParams),
		E.Chain(E.FromPredicate(
			hasAutocompleteQueryText,
			invalidParamStatusOf[autocompleteParams](
				msgEmptyAutocompleteQuery,
			),
		)),
		E.Map[error](F.Bind1st(newAutocompleteSearch, in.repo)),
		E.MapLeft[autocompleteSearch](invalidParamStatus),
	)
}

// newAutocompleteParams trims the request query and turns a non-positive limit
// into the service default, so the repository call and the response metadata
// always agree.
func newAutocompleteParams(
	r *annotation.AnnotationAutocompleteParameters,
) autocompleteParams {
	attrs := r.GetData().GetAttributes()
	requested := int(attrs.GetLimit())
	effective := F.Pipe2(
		requested,
		O.FromPredicate(isPositiveLimit),
		O.GetOrElse(F.Constant(defaultAutocompleteLimit)),
	)
	rawQuery := attrs.GetQuery()
	query := strings.TrimSpace(rawQuery)

	return autocompleteParams{
		query: query,
		limit: effective,
	}
}

// autocompleteQueryText extracts the query text of normalized parameters.
func autocompleteQueryText(params autocompleteParams) string {
	return params.query
}

// newAutocompleteSearch starts one handler run against a repository.
func newAutocompleteSearch(
	repo repository.TaggedAnnotationRepository,
	params autocompleteParams,
) autocompleteSearch {
	return autocompleteSearch{
		repo: repo,
		query: &repository.AutocompleteQuery{
			Query: params.query,
			Limit: params.limit,
		},
	}
}

// searchSuggestions runs the repository search. The fault class is decided
// here, at the step that raises it.
func searchSuggestions(
	s autocompleteSearch,
) IOE.IOEither[error, []*repository.Suggestion] {
	found := s.repo.AutocompleteAnnotation(s.query)

	return F.Pipe1(
		found,
		IOE.MapLeft[[]*repository.Suggestion](serverErrorStatus),
	)
}

// toSuggestionCollection projects a completed handler run onto the response.
func toSuggestionCollection(
	s autocompleteSearch,
) *annotation.AnnotationSuggestionCollection {
	collection := make([]*annotation.AnnotationSuggestion, 0, len(s.rows))
	for _, row := range s.rows {
		collection = append(collection, &annotation.AnnotationSuggestion{
			EntryId:     row.EntryID,
			Field:       toAnnotationSearchField(row.Field),
			DisplayText: row.DisplayText,
			Score:       row.Score,
		})
	}

	return &annotation.AnnotationSuggestionCollection{
		Data: collection,
		Meta: &annotation.Meta{
			Limit: int64(s.query.Limit),
			Total: int64(len(s.rows)),
		},
	}
}

// toAnnotationSearchField maps a repository field label onto the proto enum.
func toAnnotationSearchField(field string) annotation.AnnotationSearchField {
	switch field {
	case fieldSynonym:
		return annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_SYNONYM
	case fieldGenotype:
		return annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_GENOTYPE
	default:
		return annotation.AnnotationSearchField_ANNOTATION_SEARCH_FIELD_UNSPECIFIED
	}
}

// failedAutocompleteResult is the boundary projection of a failed handler run.
func failedAutocompleteResult(err error) autocompleteResult {
	return autocompleteResult{err: err}
}

// successfulAutocompleteResult is the boundary projection of a completed
// handler run.
func successfulAutocompleteResult(
	collection *annotation.AnnotationSuggestionCollection,
) autocompleteResult {
	return autocompleteResult{collection: collection}
}

// invalidParamStatus maps a client fault onto the InvalidArgument status the
// caller sees. status.Convert keeps the message of a fault that already carries
// a status, so a guard status is never nested inside a second one.
func invalidParamStatus(err error) error {
	return status.Error(codes.InvalidArgument, status.Convert(err).Message())
}

// serverErrorStatus maps a repository fault onto the Internal status the caller
// sees.
func serverErrorStatus(err error) error {
	return status.Error(codes.Internal, err.Error())
}

// invalidParamStatusOf is a guard's error factory: it drops the rejected value
// and returns a fixed InvalidArgument status.
func invalidParamStatusOf[A any](msg string) func(A) error {
	return func(A) error { return status.Error(codes.InvalidArgument, msg) }
}
