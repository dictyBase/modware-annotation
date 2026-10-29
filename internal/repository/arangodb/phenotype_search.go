package arangodb

import (
	"context"
	"fmt"

	A "github.com/IBM/fp-go/v2/array"
	E "github.com/IBM/fp-go/v2/either"
	fperrors "github.com/IBM/fp-go/v2/errors"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	O "github.com/IBM/fp-go/v2/option"
	P "github.com/IBM/fp-go/v2/predicate"
	eithererr "github.com/dictyBase/fp-go-loom/eithererr"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	predord "github.com/dictyBase/fp-go-loom/predicate/ord"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
)

type (
	// phenotypeSearchTargets holds the resolved ontology cv._id values
	// returned by phenotypeSearchTargetsQ.
	phenotypeSearchTargets struct {
		Graphs map[string]string `json:"graphs"`
	}

	// phenotypeSearchRequest is the validated state of one phenotype search call.
	phenotypeSearchRequest struct {
		ctx     context.Context
		query   string
		limit   int
		targets phenotypeSearchTargets
	}

	// phenotypeSearchQueryParams holds the finished AQL statement and bind map.
	phenotypeSearchQueryParams struct {
		ctx        context.Context
		statement  string
		bindParams map[string]any
	}

	// phenotypeSearchRow is one raw row of the phenotype search projection.
	phenotypeSearchRow struct {
		ID         string  `json:"id"`
		Field      string  `json:"f"`
		Name       string  `json:"name"`
		Definition string  `json:"def"`
		Score      float64 `json:"s"`
	}
)

const (
	msgNilPhenotypeSearchQuery   = "expect a non-nil phenotype search query"
	msgEmptyPhenotypeSearchQuery = "expect a non-empty phenotype search query"
)

var (
	phenotypeSearchCtxLens = L.MakeLens(
		func(r phenotypeSearchRequest) context.Context { return r.ctx },
		func(r phenotypeSearchRequest, ctx context.Context) phenotypeSearchRequest {
			r.ctx = ctx
			return r
		},
	)
	phenotypeSearchQueryLens = L.MakeLens(
		func(r phenotypeSearchRequest) string { return r.query },
		func(r phenotypeSearchRequest, q string) phenotypeSearchRequest {
			r.query = q
			return r
		},
	)
	phenotypeSearchLimitLens = L.MakeLens(
		func(r phenotypeSearchRequest) int { return r.limit },
		func(r phenotypeSearchRequest, l int) phenotypeSearchRequest {
			r.limit = l
			return r
		},
	)
	phenotypeSearchTargetsLens = L.MakeLens(
		func(r phenotypeSearchRequest) phenotypeSearchTargets { return r.targets },
		func(r phenotypeSearchRequest, t phenotypeSearchTargets) phenotypeSearchRequest {
			r.targets = t
			return r
		},
	)
	phenotypeSearchParamsCtxLens = L.MakeLens(
		func(p phenotypeSearchQueryParams) context.Context { return p.ctx },
		func(
			p phenotypeSearchQueryParams,
			ctx context.Context,
		) phenotypeSearchQueryParams {
			p.ctx = ctx
			return p
		},
	)
	phenotypeSearchStmtLens = L.MakeLens(
		func(p phenotypeSearchQueryParams) string { return p.statement },
		func(p phenotypeSearchQueryParams, s string) phenotypeSearchQueryParams {
			p.statement = s
			return p
		},
	)
	phenotypeSearchBindLens = L.MakeLens(
		func(p phenotypeSearchQueryParams) map[string]any { return p.bindParams },
		func(
			p phenotypeSearchQueryParams,
			b map[string]any,
		) phenotypeSearchQueryParams {
			p.bindParams = b
			return p
		},
	)

	hasPhenotypeRequestText = F.Pipe1(
		predord.MinStrLen(1),
		P.ContraMap(phenotypeSearchQueryLens.Get),
	)
	keepsPhenotypeNonStopword = F.Pipe2(
		repo.IsAllStopwords,
		P.Not,
		P.ContraMap(phenotypeSearchQueryLens.Get),
	)

	isPresentPhenotypeSearchQuery = P.IsNonZero[*repo.PhenotypeSearchQuery]()
	nilPhenotypeQueryErr          = eithererr.ConstErr[*repo.PhenotypeSearchQuery](msgNilPhenotypeSearchQuery)
	emptyPhenotypeRequestErr      = eithererr.ConstErr[phenotypeSearchRequest](msgEmptyPhenotypeSearchQuery)
	checkPhenotypeQueryNotNil     = E.FromPredicate(isPresentPhenotypeSearchQuery, nilPhenotypeQueryErr)
	checkPhenotypeNotEmpty        = E.FromPredicate(hasPhenotypeRequestText, emptyPhenotypeRequestErr)
	checkPhenotypeNotStopword     = E.FromPredicate(keepsPhenotypeNonStopword, stopwordPhenotypeSearchError)

	isNonEmptyPhenotypeSearchRows = predarrays.IsNonEmpty[phenotypeSearchRow]()
	mapPhenotypeSearchRows        = A.Map(toPhenotypeSearchResult)
	toPhenotypeSearchResults      = F.Flow2(
		O.FromPredicate(isNonEmptyPhenotypeSearchRows),
		O.Fold(emptyPhenotypeSearchResults, mapPhenotypeSearchRows),
	)

	runPhenotypeSearchTargetsFault = fperrors.OnError(
		"error in resolving phenotype search targets",
	)
	readPhenotypeSearchTargetsFault = fperrors.OnError(
		"error in reading phenotype search targets",
	)
	runPhenotypeSearchQueryFault = fperrors.OnError(
		"error in running phenotype search query",
	)
)

// stopwordPhenotypeSearchError reports a query whose tokens are all English stopwords.
func stopwordPhenotypeSearchError(req phenotypeSearchRequest) error {
	return fmt.Errorf(
		"expect a query with at least one non-stopword, received %q",
		req.query,
	)
}

// newPhenotypeSearchRequest normalizes the query and clamps the limit.
func newPhenotypeSearchRequest(
	params *repo.PhenotypeSearchQuery,
) phenotypeSearchRequest {
	normalized := F.Pipe1(params.Query, repo.NormalizeAutocompleteQuery)
	limit := F.Pipe1(params.Limit, clampFullSearchLimit)

	return F.Pipe2(
		phenotypeSearchRequest{},
		phenotypeSearchQueryLens.Set(normalized),
		phenotypeSearchLimitLens.Set(limit),
	)
}

// validatePhenotypeSearchRequest rejects a nil request, an empty normalized
// query, and an all-stopword query before any AQL statement runs.
func validatePhenotypeSearchRequest(
	params *repo.PhenotypeSearchQuery,
) E.Either[error, phenotypeSearchRequest] {
	return F.Pipe4(
		params,
		checkPhenotypeQueryNotNil,
		E.Map[error](newPhenotypeSearchRequest),
		E.Chain(checkPhenotypeNotEmpty),
		E.Chain(checkPhenotypeNotStopword),
	)
}

// phenotypeSearchTargetBindParams builds the bind parameters for phenotypeSearchTargetsQ.
func (ar *arangorepository) phenotypeSearchTargetBindParams() map[string]any {
	return map[string]any{
		cvCollectionBind: ar.onto.Cv.Name(),
		"term_ontologies": []string{
			phenotypeOntology,
			environmentOntology,
			assayOntology,
		},
	}
}

// readPhenotypeSearchTargetsRow reads a contextResult into phenotypeSearchTargets.
func readPhenotypeSearchTargetsRow(
	row *contextResult,
) IOE.IOEither[error, phenotypeSearchTargets] {
	return F.Pipe1(
		IOE.TryCatchError(func() (phenotypeSearchTargets, error) {
			var targets phenotypeSearchTargets
			err := row.Read(&targets)

			return targets, err
		}),
		IOE.MapLeft[phenotypeSearchTargets](readPhenotypeSearchTargetsFault),
	)
}

// resolvePhenotypeSearchTargets resolves the 3 ontology cv._id values in a
// single AQL round trip. Unloaded ontologies are absent from the returned map
// and default to "" when bound.
func (ar *arangorepository) resolvePhenotypeSearchTargets(
	req phenotypeSearchRequest,
) IOE.IOEither[error, phenotypeSearchTargets] {
	ctx := phenotypeSearchCtxLens.Get(req)

	return F.Pipe2(
		IOE.TryCatchError(func() (*contextResult, error) {
			return ar.getRow(
				ctx,
				phenotypeSearchTargetsQ,
				ar.phenotypeSearchTargetBindParams(),
			)
		}),
		IOE.MapLeft[*contextResult](runPhenotypeSearchTargetsFault),
		IOE.Chain(readPhenotypeSearchTargetsRow),
	)
}

// phenotypeSearchBindParams builds the 7-entry bind map for the 12-branch query.
func (ar *arangorepository) phenotypeSearchBindParams(
	req phenotypeSearchRequest,
) map[string]any {
	targets := phenotypeSearchTargetsLens.Get(req)
	graphs := targets.Graphs

	return map[string]any{
		"q":                  phenotypeSearchQueryLens.Get(req),
		"th":                 fullSearchNgramThreshold,
		"limit":              phenotypeSearchLimitLens.Get(req),
		"anno_cvterm_graph":  ar.anno.annotg.Name(),
		bindPhenotypeGraphID: graphs[phenotypeOntology],
		bindEnvGraphID:       graphs[environmentOntology],
		bindAssayGraphID:     graphs[assayOntology],
	}
}

// buildPhenotypeSearchQueryParams assembles the 12-branch statement and bind map.
func (ar *arangorepository) buildPhenotypeSearchQueryParams(
	req phenotypeSearchRequest,
) phenotypeSearchQueryParams {
	ctx := phenotypeSearchCtxLens.Get(req)
	statement := buildPhenotypeSearchQuery()
	bindParams := ar.phenotypeSearchBindParams(req)

	return F.Pipe3(
		phenotypeSearchQueryParams{},
		phenotypeSearchParamsCtxLens.Set(ctx),
		phenotypeSearchStmtLens.Set(statement),
		phenotypeSearchBindLens.Set(bindParams),
	)
}

// runPhenotypeSearchRows runs the phenotype search statement over SearchRows
// and wraps each fault with runPhenotypeSearchQueryFault.
func (ar *arangorepository) runPhenotypeSearchRows(
	params phenotypeSearchQueryParams,
) IOE.IOEither[error, dbRows] {
	ctx := phenotypeSearchParamsCtxLens.Get(params)
	stmt := phenotypeSearchStmtLens.Get(params)
	binds := phenotypeSearchBindLens.Get(params)

	return F.Pipe1(
		IOE.TryCatchError(func() (dbRows, error) {
			return ar.searchRows(ctx, stmt, binds)
		}),
		IOE.MapLeft[dbRows](runPhenotypeSearchQueryFault),
	)
}

// emptyPhenotypeSearchResults returns a non-nil empty slice when no rows match.
func emptyPhenotypeSearchResults() []*repo.PhenotypeSearchResult {
	return []*repo.PhenotypeSearchResult{}
}

// toPhenotypeSearchResult maps one projection row to a repository PhenotypeSearchResult.
func toPhenotypeSearchResult(row phenotypeSearchRow) *repo.PhenotypeSearchResult {
	return &repo.PhenotypeSearchResult{
		EntryID:    row.ID,
		Field:      row.Field,
		Name:       row.Name,
		Definition: row.Definition,
		Score:      row.Score,
	}
}

// SearchPhenotypes returns at most Limit ranked strain phenotype matches for the
// normalized query of params as a lazy IOEither pipeline.
func (ar *arangorepository) SearchPhenotypes(
	ctx context.Context,
	params *repo.PhenotypeSearchQuery,
) IOE.IOEither[error, []*repo.PhenotypeSearchResult] {
	baseCtx := normalizeQueryContext(ctx)
	withCtx := phenotypeSearchCtxLens.Set(baseCtx)
	rowFault := scanSearchRows[phenotypeSearchRow](
		"error in reading phenotype search row",
	)

	return F.Pipe7(
		IOE.Of[error](params),
		IOE.ChainEitherK(validatePhenotypeSearchRequest),
		IOE.Map[error](withCtx),
		IOE.Bind(phenotypeSearchTargetsLens.Set, ar.resolvePhenotypeSearchTargets),
		IOE.Map[error](ar.buildPhenotypeSearchQueryParams),
		IOE.Chain(ar.runPhenotypeSearchRows),
		IOE.Chain(rowFault),
		IOE.Map[error](toPhenotypeSearchResults),
	)
}
