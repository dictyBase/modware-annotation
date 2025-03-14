package arangodb

import (
	"fmt"
	"strings"

	"github.com/dictyBase/arangomanager/query"
	"github.com/dictyBase/modware-annotation/internal/collection"
)

type filterCallback func(*query.Filter) bool

// PickStatementResult is a struct that holds the result of pickStatement
// function.
type PickStatementResult struct {
	Statement string
	Err       error
}

// getListAnnoStatement returns the appropriate AQL statement based on filter
// string and cursor.
func getListAnnoStatement(fstr string, cursor int64) PickStatementResult {
	var result PickStatementResult
	switch {
	case len(fstr) > 0 && cursor == 0:
		result = makeAQLStatement(fstr)
	case len(fstr) > 0 && cursor != 0:
		result.Statement = fmt.Sprintf(annListFilterWithCursorQ, fstr)
	case len(fstr) == 0 && cursor == 0:
		result.Statement = annListQ
	case len(fstr) == 0 && cursor != 0:
		result.Statement = annListWithCursorQ
	}

	return result
}

func makeAQLStatement(fstr string) PickStatementResult {
	var result PickStatementResult
	pfs, err := query.ParseFilterString(fstr)
	if err != nil {
		result.Err = fmt.Errorf("error in parsing filter string")

		return result
	}

	return collection.Pipe3(
		pfs,
		collection.CurriedFilter(filterFn()),
		collection.CurriedPartitionTuple2(partFn),
		collection.CurriedTFold(pickStatement),
	)
}

func filterFn() filterCallback {
	fmap := FilterMap()

	return func(qfilter *query.Filter) bool {
		_, ok := fmap[qfilter.Field]

		return ok
	}
}

func partFn(qfilter *query.Filter) bool {
	return strings.HasPrefix(qfilter.Field, "ann.")
}

// pickStatement generates an AQL statement based on filter conditions.
func pickStatement[A, B []*query.Filter](
	tup collection.Tuple2[A, B],
) PickStatementResult {
	var result PickStatementResult
	switch {
	case !collection.IsEmpty(tup.First) && !collection.IsEmpty(tup.Second):
		return generateStatementForBothFilters(tup.First, tup.Second)
	case !collection.IsEmpty(tup.First) && collection.IsEmpty(tup.Second):
		return generateStatementForFirstFilter(tup.First)
	case collection.IsEmpty(tup.First) && !collection.IsEmpty(tup.Second):
		return generateStatementForSecondFilter(tup.Second)
	}

	return result
}

// generateStatementForBothFilters creates a statement when both filter sets are
// non-empty.
func generateStatementForBothFilters(
	first, second []*query.Filter,
) PickStatementResult {
	var result PickStatementResult
	fmap := FilterMap()

	afilter, err := query.GenQualifiedAQLFilterStatement(fmap, first)
	if err != nil {
		result.Err = err

		return result
	}

	cfilter, err := query.GenQualifiedAQLFilterStatement(fmap, second)
	if err != nil {
		result.Err = err

		return result
	}
	result.Statement = fmt.Sprintf(annCvtListFilterQ, afilter, cfilter)

	return result
}

// generateStatementForFirstFilter creates a statement when only first filter
// set is non-empty.
func generateStatementForFirstFilter(
	first []*query.Filter,
) PickStatementResult {
	var result PickStatementResult
	fmap := FilterMap()

	afilter, err := query.GenQualifiedAQLFilterStatement(fmap, first)
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(annExclusiveListFilterQ, afilter)

	return result
}

// generateStatementForSecondFilter creates a statement when only second filter
// set is non-empty.
func generateStatementForSecondFilter(
	second []*query.Filter,
) PickStatementResult {
	var result PickStatementResult
	fmap := FilterMap()

	cfilter, err := query.GenQualifiedAQLFilterStatement(fmap, second)
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(cvtExclusiveListFilterQ, cfilter)

	return result
}
