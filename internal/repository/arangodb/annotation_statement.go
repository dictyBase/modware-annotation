package arangodb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dictyBase/arangomanager/query"
	"github.com/dictyBase/modware-annotation/internal/collection"
)

const (
	// BothFilters indicates both annotation and cvterm filters are present.
	BothFilters StatementType = "both"
	// FirstFilter indicates only annotation filters are present.
	FirstFilter StatementType = "first"
	// SecondFilter indicates only cvterm filters are present.
	SecondFilter StatementType = "second"
)

// StatementType represents the type of AQL statement to be generated.
type StatementType string

// PickStatementResult is a struct that holds the result of pickStatement
// function.
type PickStatementResult struct {
	Statement string
	Err       error
}

// FilterContext holds the context for filter processing.
type FilterContext struct {
	FilterString string
	HasCursor    bool
	Filters      []*query.Filter
	FirstSet     []*query.Filter
	SecondSet    []*query.Filter
	FilterMap    map[string]string
	Type         StatementType
	Err          error
}

// templateMap maps statement types and cursor flags to appropriate templates.
var templateMap = map[string]string{
	formatKey(BothFilters, true):   annCvtListFilterWithCursorQ,
	formatKey(BothFilters, false):  annCvtListFilterQ,
	formatKey(FirstFilter, true):   annExclusiveListFilterWithCursorQ,
	formatKey(FirstFilter, false):  annExclusiveListFilterQ,
	formatKey(SecondFilter, true):  cvtExclusiveListFilterWithCursorQ,
	formatKey(SecondFilter, false): cvtExclusiveListFilterQ,
}

// formatKey creates a template map key from statement type and cursor flag.
func formatKey(statementType StatementType, hasCursor bool) string {
	return fmt.Sprintf("%s%v", string(statementType), hasCursor)
}

// statementTemplate maps configuration to the appropriate AQL template.
func statementTemplate(ctx FilterContext) (string, bool) {
	val, ok := templateMap[formatKey(ctx.Type, ctx.HasCursor)]

	return val, ok
}

// buildAQLStatement is the core function that builds AQL statements based on
// configuration.
func buildAQLStatement(ctx FilterContext) PickStatementResult {
	if ctx.Err != nil {
		return PickStatementResult{Err: ctx.Err}
	}

	var result PickStatementResult
	template, ok := statementTemplate(ctx)
	if !ok {
		result.Err = fmt.Errorf(
			"no matching template found for statement type %s with cursor=%v",
			ctx.Type,
			ctx.HasCursor,
		)

		return result
	}

	switch ctx.Type {
	case BothFilters:
		result = buildBothFiltersStatement(
			template,
			ctx.FilterMap,
			ctx.FirstSet,
			ctx.SecondSet,
		)
	case FirstFilter:
		result = buildFirstFilterStatement(
			template,
			ctx.FilterMap,
			ctx.FirstSet,
		)
	case SecondFilter:
		result = buildSecondFilterStatement(
			template,
			ctx.FilterMap,
			ctx.SecondSet,
		)
	default:
		result.Err = errors.New("unsupported statement type")
	}

	return result
}

// genFilterStatement is a helper that generates a qualified AQL filter statement
// and handles error with proper context.
func genFilterStatement(
	filterMap map[string]string,
	filters []*query.Filter,
	filterType string,
) (string, error) {
	filter, err := query.GenQualifiedAQLFilterStatement(filterMap, filters)
	if err != nil {
		return "", fmt.Errorf("error generating %s filter: %w", filterType, err)
	}

	return filter, nil
}

// buildBothFiltersStatement handles creating a statement when both filter sets
// are non-empty.
func buildBothFiltersStatement(
	template string,
	filterMap map[string]string,
	firstSet, secondSet []*query.Filter,
) PickStatementResult {
	var result PickStatementResult

	afilter, err := genFilterStatement(filterMap, firstSet, "annotation")
	if err != nil {
		result.Err = err

		return result
	}

	cfilter, err := genFilterStatement(filterMap, secondSet, "cvterm")
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(template, afilter, cfilter)

	return result
}

// buildFirstFilterStatement handles creating a statement when only first filter
// set is non-empty.
func buildFirstFilterStatement(
	template string,
	filterMap map[string]string,
	filters []*query.Filter,
) PickStatementResult {
	var result PickStatementResult

	afilter, err := genFilterStatement(filterMap, filters, "annotation")
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(template, afilter)

	return result
}

// buildSecondFilterStatement handles creating a statement when only second
// filter set is non-empty.
func buildSecondFilterStatement(
	template string,
	filterMap map[string]string,
	filters []*query.Filter,
) PickStatementResult {
	var result PickStatementResult

	cfilter, err := genFilterStatement(filterMap, filters, "cvterm")
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(template, cfilter)

	return result
}

// getListAnnoStatement returns the appropriate AQL statement based on filter
// string and cursor.
func getListAnnoStatement(fstr string, cursor int64) PickStatementResult {
	if len(fstr) == 0 {
		return PickStatementResult{
			Err: errors.New("empty filter string"),
		}
	}
	// Create a pipeline to process filters and generate statements
	return collection.Pipe4(
		FilterContext{
			FilterString: fstr,
			HasCursor:    cursor != 0,
			FilterMap:    FilterMap(),
		},
		parseFiltersFunc,
		filterAndPartitionFunc,
		determineStatementTypeFunc,
		buildAQLStatement,
	)
}

// parseFiltersFunc returns a function for parsing filter strings in a pipeline.
func parseFiltersFunc(ctx FilterContext) FilterContext {
	if ctx.Err != nil { // Pass through existing errors
		return ctx
	}
	filters, err := query.ParseFilterString(ctx.FilterString)
	if err != nil {
		ctx.Err = fmt.Errorf(
			"error parsing filter string %q: %w",
			ctx.FilterString,
			err,
		)

		return ctx
	}
	ctx.Filters = filters

	return ctx
}

// filterAndPartitionFunc returns a function for filtering and partitioning in a pipeline.
func filterAndPartitionFunc(ctx FilterContext) FilterContext {
	if ctx.Err != nil {
		return ctx
	}
	var validFilters []*query.Filter
	var firstSet []*query.Filter
	var secondSet []*query.Filter

	for _, qfl := range ctx.Filters {
		if mappedField, ok := ctx.FilterMap[qfl.Field]; ok {
			// Create a new filter with the mapped field
			mappedFilter := &query.Filter{
				Field:    qfl.Field,
				Value:    qfl.Value,
				Operator: qfl.Operator,
			}
			if len(qfl.Logic) != 0 {
				mappedFilter.Logic = qfl.Logic
			}
			validFilters = append(validFilters, mappedFilter)
			// Check if the mapped field is for annotation or cvterm
			if strings.HasPrefix(mappedField, "ann.") {
				firstSet = append(firstSet, mappedFilter)
			} else {
				secondSet = append(secondSet, mappedFilter)
			}
		}
	}

	if collection.IsEmpty(validFilters) {
		ctx.Err = fmt.Errorf(
			"no valid filters found in filter string %q",
			ctx.FilterString,
		)

		return ctx
	}

	ctx.Filters = validFilters
	ctx.FirstSet = firstSet
	ctx.SecondSet = secondSet

	return ctx
}

// determineStatementTypeFunc returns a function for determining statement type in a pipeline.
func determineStatementTypeFunc(ctx FilterContext) FilterContext {
	if ctx.Err != nil {
		return ctx
	}
	statementType, ok := determineStatementType(ctx.FirstSet, ctx.SecondSet)
	if !ok {
		ctx.Err = errors.New(
			"no valid filters found after parsing",
		)

		return ctx
	}
	ctx.Type = statementType
	ctx.Filters = append(
		ctx.Filters,
		&query.Filter{Field: (string(statementType))},
	)

	return ctx
}

// determineStatementType determines the statement type based on filter
// presence.
func determineStatementType(
	first, second []*query.Filter,
) (StatementType, bool) {
	switch {
	case !collection.IsEmpty(first) && !collection.IsEmpty(second):
		return BothFilters, true
	case !collection.IsEmpty(first) && collection.IsEmpty(second):
		return FirstFilter, true
	case collection.IsEmpty(first) && !collection.IsEmpty(second):
		return SecondFilter, true
	default:
		return "", false
	}
}
