package arangodb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dictyBase/arangomanager/query"
	"github.com/dictyBase/modware-annotation/internal/collection"
)

// PickStatementResult is a struct that holds the result of pickStatement
// function.
type PickStatementResult struct {
	Statement string
	Err       error
}

// StatementType represents the type of AQL statement to be generated.
type StatementType string

const (
	// BothFilters indicates both annotation and cvterm filters are present.
	BothFilters StatementType = "both"
	// FirstFilter indicates only annotation filters are present.
	FirstFilter StatementType = "first"
	// SecondFilter indicates only cvterm filters are present.
	SecondFilter StatementType = "second"
)

// StatementConfig configures the AQL statement generation.
type StatementConfig struct {
	Type      StatementType
	HasCursor bool
	FilterMap map[string]string
	FirstSet  []*query.Filter
	SecondSet []*query.Filter
}

// formatKey creates a template map key from statement type and cursor flag.
func formatKey(statementType StatementType, hasCursor bool) string {
	return fmt.Sprintf("%s%v", string(statementType), hasCursor)
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

// statementTemplate maps configuration to the appropriate AQL template.
func statementTemplate(cfg *StatementConfig) string {
	key := formatKey(cfg.Type, cfg.HasCursor)

	return templateMap[key]
}

// buildAQLStatement is the core function that builds AQL statements based on
// configuration.
func buildAQLStatement(cfg *StatementConfig) PickStatementResult {
	var result PickStatementResult

	template := statementTemplate(cfg)
	if template == "" {
		result.Err = fmt.Errorf(
			"no matching template found for statement type %s with cursor=%v",
			cfg.Type,
			cfg.HasCursor,
		)

		return result
	}

	switch cfg.Type {
	case BothFilters:
		return buildBothFiltersStatement(
			template,
			cfg.FilterMap,
			cfg.FirstSet,
			cfg.SecondSet,
		)
	case FirstFilter:
		return buildFirstFilterStatement(template, cfg.FilterMap, cfg.FirstSet)
	case SecondFilter:
		return buildSecondFilterStatement(
			template,
			cfg.FilterMap,
			cfg.SecondSet,
		)
	default:
		result.Err = errors.New("unsupported statement type")

		return result
	}
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

// determineStatementType determines the statement type based on filter
// presence.
func determineStatementType(first, second []*query.Filter) StatementType {
	switch {
	case !collection.IsEmpty(first) && !collection.IsEmpty(second):
		return BothFilters
	case !collection.IsEmpty(first) && collection.IsEmpty(second):
		return FirstFilter
	case collection.IsEmpty(first) && !collection.IsEmpty(second):
		return SecondFilter
	default:
		return ""
	}
}

// generateStatement is a unified function for generating AQL statements
// that handles both cursor and non-cursor cases.
func generateStatement(
	first, second []*query.Filter,
	hasCursor bool,
) PickStatementResult {
	statementType := determineStatementType(first, second)
	if statementType == "" {
		return PickStatementResult{
			Err: errors.New("no valid filters found after parsing"),
		}
	}

	return buildAQLStatement(&StatementConfig{
		Type:      statementType,
		HasCursor: hasCursor,
		FilterMap: FilterMap(),
		FirstSet:  first,
		SecondSet: second,
	})
}

// getListAnnoStatement returns the appropriate AQL statement based on filter
// string and cursor.
func getListAnnoStatement(fstr string, cursor int64) PickStatementResult {
	if len(fstr) == 0 {
		return PickStatementResult{
			Err: errors.New("empty filter string"),
		}
	}
	hasCursor := cursor != 0

	return processFilters(fstr, hasCursor)
}

// processFilters parses the filter string and processes it to generate an AQL
// statement.
func processFilters(fstr string, hasCursor bool) PickStatementResult {
	pfs, err := query.ParseFilterString(fstr)
	if err != nil {
		return PickStatementResult{
			Err: fmt.Errorf("error parsing filter string %q: %w", fstr, err),
		}
	}

	// Create a single shared FilterMap to reduce allocations
	fmap := FilterMap()
	// Filter and partition in a single step
	var validFilters []*query.Filter
	var firstSet []*query.Filter
	var secondSet []*query.Filter
	for _, qfl := range pfs {
		if _, ok := fmap[qfl.Field]; ok {
			validFilters = append(validFilters, qfl)
			if strings.HasPrefix(qfl.Field, "ann.") {
				firstSet = append(firstSet, qfl)
			} else {
				secondSet = append(secondSet, qfl)
			}
		}
	}

	if collection.IsEmpty(validFilters) {
		return PickStatementResult{
			Err: fmt.Errorf("no valid filters found in filter string %q", fstr),
		}
	}

	return generateStatement(firstSet, secondSet, hasCursor)
}
