package arangodb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dictyBase/arangomanager/query"
	"github.com/stretchr/testify/require"
)

func createTestFilter(field, value string) *query.Filter {
	return &query.Filter{
		Field:    field,
		Value:    value,
		Operator: "==",
	}
}

func testBothFiltersWithCursor(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      BothFilters,
		HasCursor: true,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{createTestFilter("value", "val1")},
		SecondSet: []*query.Filter{createTestFilter("tag", "tag1")},
	}
	result := buildAQLStatement(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.NotEmpty(result.Statement, "statement should not be empty")
	assert.Contains(
		result.Statement,
		"FILTER ann.value",
		"should contain first filter",
	)
	assert.Contains(
		result.Statement,
		"FILTER cvt.label",
		"should contain second filter",
	)
	assert.Contains(
		result.Statement,
		"DATE_ISO8601(@cursor)",
		"should contain cursor logic",
	)
}

func testBothFiltersWithoutCursor(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      BothFilters,
		HasCursor: false,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{createTestFilter("value", "val1")},
		SecondSet: []*query.Filter{createTestFilter("tag", "tag1")},
	}
	result := buildAQLStatement(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.NotEmpty(result.Statement, "statement should not be empty")
	assert.Contains(
		result.Statement,
		"FILTER ann.value",
		"should contain first filter",
	)
	assert.Contains(
		result.Statement,
		"FILTER cvt.label",
		"should contain second filter",
	)
	assert.NotContains(
		result.Statement,
		"DATE_ISO8601",
		"should not contain cursor logic",
	)
	assert.Contains(
		result.Statement,
		"FOR ann IN @anno_collection",
		"should use annCvtListFilterQ base",
	)
}

func testBothFiltersStatementTemplate(t *testing.T) {
	t.Helper()
	t.Run("with cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      BothFilters,
			HasCursor: true,
		}
		template, ok := statementTemplate(ctx)
		assert.True(ok, "should find template")
		assert.Equal(
			annCvtListFilterWithCursorQ,
			template,
			"should return correct template",
		)
	})

	t.Run("without cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      BothFilters,
			HasCursor: false,
		}
		template, ok := statementTemplate(ctx)
		assert.True(ok, "should find template")
		assert.Equal(
			annCvtListFilterQ,
			template,
			"should return correct template",
		)
	})
}

func testFirstFilterStatementTemplate(t *testing.T) {
	t.Helper()
	t.Run("with cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      FirstFilter,
			HasCursor: true,
		}
		template, ok := statementTemplate(ctx)
		assert.True(ok, "should find template")
		assert.Equal(
			annExclusiveListFilterWithCursorQ,
			template,
			"should return correct template",
		)
	})

	t.Run("without cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      FirstFilter,
			HasCursor: false,
		}
		template, ok := statementTemplate(ctx)
		assert.True(ok, "should find template")
		assert.Equal(
			annExclusiveListFilterQ,
			template,
			"should return correct template",
		)
	})
}

func testSecondFilterStatementTemplate(t *testing.T) {
	t.Helper()
	t.Run("with cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      SecondFilter,
			HasCursor: true,
		}
		template, ok := statementTemplate(ctx)
		assert.True(ok, "should find template")
		assert.Equal(
			cvtExclusiveListFilterWithCursorQ,
			template,
			"should return correct template",
		)
	})

	t.Run("without cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      SecondFilter,
			HasCursor: false,
		}
		template, ok := statementTemplate(ctx)
		assert.True(ok, "should find template")
		assert.Equal(
			cvtExclusiveListFilterQ,
			template,
			"should return correct template",
		)
	})
}

func testInvalidStatementTemplate(t *testing.T) {
	t.Helper()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      StatementType("invalid"),
		HasCursor: false,
	}
	template, ok := statementTemplate(ctx)
	assert.False(ok, "should not find template")
	assert.Empty(template, "should return empty template")
}

func testValidFilters(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)
	ctx := FilterContext{
		FilterMap: filterMap,
		Filters: []*query.Filter{
			{Field: "entry_id", Value: "DBS123"},
			{Field: "tag", Value: "gene"},
		},
	}

	result := filterAndPartitionFunc(ctx)
	assert.Nil(result.Err, "should not have error")
	assert.Len(result.Filters, 2, "should have 2 valid filters")
	assert.Len(result.FirstSet, 1, "should have 1 filter in first set")
	assert.Len(result.SecondSet, 1, "should have 1 filter in second set")
	assert.Equal(
		"ann.entry_id",
		result.FirstSet[0].Field,
		"first set should contain annotation filter",
	)
	assert.Equal(
		"cvt.label",
		result.SecondSet[0].Field,
		"second set should contain cvterm filter",
	)
}

func testOnlyAnnotationFilters(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)

	ctx := FilterContext{
		FilterMap: filterMap,
		Filters: []*query.Filter{
			{Field: "entry_id", Value: "DBS123"},
			{Field: "value", Value: "test"},
		},
	}

	result := filterAndPartitionFunc(ctx)

	assert.Nil(result.Err, "should not have error")
	assert.Len(result.Filters, 2, "should have 2 valid filters")
	assert.Len(result.FirstSet, 2, "should have 2 filters in first set")
	assert.Len(result.SecondSet, 0, "should have 0 filters in second set")
}

func testOnlyCvtermFilters(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)

	ctx := FilterContext{
		FilterMap: filterMap,
		Filters: []*query.Filter{
			{Field: "tag", Value: "gene"},
			{Field: "ontology", Value: "GO"},
		},
	}

	result := filterAndPartitionFunc(ctx)

	assert.Nil(result.Err, "should not have error")
	assert.Len(result.Filters, 2, "should have 2 valid filters")
	assert.Len(result.FirstSet, 0, "should have 0 filters in first set")
	assert.Len(result.SecondSet, 2, "should have 2 filters in second set")
}

func testInvalidFilters(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)

	ctx := FilterContext{
		FilterMap: filterMap,
		Filters: []*query.Filter{
			{Field: "invalid_field", Value: "test"},
		},
	}

	result := filterAndPartitionFunc(ctx)

	assert.NotNil(result.Err, "should have error")
	assert.Contains(
		result.Err.Error(),
		"no valid filters found",
		"should have appropriate error message",
	)
}

func testMixedFilters(t *testing.T, filterMap map[string]string) {
	t.Helper()
	assert := require.New(t)

	ctx := FilterContext{
		FilterMap: filterMap,
		Filters: []*query.Filter{
			{Field: "entry_id", Value: "DBS123"},
			{Field: "invalid_field", Value: "test"},
		},
	}

	result := filterAndPartitionFunc(ctx)

	assert.Nil(result.Err, "should not have error")
	assert.Len(result.Filters, 1, "should have 1 valid filter")
	assert.Len(result.FirstSet, 1, "should have 1 filter in first set")
	assert.Len(result.SecondSet, 0, "should have 0 filters in second set")
}

func testExistingError(t *testing.T) {
	t.Helper()
	assert := require.New(t)

	existingErr := errors.New("existing error")
	ctx := FilterContext{
		Err: existingErr,
	}

	result := filterAndPartitionFunc(ctx)

	assert.Equal(existingErr, result.Err, "should preserve existing error")
}

func testParseFiltersFuncSuccess(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		FilterString: "tag==gene;value!=test",
	}
	result := parseFiltersFunc(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.Len(result.Filters, 2, "should parse two filters")
	assert.Equal(
		"tag",
		result.Filters[0].Field,
		"first filter field should be tag",
	)
	assert.Equal(
		"gene",
		result.Filters[0].Value,
		"first filter value should be gene",
	)
	assert.Equal(
		"value",
		result.Filters[1].Field,
		"second filter field should be value",
	)
	assert.Equal(
		"test",
		result.Filters[1].Value,
		"second filter value should be test",
	)
}

func testParseFiltersFuncFailureInvalidString(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		FilterString: "tag=gene;", // Invalid format
	}
	result := parseFiltersFunc(ctx)
	assert.NoError(
		result.Err,
		"should not return error for this specific invalid format",
	)
	assert.Empty(
		result.Filters,
		"filters should be empty for this specific invalid format",
	)
}

func testParseFiltersFuncEdgeEmptyString(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		FilterString: "",
	}
	result := parseFiltersFunc(ctx)
	// Assuming query.ParseFilterString returns empty slice and no error for empty string
	assert.NoError(result.Err, "should not return error for empty string")
	assert.Empty(result.Filters, "should return empty slice for empty string")
}

func testParseFiltersFuncExistingError(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	originalErr := errors.New("previous error")
	ctx := FilterContext{
		FilterString: "tag==gene", // This string doesn't matter as the function should return early
		Err:          originalErr,
	}
	result := parseFiltersFunc(ctx)
	// The function should return immediately if ctx.Err is already set.
	assert.Equal(originalErr, result.Err, "should preserve the original error")
	assert.Nil(
		result.Filters,
		"filters should not be parsed when an error already exists",
	)
}

func testGenFilterStatementSuccessSingle(
	t *testing.T,
	filterMap map[string]string,
) {
	t.Helper()
	assert := require.New(t)
	filters := []*query.Filter{
		{Field: "tag", Operator: "==", Value: "gene"},
	}
	expected := "FILTER cvt.label == 'gene'"
	filterType := "cvterm"

	stmt, err := genFilterStatement(filterMap, filters, filterType)
	assert.NoError(err, "should not return error for valid filter")
	assert.Equal(
		expected,
		stmt,
		"should generate correct AQL filter statement",
	)
}

func testGenFilterStatementSuccessMultiple(
	t *testing.T,
	filterMap map[string]string,
) {
	t.Helper()
	assert := require.New(t)
	filters := []*query.Filter{
		{
			Field:    "entry_id",
			Operator: "==", Value: "DBS01234", Logic: ";",
		},
		{Field: "value", Operator: "!=", Value: "test"},
	}
	// Note: The order might vary depending on map iteration,
	// but both filters should be present.
	expectedPart1 := "FILTER ann.entry_id == 'DBS01234'"
	expectedPart2 := "ann.value != 'test'"
	filterType := "annotation"

	stmt, err := genFilterStatement(filterMap, filters, filterType)
	assert.NoError(
		err,
		"should not return error for multiple valid filters",
	)
	assert.Contains(stmt, expectedPart1, "should contain entry_id filter")
	assert.Contains(stmt, expectedPart2, "should contain value filter")
	assert.Contains(stmt, "AND", "should contain AND operator")
}

func testGenFilterStatementErrorInvalidField(
	t *testing.T,
	filterMap map[string]string,
) {
	t.Helper()
	assert := require.New(t)
	filters := []*query.Filter{
		{Field: "invalid_field", Operator: "==", Value: "some_value"},
	}
	filterType := "annotation"

	_, err := genFilterStatement(filterMap, filters, filterType)
	assert.Error(err, "should return error for invalid filter field")
	assert.Contains(
		err.Error(),
		fmt.Sprintf("error generating %s filter", filterType),
		"error message should include filter type",
	)
}

func testGenFilterStatementEdgeEmpty(
	t *testing.T,
	filterMap map[string]string,
) {
	t.Helper()
	assert := require.New(t)
	filters := []*query.Filter{}
	filterType := "cvterm"

	// query.GenQualifiedAQLFilterStatement returns empty string for empty filters
	_, err := genFilterStatement(filterMap, filters, filterType)
	assert.Error(err, "should return error for empty filter slice")
}
