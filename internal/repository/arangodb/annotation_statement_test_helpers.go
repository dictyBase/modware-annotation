package arangodb

import (
	"errors"
	"testing"

	"github.com/dictyBase/arangomanager/query"
	"github.com/stretchr/testify/require"
)

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

func testBuildAQLStatementExistingError(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	originalErr := errors.New("pre-existing error")
	ctx := FilterContext{Err: originalErr}
	result := buildAQLStatement(ctx)
	assert.Equal(originalErr, result.Err, "should return the existing error")
	assert.Empty(
		result.Statement,
		"statement should be empty on existing error",
	)
}

func testBuildAQLStatementInvalidType(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{Type: StatementType("invalid")}
	result := buildAQLStatement(ctx)
	assert.Error(result.Err, "should return error for invalid statement type")
	assert.Contains(
		result.Err.Error(),
		"no matching template found",
		"error message should indicate unsupported type",
	)
	assert.Empty(result.Statement, "statement should be empty")
}

func testBuildAQLStatementBothFiltersNoCursor(
	t *testing.T,
	filterMap map[string]string,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      BothFilters,
		HasCursor: false,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{createFilter("value", "val1")},
		SecondSet: []*query.Filter{createFilter("tag", "tag1")},
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
	assert.Contains(
		result.Statement,
		"FOR cv IN @@cv_collection",
		"should use annCvtListFilterQ base",
	)
}

func testBuildAQLStatementBothFiltersWithCursor(
	t *testing.T,
	filterMap map[string]string,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      BothFilters,
		HasCursor: true,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{createFilter("ann.value", "val1")},
		SecondSet: []*query.Filter{createFilter("cvt.label", "tag1")},
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
	assert.Contains(
		result.Statement,
		"FOR ann IN @anno_collection",
		"should use annCvtListFilterWithCursorQ base",
	)
	assert.Contains(
		result.Statement,
		"FOR cvt IN @@cvterm_collection",
		"should use annCvtListFilterWithCursorQ base",
	)
}

func testBuildAQLStatementFirstFilterNoCursor(
	t *testing.T,
	filterMap map[string]string,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      FirstFilter,
		HasCursor: false,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{createFilter("ann.value", "val1")},
		SecondSet: []*query.Filter{}, // Ensure second set is empty
	}
	result := buildAQLStatement(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.NotEmpty(result.Statement, "statement should not be empty")
	assert.Contains(
		result.Statement,
		"FILTER ann.value",
		"should contain first filter",
	)
	assert.NotContains(
		result.Statement,
		"cvt.label",
		"should not contain second filter",
	)
	assert.NotContains(
		result.Statement,
		"DATE_ISO8601",
		"should not contain cursor logic",
	)
	assert.Contains(
		result.Statement,
		"FOR ann IN @anno_collection",
		"should use annExclusiveListFilterQ base",
	)
	assert.NotContains(
		result.Statement,
		"FOR cvt IN @@cvterm_collection",
		"should not use cvterm parts",
	)
}

func testBuildAQLStatementFirstFilterWithCursor(
	t *testing.T,
	filterMap map[string]string,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      FirstFilter,
		HasCursor: true,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{createFilter("ann.value", "val1")},
		SecondSet: []*query.Filter{},
	}
	result := buildAQLStatement(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.NotEmpty(result.Statement, "statement should not be empty")
	assert.Contains(
		result.Statement,
		"FILTER ann.value",
		"should contain first filter",
	)
	assert.NotContains(
		result.Statement,
		"cvt.label",
		"should not contain second filter",
	)
	assert.Contains(
		result.Statement,
		"DATE_ISO8601(@cursor)",
		"should contain cursor logic",
	)
	assert.Contains(
		result.Statement,
		"FOR ann IN @anno_collection",
		"should use annExclusiveListFilterWithCursorQ base",
	)
	assert.NotContains(
		result.Statement,
		"FOR cvt IN @@cvterm_collection",
		"should not use cvterm parts",
	)
}

func testBuildAQLStatementSecondFilterNoCursor(
	t *testing.T,
	filterMap map[string]string,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      SecondFilter,
		HasCursor: false,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{}, // Ensure first set is empty
		SecondSet: []*query.Filter{createFilter("cvt.label", "tag1")},
	}
	result := buildAQLStatement(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.NotEmpty(result.Statement, "statement should not be empty")
	assert.NotContains(
		result.Statement,
		"ann.value",
		"should not contain first filter",
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
	assert.NotContains(
		result.Statement,
		"FOR ann IN @anno_collection",
		"should not use annotation parts",
	)
	assert.Contains(
		result.Statement,
		"FOR cvt IN @@cvterm_collection",
		"should use cvtExclusiveListFilterQ base",
	)
}

func testBuildAQLStatementSecondFilterWithCursor(
	t *testing.T,
	filterMap map[string]string,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	ctx := FilterContext{
		Type:      SecondFilter,
		HasCursor: true,
		FilterMap: filterMap,
		FirstSet:  []*query.Filter{},
		SecondSet: []*query.Filter{createFilter("cvt.label", "tag1")},
	}
	result := buildAQLStatement(ctx)
	assert.NoError(result.Err, "should not return error")
	assert.NotEmpty(result.Statement, "statement should not be empty")
	assert.NotContains(
		result.Statement,
		"ann.value",
		"should not contain first filter",
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
	assert.NotContains(
		result.Statement,
		"FOR ann IN @anno_collection",
		"should not use annotation parts",
	)
	assert.Contains(
		result.Statement,
		"FOR cvt IN @@cvterm_collection",
		"should use cvtExclusiveListFilterWithCursorQ base",
	)
}

func testBuildAQLStatementErrorGenBoth(
	t *testing.T,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	badFilterMap := map[string]string{"valid": "good"}
	ctx := FilterContext{
		Type:      BothFilters,
		HasCursor: false,
		FilterMap: badFilterMap,
		FirstSet:  []*query.Filter{createFilter("invalid", "val1")},
		SecondSet: []*query.Filter{createFilter("cvt.label", "tag1")},
	}
	result := buildAQLStatement(ctx)
	assert.Error(result.Err, "should return error from filter generation")
	assert.Contains(
		result.Err.Error(),
		"error generating annotation filter",
		"error message should indicate annotation filter error",
	)
	assert.Empty(result.Statement, "statement should be empty on error")
}

func testBuildAQLStatementErrorGenFirst(
	t *testing.T,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	badFilterMap := map[string]string{"valid": "good"}
	ctx := FilterContext{
		Type:      FirstFilter,
		HasCursor: false,
		FilterMap: badFilterMap,
		FirstSet:  []*query.Filter{createFilter("invalid", "val1")},
		SecondSet: []*query.Filter{},
	}
	result := buildAQLStatement(ctx)
	assert.Error(result.Err, "should return error from filter generation")
	assert.Contains(
		result.Err.Error(),
		"error generating annotation filter",
		"error message should indicate annotation filter error",
	)
	assert.Empty(result.Statement, "statement should be empty on error")
}

func testBuildAQLStatementErrorGenSecond(
	t *testing.T,
	createFilter func(string, string) *query.Filter,
) {
	t.Helper()
	t.Parallel()
	assert := require.New(t)
	badFilterMap := map[string]string{"valid": "good"}
	ctx := FilterContext{
		Type:      SecondFilter,
		HasCursor: false,
		FilterMap: badFilterMap,
		FirstSet:  []*query.Filter{},
		SecondSet: []*query.Filter{createFilter("invalid", "tag1")},
	}
	result := buildAQLStatement(ctx)
	assert.Error(result.Err, "should return error from filter generation")
	assert.Contains(
		result.Err.Error(),
		"error generating cvterm filter",
		"error message should indicate cvterm filter error",
	)
	assert.Empty(result.Statement, "statement should be empty on error")
}
