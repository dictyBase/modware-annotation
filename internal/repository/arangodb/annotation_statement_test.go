package arangodb

import (
	"errors"
	"testing"

	"github.com/dictyBase/arangomanager/query"
	"github.com/stretchr/testify/require"
)

func TestStatementTemplate(t *testing.T) {
	t.Parallel()
	t.Run("both filters cases", func(t *testing.T) {
		t.Parallel()
		testBothFiltersStatementTemplate(t)
	})
	t.Run("first filter cases", func(t *testing.T) {
		t.Parallel()
		testFirstFilterStatementTemplate(t)
	})
	t.Run("second filter cases", func(t *testing.T) {
		t.Parallel()
		testSecondFilterStatementTemplate(t)
	})
	t.Run("invalid case", func(t *testing.T) {
		t.Parallel()
		testInvalidStatementTemplate(t)
	})
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

func TestFormatKey(t *testing.T) {
	t.Parallel()

	t.Run("both filters with cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		key := formatKey(BothFilters, true)
		assert.Equal("bothtrue", key, "should format key correctly")
	})

	t.Run("first filter without cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		key := formatKey(FirstFilter, false)
		assert.Equal("firstfalse", key, "should format key correctly")
	})

	t.Run("second filter with cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		key := formatKey(SecondFilter, true)
		assert.Equal("secondtrue", key, "should format key correctly")
	})
}

func TestDetermineStatementType(t *testing.T) {
	t.Parallel()

	t.Run("both filters", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		firstSet := []*query.Filter{
			{Field: "ann.value", Value: "test"},
		}
		secondSet := []*query.Filter{
			{Field: "cvt.label", Value: "test"},
		}
		stmtType, ok := determineStatementType(firstSet, secondSet)
		assert.True(ok, "should return true")
		assert.Equal(BothFilters, stmtType, "should return BothFilters type")
	})

	t.Run("first filter only", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		firstSet := []*query.Filter{
			{Field: "ann.value", Value: "test"},
		}
		secondSet := []*query.Filter{}
		stmtType, ok := determineStatementType(firstSet, secondSet)
		assert.True(ok, "should return true")
		assert.Equal(FirstFilter, stmtType, "should return FirstFilter type")
	})

	t.Run("second filter only", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		firstSet := []*query.Filter{}
		secondSet := []*query.Filter{
			{Field: "cvt.label", Value: "test"},
		}
		stmtType, ok := determineStatementType(firstSet, secondSet)
		assert.True(ok, "should return true")
		assert.Equal(SecondFilter, stmtType, "should return SecondFilter type")
	})

	t.Run("no filters", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		firstSet := []*query.Filter{}
		secondSet := []*query.Filter{}
		stmtType, ok := determineStatementType(firstSet, secondSet)
		assert.False(ok, "should return false")
		assert.Empty(stmtType, "should return empty statement type")
	})
}

func TestFilterAndPartitionFunc(t *testing.T) {
	t.Parallel()

	filterMap := map[string]string{
		"entry_id":   "ann.entry_id",
		"value":      "ann.value",
		"created_by": "ann.created_by",
		"tag":        "cvt.label",
		"ontology":   "cv.metadata.namespace",
	}

	t.Run("with valid filters", func(t *testing.T) {
		t.Parallel()
		testValidFilters(t, filterMap)
	})

	t.Run("with only annotation filters", func(t *testing.T) {
		t.Parallel()
		testOnlyAnnotationFilters(t, filterMap)
	})

	t.Run("with only cvterm filters", func(t *testing.T) {
		t.Parallel()
		testOnlyCvtermFilters(t, filterMap)
	})

	t.Run("with invalid filters", func(t *testing.T) {
		t.Parallel()
		testInvalidFilters(t, filterMap)
	})

	t.Run("with mixed valid and invalid filters", func(t *testing.T) {
		t.Parallel()
		testMixedFilters(t, filterMap)
	})

	t.Run("with existing error", func(t *testing.T) {
		t.Parallel()
		testExistingError(t)
	})
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

func TestParseFiltersFunc(t *testing.T) {
	t.Parallel()
	t.Run("success case", testParseFiltersFuncSuccess)
	t.Run(
		"failure case - invalid filter string",
		testParseFiltersFuncFailureInvalidString,
	)
	t.Run(
		"edge case - empty filter string",
		testParseFiltersFuncEdgeEmptyString,
	)
	t.Run("existing error case", testParseFiltersFuncExistingError)
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

// modify this test function based on parseFiltersFunc.
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
