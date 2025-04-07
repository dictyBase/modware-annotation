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

func TestGenFilterStatement(t *testing.T) {
	t.Parallel()
	filterMap := FilterMap() // Use the actual filter map

	t.Run("success - single filter", func(t *testing.T) {
		t.Parallel()
		testGenFilterStatementSuccessSingle(t, filterMap)
	})

	t.Run("success - multiple filters", func(t *testing.T) {
		t.Parallel()
		testGenFilterStatementSuccessMultiple(t, filterMap)
	})

	t.Run("error - invalid filter field", func(t *testing.T) {
		t.Parallel()
		testGenFilterStatementErrorInvalidField(t, filterMap)
	})

	t.Run("edge case - empty filters slice", func(t *testing.T) {
		t.Parallel()
		testGenFilterStatementEdgeEmpty(t, filterMap)
	})
}

func TestBuildAQLStatementErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("with existing error", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		originalErr := errors.New("pre-existing error")
		ctx := FilterContext{Err: originalErr}
		result := buildAQLStatement(ctx)
		assert.Equal(
			originalErr,
			result.Err,
			"should return the existing error",
		)
		assert.Empty(
			result.Statement,
			"statement should be empty on existing error",
		)
	})

	t.Run("with invalid statement type", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{Type: StatementType("invalid")}
		result := buildAQLStatement(ctx)
		assert.Error(
			result.Err,
			"should return error for invalid statement type",
		)
		assert.Contains(
			result.Err.Error(),
			"no matching template found",
			"error message should indicate unsupported type",
		)
		assert.Empty(result.Statement, "statement should be empty")
	})
}

func TestBuildAQLStatementBothFilters(t *testing.T) {
	t.Parallel()
	filterMap := FilterMap()

	t.Run("without cursor", func(t *testing.T) {
		t.Parallel()
		testBothFiltersWithoutCursor(t, filterMap)
	})

	t.Run("with cursor", func(t *testing.T) {
		t.Parallel()
		testBothFiltersWithCursor(t, filterMap)
	})
}

func TestBuildAQLStatementFirstFilter(t *testing.T) {
	t.Parallel()
	filterMap := FilterMap()

	t.Run("without cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      FirstFilter,
			HasCursor: false,
			FilterMap: filterMap,
			FirstSet:  []*query.Filter{createTestFilter("value", "val1")},
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
		// Note: We don't check for absence of cvt.label since it might appear in the template
		// but not as part of a FILTER statement (more as a reference in joins)
	})

	t.Run("with cursor", func(t *testing.T) {
		assert := require.New(t)
		ctx := FilterContext{
			Type:      FirstFilter,
			HasCursor: true,
			FilterMap: filterMap,
			FirstSet:  []*query.Filter{createTestFilter("value", "val1")},
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
		assert.Contains(
			result.Statement,
			"DATE_ISO8601(@cursor)",
			"should contain cursor logic",
		)
	})
}

func TestBuildAQLStatementSecondFilter(t *testing.T) {
	t.Parallel()
	filterMap := FilterMap()

	t.Run("without cursor", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      SecondFilter,
			HasCursor: false,
			FilterMap: filterMap,
			FirstSet:  []*query.Filter{}, // Ensure first set is empty
			SecondSet: []*query.Filter{createTestFilter("tag", "tag1")},
		}
		result := buildAQLStatement(ctx)
		assert.NoError(result.Err, "should not return error")
		assert.NotEmpty(result.Statement, "statement should not be empty")
		assert.Contains(
			result.Statement,
			"FILTER cvt.label",
			"should contain second filter",
		)
	})

	t.Run("with cursor", func(t *testing.T) {
		assert := require.New(t)
		ctx := FilterContext{
			Type:      SecondFilter,
			HasCursor: true,
			FilterMap: filterMap,
			FirstSet:  []*query.Filter{},
			SecondSet: []*query.Filter{createTestFilter("tag", "tag1")},
		}
		result := buildAQLStatement(ctx)
		assert.NoError(result.Err, "should not return error")
		assert.NotEmpty(result.Statement, "statement should not be empty")
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
	})
}

func TestBuildAQLStatementFilterGenerationErrors(t *testing.T) {
	t.Parallel()
	badFilterMap := map[string]string{"valid": "good"}

	t.Run("error generating both filters", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		ctx := FilterContext{
			Type:      BothFilters,
			HasCursor: false,
			FilterMap: badFilterMap,
			FirstSet:  []*query.Filter{createTestFilter("invalid", "val1")},
			SecondSet: []*query.Filter{createTestFilter("cvt.label", "tag1")},
		}
		result := buildAQLStatement(ctx)
		assert.Error(result.Err, "should return error from filter generation")
		assert.Contains(
			result.Err.Error(),
			"error generating annotation filter",
			"error message should indicate annotation filter error",
		)
	})

	t.Run("error generating first filter", func(t *testing.T) {
		assert := require.New(t)
		ctx := FilterContext{
			Type:      FirstFilter,
			HasCursor: false,
			FilterMap: badFilterMap,
			FirstSet:  []*query.Filter{createTestFilter("invalid", "val1")},
			SecondSet: []*query.Filter{},
		}
		result := buildAQLStatement(ctx)
		assert.Error(result.Err, "should return error from filter generation")
		assert.Contains(
			result.Err.Error(),
			"error generating annotation filter",
			"error message should indicate annotation filter error",
		)
	})

	t.Run("error generating second filter", func(t *testing.T) {
		assert := require.New(t)
		ctx := FilterContext{
			Type:      SecondFilter,
			HasCursor: false,
			FilterMap: badFilterMap,
			FirstSet:  []*query.Filter{},
			SecondSet: []*query.Filter{createTestFilter("invalid", "tag1")},
		}
		result := buildAQLStatement(ctx)
		assert.Error(result.Err, "should return error from filter generation")
		assert.Contains(
			result.Err.Error(),
			"error generating cvterm filter",
			"error message should indicate cvterm filter error",
		)
	})
}

func TestGetListAnnoStatement(t *testing.T) {
	t.Parallel()
	t.Run("basic cases", testGetListAnnoStatementBasicCases)
	t.Run("valid filters", testGetListAnnoStatementValidFilters)
	t.Run("tag filters", testGetListAnnoStatementTagFilters)
}
