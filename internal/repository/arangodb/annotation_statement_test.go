package arangodb

import (
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
