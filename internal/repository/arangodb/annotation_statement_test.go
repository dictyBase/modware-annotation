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
