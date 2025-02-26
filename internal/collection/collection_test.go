package collection

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapSeq(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	// Test case 1: Convert ints to strings
	t.Run("convert ints to strings", func(t *testing.T) {
		t.Parallel()
		input := []int{1, 2, 3, 4, 5}
		resultSeq := MapSeq(slices.Values(input), strconv.Itoa)
		expected := []string{"1", "2", "3", "4", "5"}
		assert.ElementsMatch(expected, slices.Collect(resultSeq))
	})

	// Test case 2: Double and convert to string
	t.Run("double and convert to string", func(t *testing.T) {
		t.Parallel()
		input := []int{10, 20, 30}
		resultSeq := MapSeq(slices.Values(input), func(n int) string {
			return strconv.Itoa(n * 2)
		})
		expected := []string{"20", "40", "60"}
		assert.ElementsMatch(expected, slices.Collect(resultSeq))
	})

	// Test case 3: Empty input
	t.Run("empty input", func(t *testing.T) {
		t.Parallel()
		input := []int{}
		resultSeq := MapSeq(slices.Values(input), strconv.Itoa)
		assert.ElementsMatch(input, slices.Collect(resultSeq))
	})
}
