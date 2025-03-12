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

	// Test case 4: Transform to different type with conditional logic
	t.Run("conditional transformation", func(t *testing.T) {
		t.Parallel()
		input := []int{1, 2, 3, 4, 5}
		resultSeq := MapSeq(slices.Values(input), func(n int) string {
			if n%2 == 0 {
				return "even"
			}

			return "odd"
		})
		expected := []string{"odd", "even", "odd", "even", "odd"}
		assert.ElementsMatch(expected, slices.Collect(resultSeq))
	})
}

func TestPartition(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	// Test case 1: Partition integers by even/odd
	t.Run("partition integers by even/odd", func(t *testing.T) {
		t.Parallel()
		input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		isEven := func(n int) bool { return n%2 == 0 }
		evens, odds := Partition(input, isEven)
		assert.ElementsMatch([]int{2, 4, 6, 8, 10}, evens)
		assert.ElementsMatch([]int{1, 3, 5, 7, 9}, odds)
	})

	// Test case 2: Partition strings by length
	t.Run("partition strings by length", func(t *testing.T) {
		t.Parallel()
		input := []string{"a", "bb", "ccc", "dddd", "eeeee"}
		isShort := func(s string) bool { return len(s) <= 2 }

		short, long := Partition(input, isShort)

		assert.ElementsMatch([]string{"a", "bb"}, short)
		assert.ElementsMatch([]string{"ccc", "dddd", "eeeee"}, long)
	})

	// Test case 3: Empty input
	t.Run("empty input", func(t *testing.T) {
		t.Parallel()
		input := []int{}
		isEven := func(n int) bool { return n%2 == 0 }

		evens, odds := Partition(input, isEven)

		assert.Empty(evens)
		assert.Empty(odds)
	})

	// Test case 4: All elements satisfy predicate
	t.Run("all elements satisfy predicate", func(t *testing.T) {
		t.Parallel()
		input := []int{2, 4, 6, 8, 10}
		isEven := func(n int) bool { return n%2 == 0 }

		evens, odds := Partition(input, isEven)

		assert.ElementsMatch(input, evens)
		assert.Empty(odds)
	})

	// Test case 5: No elements satisfy predicate
	t.Run("no elements satisfy predicate", func(t *testing.T) {
		t.Parallel()
		input := []int{1, 3, 5, 7, 9}
		isEven := func(n int) bool { return n%2 == 0 }

		evens, odds := Partition(input, isEven)

		assert.Empty(evens)
		assert.ElementsMatch(input, odds)
	})
}
