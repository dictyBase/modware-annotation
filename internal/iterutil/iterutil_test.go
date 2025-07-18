package iterutil

import (
	"iter"
	"slices"
	"strconv"
	"testing"

	"github.com/dictyBase/modware-annotation/internal/pipe"
	"github.com/stretchr/testify/require"
)

// Helper predicates for testing.
func isEven(n int) bool {
	return n%2 == 0
}

func isPositive(n int) bool {
	return n > 0
}

func double(n int) int {
	return n * 2
}

func add(a, b int) int {
	return a + b
}

func TestZip(t *testing.T) {
	t.Parallel()

	t.Run("equal length sequences", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2, 3})
		seq2 := slices.Values([]string{"a", "b", "c"})

		result := slices.Collect(Zip(seq1, seq2))

		expected := []Zipped[int, string]{
			{1, true, "a", true},
			{2, true, "b", true},
			{3, true, "c", true},
		}
		require.Equal(t, expected, result)
	})

	t.Run("first sequence longer", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2, 3, 4})
		seq2 := slices.Values([]string{"a", "b"})

		result := slices.Collect(Zip(seq1, seq2))

		expected := []Zipped[int, string]{
			{1, true, "a", true},
			{2, true, "b", true},
			{3, true, "", false},
			{4, true, "", false},
		}
		require.Equal(t, expected, result)
	})

	t.Run("second sequence longer", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2})
		seq2 := slices.Values([]string{"a", "b", "c", "d"})

		result := slices.Collect(Zip(seq1, seq2))

		expected := []Zipped[int, string]{
			{1, true, "a", true},
			{2, true, "b", true},
			{0, false, "c", true},
			{0, false, "d", true},
		}
		require.Equal(t, expected, result)
	})

	t.Run("empty sequences", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{})
		seq2 := slices.Values([]string{})

		result := slices.Collect(Zip(seq1, seq2))

		require.Empty(t, result)
	})
}

func TestEqual(t *testing.T) {
	t.Parallel()

	t.Run("equal sequences", func(t *testing.T) {
		t.Parallel()
		require.True(
			t,
			Equal(
				slices.Values([]int{1, 2, 3}),
				slices.Values([]int{1, 2, 3}),
			),
		)
	})

	t.Run("different values", func(t *testing.T) {
		t.Parallel()
		require.False(
			t,
			Equal(
				slices.Values([]int{1, 3, 0}),
				slices.Values([]int{1, 2, 3}),
			),
		)
	})

	t.Run("different lengths", func(t *testing.T) {
		t.Parallel()
		require.False(
			t,
			Equal(
				slices.Values([]int{1, 2, 3}),
				slices.Values([]int{1, 2}),
			),
		)
	})

	t.Run("empty sequences", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{})
		seq2 := slices.Values([]int{})

		result := Equal(seq1, seq2)

		require.True(t, result)
	})
}

func TestReduce(t *testing.T) {
	t.Parallel()

	t.Run("sum integers", func(t *testing.T) {
		t.Parallel()
		result := Reduce(
			add,
			0,
			slices.Values([]int{1, 2, 3, 4, 5}),
		)
		require.Equal(t, 15, result)
	})

	t.Run("concatenate strings", func(t *testing.T) {
		t.Parallel()
		result := Reduce(
			func(acc, s string) string { return acc + s },
			"",
			slices.Values([]string{"hello", " ", "world"}),
		)
		require.Equal(t, "hello world", result)
	})

	t.Run("empty sequence", func(t *testing.T) {
		t.Parallel()
		require.Equal(
			t,
			10,
			Reduce(add, 10, slices.Values([]int{})),
		)
	})
}

func TestMap(t *testing.T) {
	t.Parallel()

	t.Run("double integers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3, 4})

		result := slices.Collect(Map(double, seq))

		require.Equal(t, []int{2, 4, 6, 8}, result)
	})

	t.Run("int to string", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3})

		result := slices.Collect(Map(strconv.Itoa, seq))

		require.Equal(t, []string{"1", "2", "3"}, result)
	})

	t.Run("empty sequence", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{})

		result := slices.Collect(Map(double, seq))

		require.Empty(t, result)
	})
}

func TestFilter(t *testing.T) {
	t.Parallel()

	t.Run("filter even numbers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3, 4, 5, 6})

		result := slices.Collect(Filter(isEven, seq))

		require.Equal(t, []int{2, 4, 6}, result)
	})

	t.Run("filter positive numbers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{-2, -1, 0, 1, 2})

		result := slices.Collect(Filter(isPositive, seq))

		require.Equal(t, []int{1, 2}, result)
	})

	t.Run("no matches", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 3, 5})

		result := slices.Collect(Filter(isEven, seq))

		require.Empty(t, result)
	})

	t.Run("empty sequence", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{})

		result := slices.Collect(Filter(isEven, seq))

		require.Empty(t, result)
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete even numbers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3, 4, 5, 6})

		result := slices.Collect(Delete(isEven, seq))

		require.Equal(t, []int{1, 3, 5}, result)
	})

	t.Run("delete positive numbers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{-2, -1, 0, 1, 2})

		result := slices.Collect(Delete(isPositive, seq))

		require.Equal(t, []int{-2, -1, 0}, result)
	})

	t.Run("delete all", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{2, 4, 6})

		result := slices.Collect(Delete(isEven, seq))

		require.Empty(t, result)
	})

	t.Run("empty sequence", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{})

		result := slices.Collect(Delete(isEven, seq))

		require.Empty(t, result)
	})
}

func TestAny(t *testing.T) {
	t.Parallel()

	t.Run("has even numbers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3})

		result := Any(isEven, seq)

		require.True(t, result)
	})

	t.Run("no even numbers", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 3, 5})

		result := Any(isEven, seq)

		require.False(t, result)
	})

	t.Run("empty sequence", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{})

		result := Any(isEven, seq)

		require.False(t, result)
	})

	t.Run("early termination", func(t *testing.T) {
		t.Parallel()
		// Create a sequence that would be infinite if not terminated early
		seq := func(yield func(int) bool) {
			for i := 1; i <= 1000; i++ {
				if !yield(i) {
					return
				}
			}
		}

		result := Any(isEven, seq)

		require.True(t, result) // Should find 2 and terminate early
	})
}

func TestConcat(t *testing.T) {
	t.Parallel()

	t.Run("concatenate multiple sequences", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2})
		seq2 := slices.Values([]int{3, 4})
		seq3 := slices.Values([]int{5, 6})

		result := slices.Collect(Concat(seq1, seq2, seq3))

		require.Equal(t, []int{1, 2, 3, 4, 5, 6}, result)
	})

	t.Run("concatenate with empty sequence", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2})
		seq2 := slices.Values([]int{})
		seq3 := slices.Values([]int{3, 4})

		result := slices.Collect(Concat(seq1, seq2, seq3))

		require.Equal(t, []int{1, 2, 3, 4}, result)
	})

	t.Run("no sequences", func(t *testing.T) {
		t.Parallel()
		result := slices.Collect(Concat[int]())

		require.Empty(t, result)
	})

	t.Run("single sequence", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3})

		result := slices.Collect(Concat(seq))

		require.Equal(t, []int{1, 2, 3}, result)
	})
}

func TestContains(t *testing.T) {
	t.Parallel()

	t.Run("element found", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 3, 4, 5})

		result := Contains(3, seq)

		require.True(t, result)
	})

	t.Run("element not found", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{1, 2, 4, 5})

		result := Contains(3, seq)

		require.False(t, result)
	})

	t.Run("empty sequence", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]int{})

		result := Contains(3, seq)

		require.False(t, result)
	})

	t.Run("string elements", func(t *testing.T) {
		t.Parallel()
		seq := slices.Values([]string{"hello", "world", "test"})

		require.True(t, Contains("world", seq))

		seq2 := slices.Values([]string{"hello", "world", "test"})
		require.False(t, Contains("missing", seq2))
	})
}

func TestFind(t *testing.T) {
	t.Parallel()

	t.Run("find is alias for contains", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2, 3, 4, 5})
		seq2 := slices.Values([]int{1, 2, 3, 4, 5})

		containsResult := Contains(3, seq1)
		findResult := Find(3, seq2)

		require.Equal(t, containsResult, findResult)
		require.True(t, findResult)
	})
}

// Test curried functions

func TestMapWith(t *testing.T) {
	t.Parallel()

	t.Run("curried map function", func(t *testing.T) {
		t.Parallel()
		doubleMapper := MapWith(double)
		seq := slices.Values([]int{1, 2, 3})

		result := slices.Collect(doubleMapper(seq))

		require.Equal(t, []int{2, 4, 6}, result)
	})
}

func TestFilterWith(t *testing.T) {
	t.Parallel()

	t.Run("curried filter function", func(t *testing.T) {
		t.Parallel()
		evenFilter := FilterWith(isEven)
		seq := slices.Values([]int{1, 2, 3, 4, 5, 6})

		result := slices.Collect(evenFilter(seq))

		require.Equal(t, []int{2, 4, 6}, result)
	})
}

func TestDeleteWith(t *testing.T) {
	t.Parallel()

	t.Run("curried delete function", func(t *testing.T) {
		t.Parallel()
		evenDeleter := DeleteWith(isEven)
		seq := slices.Values([]int{1, 2, 3, 4, 5, 6})

		result := slices.Collect(evenDeleter(seq))

		require.Equal(t, []int{1, 3, 5}, result)
	})
}

func TestAnyWith(t *testing.T) {
	t.Parallel()

	t.Run("curried any function", func(t *testing.T) {
		t.Parallel()
		hasEven := AnyWith(isEven)
		seq1 := slices.Values([]int{1, 2, 3})
		seq2 := slices.Values([]int{1, 3, 5})

		require.True(t, hasEven(seq1))
		require.False(t, hasEven(seq2))
	})
}

func TestReduceWith(t *testing.T) {
	t.Parallel()

	t.Run("curried reduce function", func(t *testing.T) {
		t.Parallel()
		sumFromZero := ReduceWith(add, 0)
		seq := slices.Values([]int{1, 2, 3, 4, 5})

		result := sumFromZero(seq)

		require.Equal(t, 15, result)
	})

	t.Run("curried reduce with different initial value", func(t *testing.T) {
		t.Parallel()
		sumFromTen := ReduceWith(add, 10)
		seq := slices.Values([]int{1, 2, 3})

		result := sumFromTen(seq)

		require.Equal(t, 16, result)
	})
}

func TestContainsElement(t *testing.T) {
	t.Parallel()

	t.Run("curried contains function", func(t *testing.T) {
		t.Parallel()
		containsThree := ContainsElement(3)
		seq1 := slices.Values([]int{1, 2, 3, 4, 5})
		seq2 := slices.Values([]int{1, 2, 4, 5})

		require.True(t, containsThree(seq1))
		require.False(t, containsThree(seq2))
	})
}

// Integration tests demonstrating composition

func TestFunctionComposition(t *testing.T) {
	t.Parallel()

	t.Run("chain multiple operations", func(t *testing.T) {
		t.Parallel()
		// Filter even numbers, then double them using Pipe2
		result := pipe.Pipe3(
			slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			FilterWith(isEven),
			MapWith(double),
			slices.Collect,
		)

		require.Equal(t, []int{4, 8, 12, 16, 20}, result)
	})

	t.Run("complex pipeline", func(t *testing.T) {
		t.Parallel()
		// Numbers from -5 to 5
		seq := slices.Values([]int{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5})

		// Keep only positive numbers, double them, then sum using Pipe3
		sum := pipe.Pipe3(
			seq,
			FilterWith(isPositive),
			MapWith(double),
			ReduceWith(add, 0),
		)

		require.Equal(t, 30, sum) // (1+2+3+4+5)*2 = 15*2 = 30
	})
}

// Test comprehensive pipe composition patterns.
func TestPipeComposition(t *testing.T) {
	t.Parallel()

	t.Run("Pipe2 with filter and collect", func(t *testing.T) {
		t.Parallel()
		result := pipe.Pipe2(
			slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			FilterWith(isEven),
			slices.Collect,
		)

		require.Equal(t, []int{2, 4, 6, 8, 10}, result)
	})

	t.Run("Pipe3 with filter, map, and reduce", func(t *testing.T) {
		t.Parallel()
		// Filter evens, double them, sum the result
		sum := pipe.Pipe3(
			slices.Values([]int{1, 2, 3, 4, 5}),
			FilterWith(isEven),
			MapWith(double),
			ReduceWith(add, 0),
		)

		require.Equal(t, 12, sum) // (2+4)*2 = 12
	})

	t.Run("Pipe4 with complex transformation", func(t *testing.T) {
		t.Parallel()
		result := pipe.Pipe4(
			slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			FilterWith(isEven),
			MapWith(double),
			FilterWith(func(n int) bool { return n > 10 }),
			slices.Collect,
		)

		require.Equal(t, []int{12, 16, 20}, result)
	})
}

func TestPipeCompositionAdvanced(t *testing.T) {
	t.Parallel()

	t.Run("Pipe2 with concatenation and contains check", func(t *testing.T) {
		t.Parallel()
		seq1 := slices.Values([]int{1, 2, 3})
		seq2 := slices.Values([]int{4, 5, 6})

		hasTarget := pipe.Pipe2(
			[]iter.Seq[int]{seq1, seq2},
			func(seqs []iter.Seq[int]) iter.Seq[int] { return Concat(seqs...) },
			ContainsElement(5),
		)

		require.True(t, hasTarget)
	})

	t.Run("Pipe3 with delete, map, and any", func(t *testing.T) {
		t.Parallel()
		numbers := slices.Values([]int{-3, -2, -1, 0, 1, 2, 3})

		// Delete negative numbers, double positives, check if any are > 4
		hasLarge := pipe.Pipe3(
			numbers,
			DeleteWith(func(n int) bool { return n < 0 }),
			MapWith(double),
			AnyWith(func(n int) bool { return n > 4 }),
		)

		require.True(t, hasLarge) // 2*3=6 > 4
	})

	t.Run("Pipe4 with string transformation pipeline", func(t *testing.T) {
		t.Parallel()
		numbers := slices.Values([]int{1, 2, 3, 4, 5})

		// Filter evens, convert to string, add prefix, collect
		result := pipe.Pipe4(
			numbers,
			FilterWith(isEven),
			MapWith(strconv.Itoa),
			MapWith(func(s string) string { return "num_" + s }),
			slices.Collect[string],
		)

		require.Equal(t, []string{"num_2", "num_4"}, result)
	})
}
