package collection

import (
	"cmp"
	"iter"
	"slices"
)

// Map returns the slice obtained after applying the given function over every
// element in the given slice.
func Map[T1, T2 any](slc []T1, fnc func(T1) T2) []T2 {
	ret := make([]T2, 0)
	for _, elem := range slc {
		ret = append(ret, fnc(elem))
	}

	return ret
}

// CurriedMap returns a function that, when given a slice, applies the provided
// function to each element of the slice. This is a curried version of the Map
// function.
func CurriedMap[T1, T2 any](fnc func(T1) T2) func([]T1) []T2 {
	return func(slc []T1) []T2 {
		return Map(slc, fnc)
	}
}

// Include determines whether the given element is present in the slice.
// The slice is sorted before searching.
func Include[T cmp.Ordered](slice []T, element T) bool {
	if !slices.IsSorted(slice) {
		slices.Sort(slice)
	}
	_, found := slices.BinarySearch(slice, element)

	return found
}

// RemoveStringItems removes elements from a that are present in
// items.
func RemoveStringItems(slice []string, items ...string) []string {
	str := make([]string, 0)
	for _, val := range slice {
		if !Include(items, val) {
			str = append(str, val)
		}
	}

	return str
}

// Filter returns a new slice containing all elements that satisfy the
// predicate.
func Filter[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0)
	for _, item := range slice {
		if predicate(item) {
			result = append(result, item)
		}
	}

	return result
}

// CurriedFilter returns a function that, when given a slice, filters it based on
// the provided predicate. This is a curried version of the Filter function.
func CurriedFilter[T any](predicate func(T) bool) func([]T) []T {
	return func(slice []T) []T {
		return Filter(slice, predicate)
	}
}

// MapSeq transforms an iter.Seq to another iter.Seq by applying the given
// function to each element in the sequence.
func MapSeq[T1, T2 any](seq iter.Seq[T1], fn func(T1) T2) iter.Seq[T2] {
	return func(yield func(T2) bool) {
		for v := range seq {
			if !yield(fn(v)) {
				return
			}
		}
	}
}

// PartitionTuple2 splits a slice into two parts based on a predicate function and
// returns them as a Tuple2. The First field contains all elements for which the
// predicate returns true, and the Second field contains all elements for which
// the predicate returns false.
func PartitionTuple2[T any](
	slice []T,
	predicate func(T) bool,
) Tuple2[[]T, []T] {
	trueSlice := make([]T, 0)
	falseSlice := make([]T, 0)
	for _, item := range slice {
		if predicate(item) {
			trueSlice = append(trueSlice, item)
		} else {
			falseSlice = append(falseSlice, item)
		}
	}

	return NewTuple2(trueSlice, falseSlice)
}

// CurriedPartitionTuple2 returns a function that, when given a slice, partitions it
// based on the provided predicate and returns the result as a Tuple2. This is a
// curried version of the PartitionTuple2 function.
func CurriedPartitionTuple2[T any](
	predicate func(T) bool,
) func([]T) Tuple2[[]T, []T] {
	return func(slice []T) Tuple2[[]T, []T] {
		return PartitionTuple2(slice, predicate)
	}
}

// Partition splits a slice into two slices based on a predicate function. The
// first returned slice contains all elements for which the predicate returns
// true, and the second contains all elements for which the predicate returns
// false.
func Partition[T any](slice []T, predicate func(T) bool) ([]T, []T) {
	trueSlice := make([]T, 0)
	falseSlice := make([]T, 0)
	for _, item := range slice {
		if predicate(item) {
			trueSlice = append(trueSlice, item)
		} else {
			falseSlice = append(falseSlice, item)
		}
	}

	return trueSlice, falseSlice
}

// CurriedPartition returns a function that, when given a slice, partitions it
// based on the provided predicate. This is a curried version of the Partition
// function.
func CurriedPartition[T any](predicate func(T) bool) func([]T) ([]T, []T) {
	return func(slice []T) ([]T, []T) {
		return Partition(slice, predicate)
	}
}

// Pipe2 creates a functional pipeline by taking an initial value and applying
// two functions in succession. The output of the first function becomes the
// input to the second function. The final return value is the result of the
// last function application.
func Pipe2[T1, T2, T3 any](tup T1, f1 func(T1) T2, f2 func(T2) T3) T3 {
	return f2(f1(tup))
}

// Pipe3 creates a functional pipeline by taking an initial value and applying
// three functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe3[T1, T2, T3, T4 any](
	initial T1,
	f1 func(T1) T2,
	f2 func(T2) T3,
	f3 func(T3) T4,
) T4 {
	return f3(f2(f1(initial)))
}

// Pipe4 creates a functional pipeline by taking an initial value and applying
// four functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe4[T1, T2, T3, T4, T5 any](
	initial T1,
	f1 func(T1) T2,
	f2 func(T2) T3,
	f3 func(T3) T4,
	f4 func(T4) T5,
) T5 {
	return f4(f3(f2(f1(initial))))
}

// Tuple2 represents a pair of values with independent types.
// It's useful for functions that need to return two values of different types.
type Tuple2[T1, T2 any] struct {
	First  T1
	Second T2
}

// NewTuple2 creates a new Tuple2 with the given values.
func NewTuple2[T1, T2 any](first T1, second T2) Tuple2[T1, T2] {
	return Tuple2[T1, T2]{
		First:  first,
		Second: second,
	}
}

// SliceToTuple2 converts the first two elements of a slice to a Tuple2. If the
// slice has fewer than two elements, it uses zero values for the missing
// elements.
func SliceToTuple2[T1, T2 any](slice []any) Tuple2[T1, T2] {
	var first T1
	var second T2
	if len(slice) > 0 {
		if val, ok := slice[0].(T1); ok {
			first = val
		}
	}
	if len(slice) > 1 {
		if val, ok := slice[1].(T2); ok {
			second = val
		}
	}

	return NewTuple2(first, second)
}

// Fold applies a folding function to each element of a slice along with an accumulator value,
// returning a result of type R as determined by the folder function.
// The accumulator is passed as part of a Tuple2 structure.
func TFold[A, B, R any](
	tup Tuple2[A, B],
	folder func(Tuple2[A, B]) R,
) R {
	return folder(tup)
}

// CurriedFold returns a function that applies the folder function to a Tuple2,
// returning a result of type R. This is a curried version of Fold.
func CurriedTFold[A, B, R any](
	folder func(Tuple2[A, B]) R,
) func(Tuple2[A, B]) R {
	return func(tup Tuple2[A, B]) R {
		return TFold(tup, folder)
	}
}

// IsEmpty checks if a slice is empty (has zero elements). Returns true if the
// slice is nil or has no elements, false otherwise.
func IsEmpty[T any](slice []T) bool {
	return len(slice) == 0
}
