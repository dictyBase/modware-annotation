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

// MapSeq transforms an iter.Seq to another iter.Seq by applying the given
// function to each element in the sequence.
// Write unit tests. AI!
func MapSeq[T1, T2 any](seq iter.Seq[T1], fn func(T1) T2) iter.Seq[T2] {
	return func(yield func(T2) bool) {
		for v := range seq {
			if !yield(fn(v)) {
				return
			}
		}
	}
}
