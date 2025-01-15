package collection

import (
	"cmp"
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
func RemoveStringItems(a []string, items ...string) []string {
	var str []string
	for _, v := range a {
		if !IncludeString(items, v) {
			str = append(str, v)
		}
	}

	return str
}
