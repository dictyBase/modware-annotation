package collection

// Map returns the slice obtained after applying the given function over every
// element in the given slice.
func Map[T1, T2 any](slc []T1, fnc func(T1) T2) []T2 {
	ret := make([]T2, 0)
	for _, elem := range slc {
		ret = append(ret, fnc(elem))
	}

	return ret
}

// IncludeString determines whether the given string
// string is included in the string slice.
func IncludeString(a []string, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}

	return false
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
