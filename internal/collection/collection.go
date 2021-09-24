package collection

// IncludeString determines whether the given string
// string is included in the string slice
func IncludeString(a []string, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}

// RemoveStringItems removes elements from a that are present in
// items
func RemoveStringItems(a []string, items ...string) []string {
	var s []string
	for _, v := range a {
		if !IncludeString(items, v) {
			s = append(s, v)
		}
	}
	return s
}
