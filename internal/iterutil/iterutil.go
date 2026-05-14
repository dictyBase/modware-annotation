// Package iterutil provides generic iterator utility functions for Go's iter.Seq type.
package iterutil

import "iter"

// A Zipped is a pair of zipped values, one of which may be missing,
// drawn from two different sequences.
type Zipped[V1, V2 any] struct {
	V1  V1
	Ok1 bool // whether V1 is present (if not, it will be zero)
	V2  V2
	Ok2 bool // whether V2 is present (if not, it will be zero)
}

// Zip returns an iterator that iterates x and y in parallel,
// yielding Zipped values of successive elements of x and y.
// If one sequence ends before the other, the iteration continues
// with Zipped values in which either Ok1 or Ok2 is false,
// depending on which sequence ended first.
func Zip[V1, V2 any](
	xit iter.Seq[V1],
	yit iter.Seq[V2],
) iter.Seq[Zipped[V1, V2]] {
	return func(yield func(z Zipped[V1, V2]) bool) {
		next, stop := iter.Pull(yit)
		defer stop()
		vn2, ok2 := next()
		for v1 := range xit {
			if !yield(Zipped[V1, V2]{v1, true, vn2, ok2}) {
				return
			}
			vn2, ok2 = next()
		}
		var zv1 V1
		for ok2 {
			if !yield(Zipped[V1, V2]{zv1, false, vn2, ok2}) {
				return
			}
			vn2, ok2 = next()
		}
	}
}

// Equal reports whether the two sequences are equal.
func Equal[V comparable](xit, yit iter.Seq[V]) bool {
	for z := range Zip(xit, yit) {
		if z.Ok1 != z.Ok2 || z.V1 != z.V2 {
			return false
		}
	}
	return true
}

// Reduce combines the values in seq using f.
// For each value v in seq, it updates sum = f(sum, v)
// and then returns the final sum.
// For example, if iterating over seq yields v1, v2, v3,
// Reduce returns f(f(f(sum, v1), v2), v3).
func Reduce[Sum, V any](
	f func(Sum, V) Sum,
	sum Sum,
	seq iter.Seq[V],
) Sum {
	for v := range seq {
		sum = f(sum, v)
	}
	return sum
}

// Map returns an iterator over f applied to seq.
func Map[In, Out any](
	f func(In) Out,
	seq iter.Seq[In],
) iter.Seq[Out] {
	return func(yield func(Out) bool) {
		for in := range seq {
			if !yield(f(in)) {
				return
			}
		}
	}
}

// Filter returns an iterator over seq that only includes
// the values v for which f(v) is true.
func Filter[V any](
	f func(V) bool,
	seq iter.Seq[V],
) iter.Seq[V] {
	return func(yield func(V) bool) {
		for v := range seq {
			if f(v) && !yield(v) {
				return
			}
		}
	}
}

// Delete returns an iterator over seq that excludes
// the values v for which f(v) is true.
func Delete[V any](
	f func(V) bool,
	seq iter.Seq[V],
) iter.Seq[V] {
	return func(yield func(V) bool) {
		for v := range seq {
			if !f(v) && !yield(v) {
				return
			}
		}
	}
}

// Any returns true if at least one of the elements in seq
// matches the predicate f, false otherwise.
func Any[V any](
	f func(V) bool,
	seq iter.Seq[V],
) bool {
	for v := range seq {
		if f(v) {
			return true
		}
	}
	return false
}

// Concat returns an iterator over the concatenation of the sequences.
func Concat[V any](seqs ...iter.Seq[V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, seq := range seqs {
			for e := range seq {
				if !yield(e) {
					return
				}
			}
		}
	}
}

// Contains returns true if element is present in seq, false otherwise.
func Contains[V comparable](
	element V,
	seq iter.Seq[V],
) bool {
	for v := range seq {
		if v == element {
			return true
		}
	}
	return false
}

// Find is an alias for Contains.
func Find[V comparable](
	element V,
	seq iter.Seq[V],
) bool {
	return Contains(element, seq)
}

// MapWith returns a curried version of Map that takes a transformation function
// and returns a function that applies it to a sequence.
func MapWith[In, Out any](f func(In) Out) func(iter.Seq[In]) iter.Seq[Out] {
	return func(seq iter.Seq[In]) iter.Seq[Out] {
		return Map(f, seq)
	}
}

// FilterWith returns a curried version of Filter that takes a predicate function
// and returns a function that filters a sequence.
func FilterWith[V any](f func(V) bool) func(iter.Seq[V]) iter.Seq[V] {
	return func(seq iter.Seq[V]) iter.Seq[V] {
		return Filter(f, seq)
	}
}

// DeleteWith returns a curried version of Delete that takes a predicate function
// and returns a function that removes matching elements from a sequence.
func DeleteWith[V any](f func(V) bool) func(iter.Seq[V]) iter.Seq[V] {
	return func(seq iter.Seq[V]) iter.Seq[V] {
		return Delete(f, seq)
	}
}

// AnyWith returns a curried version of Any that takes a predicate function
// and returns a function that checks if any element matches.
func AnyWith[V any](f func(V) bool) func(iter.Seq[V]) bool {
	return func(seq iter.Seq[V]) bool {
		return Any(f, seq)
	}
}

// ReduceWith returns a curried version of Reduce that takes a reducer function
// and initial value, returning a function that reduces a sequence.
func ReduceWith[Sum, V any](f func(Sum, V) Sum, sum Sum) func(iter.Seq[V]) Sum {
	return func(seq iter.Seq[V]) Sum {
		return Reduce(f, sum, seq)
	}
}

// ContainsElement returns a curried version of Contains that takes an element
// and returns a function that checks if the element is present in a sequence.
func ContainsElement[V comparable](element V) func(iter.Seq[V]) bool {
	return func(seq iter.Seq[V]) bool {
		return Contains(element, seq)
	}
}
