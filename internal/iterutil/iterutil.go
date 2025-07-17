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
