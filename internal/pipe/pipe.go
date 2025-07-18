// Package pipe provides functional pipeline utilities for composing functions
// in sequence. These utilities enable clean and readable functional programming
// patterns by allowing multiple functions to be chained together.
package pipe

// Pipe2 creates a functional pipeline by taking an initial value and applying
// two functions in succession. The output of the first function becomes the
// input to the second function. The final return value is the result of the
// last function application.
func Pipe2[T1, T2, T3 any](tup T1, f1 func(T1) T2, fn2 func(T2) T3) T3 {
	return fn2(f1(tup))
}

// Pipe3 creates a functional pipeline by taking an initial value and applying
// three functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe3[T1, T2, T3, T4 any](
	initial T1,
	f1 func(T1) T2,
	fn2 func(T2) T3,
	fn3 func(T3) T4,
) T4 {
	return fn3(fn2(f1(initial)))
}

// Pipe4 creates a functional pipeline by taking an initial value and applying
// four functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe4[T1, T2, T3, T4, T5 any](
	initial T1,
	f1 func(T1) T2,
	fn2 func(T2) T3,
	fn3 func(T3) T4,
	fn4 func(T4) T5,
) T5 {
	return fn4(fn3(fn2(f1(initial))))
}

// Pipe5 creates a functional pipeline by taking an initial value and applying
// five functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe5[T1, T2, T3, T4, T5, T6 any](
	initial T1,
	fn1 func(T1) T2,
	fn2 func(T2) T3,
	fn3 func(T3) T4,
	fn4 func(T4) T5,
	fn5 func(T5) T6,
) T6 {
	return fn5(fn4(fn3(fn2(fn1(initial)))))
}

// Pipe6 creates a functional pipeline by taking an initial value and applying
// six functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe6[T1, T2, T3, T4, T5, T6, T7 any](
	initial T1,
	fn1 func(T1) T2,
	fn2 func(T2) T3,
	fn3 func(T3) T4,
	fn4 func(T4) T5,
	fn5 func(T5) T6,
	fn6 func(T6) T7,
) T7 {
	return fn6(fn5(fn4(fn3(fn2(fn1(initial))))))
}

// Pipe7 creates a functional pipeline by taking an initial value and applying
// seven functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe7[T1, T2, T3, T4, T5, T6, T7, T8 any](
	initial T1,
	fn1 func(T1) T2,
	fn2 func(T2) T3,
	fn3 func(T3) T4,
	fn4 func(T4) T5,
	fn5 func(T5) T6,
	fn6 func(T6) T7,
	fn7 func(T7) T8,
) T8 {
	return fn7(fn6(fn5(fn4(fn3(fn2(fn1(initial)))))))
}

// Pipe8 creates a functional pipeline by taking an initial value and applying
// eight functions in succession. The output of each function becomes the input
// to the next function. The final return value is the result of the last
// function application.
func Pipe8[T1, T2, T3, T4, T5, T6, T7, T8, T9 any](
	initial T1,
	fn1 func(T1) T2,
	fn2 func(T2) T3,
	fn3 func(T3) T4,
	fn4 func(T4) T5,
	fn5 func(T5) T6,
	fn6 func(T6) T7,
	fn7 func(T7) T8,
	fn8 func(T8) T9,
) T9 {
	return fn8(fn7(fn6(fn5(fn4(fn3(fn2(fn1(initial))))))))
}
