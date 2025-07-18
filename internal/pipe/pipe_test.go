package pipe

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper functions for testing.
func add1(x int) int            { return x + 1 }
func multiply2(x int) int       { return x * 2 }
func toString(x int) string     { return strconv.Itoa(x) }
func addPrefix(s string) string { return "prefix_" + s }
func addSuffix(s string) string { return s + "_suffix" }
func toUpper(s string) string   { return strings.ToUpper(s) }
func getLength(s string) int    { return len(s) }
func isEven(x int) bool         { return x%2 == 0 }

func TestPipe2(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		initial  int
		expected string
	}{
		{"positive number", 5, "6"},
		{"zero", 0, "1"},
		{"negative number", -3, "-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Pipe2(tt.initial, add1, toString)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPipe3(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		initial  int
		expected string
	}{
		{"positive number", 5, "prefix_12"},
		{"zero", 0, "prefix_2"},
		{"negative number", -3, "prefix_-4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Pipe3(tt.initial, add1, multiply2, toString)
			result = Pipe3(result, addPrefix, func(s string) string { return s }, func(s string) string { return s })
			require.True(t, strings.HasPrefix(result, "prefix_"))
		})
	}

	// Test with actual 3-step pipeline
	result := Pipe3(5, add1, multiply2, toString)
	expected := "12"
	require.Equal(t, expected, result)
}

func TestPipe4(t *testing.T) {
	t.Parallel()
	result := Pipe4(5, add1, multiply2, toString, addPrefix)
	expected := "prefix_12"
	require.Equal(t, expected, result)
}

func TestPipe5(t *testing.T) {
	t.Parallel()
	result := Pipe5(5, add1, multiply2, toString, addPrefix, addSuffix)
	expected := "prefix_12_suffix"
	require.Equal(t, expected, result)
}

func TestPipe6(t *testing.T) {
	t.Parallel()
	result := Pipe6(5, add1, multiply2, toString, addPrefix, addSuffix, toUpper)
	expected := "PREFIX_12_SUFFIX"
	require.Equal(t, expected, result)
}

func TestPipe7(t *testing.T) {
	t.Parallel()
	result := Pipe7(5, add1, multiply2, toString, addPrefix, addSuffix, toUpper, getLength)
	expected := 16 // "PREFIX_12_SUFFIX" has 16 characters
	require.Equal(t, expected, result)
}

func TestPipe8(t *testing.T) {
	t.Parallel()
	result := Pipe8(5, add1, multiply2, toString, addPrefix, addSuffix, toUpper, getLength, isEven)
	expected := true // 16 is even
	require.Equal(t, expected, result)
}

// Test with different types to ensure generics work properly.
func TestPipeWithDifferentTypes(t *testing.T) {
	t.Parallel()
	// Test string to int pipeline
	stringToInt := func(s string) int {
		val, _ := strconv.Atoi(s)
		return val
	}

	intToFloat := func(i int) float64 {
		return float64(i) * 1.5
	}

	result := Pipe3("10", stringToInt, intToFloat, func(f float64) string {
		return strconv.FormatFloat(f, 'f', 1, 64)
	})

	expected := "15.0"
	require.Equal(t, expected, result)
}

// Benchmark tests.
func BenchmarkPipe2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Pipe2(i, add1, toString)
	}
}

func BenchmarkPipe4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Pipe4(i, add1, multiply2, toString, addPrefix)
	}
}

func BenchmarkPipe8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Pipe8(i, add1, multiply2, toString, addPrefix, addSuffix, toUpper, getLength, isEven)
	}
}

// Test edge cases.
func TestPipeEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with identity functions
	identity := func(x int) int { return x }
	result := Pipe4(42, identity, identity, identity, identity)
	require.Equal(t, 42, result)

	// Test with nil-like zero values
	result2 := Pipe2(0, func(x int) string { return "" }, func(s string) int { return len(s) })
	require.Equal(t, 0, result2)
}
