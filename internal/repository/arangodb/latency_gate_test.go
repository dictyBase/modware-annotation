package arangodb

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	driver "github.com/arangodb/go-driver"
	"github.com/stretchr/testify/require"
)

const (
	// latencyRuns is the number of measured statement runs. Fifty runs make
	// the p95 estimate stable enough to gate on.
	latencyRuns = 50
	// latencyWarmupRuns is the number of unrecorded runs before a measurement,
	// so the first recorded sample does not carry a cold server.
	latencyWarmupRuns = 10
	// latencyCeilingRatio is the accepted multiple of the control-derived
	// ceiling. Both gates compare a statement with a control statement of the
	// same shape, so this ratio covers the run-to-run noise of a host and not
	// the speed of the host itself.
	latencyCeilingRatio = 1.25
)

// measureStatementLatency warms the statement up and then records latencyRuns
// samples of one full request, in milliseconds, sorted in ascending order. It
// reports the row count of the last sample, so a caller can detect a fixture
// that no longer matches the statement.
func measureStatementLatency[T any](
	t *testing.T,
	dbh driver.Database,
	ctx context.Context,
	statement string,
	params map[string]any,
) ([]float64, int) {
	t.Helper()

	for range latencyWarmupRuns {
		measureOneStatementRun[T](t, dbh, ctx, statement, params)
	}

	timings := make([]float64, 0, latencyRuns)
	rows := 0
	for range latencyRuns {
		elapsed, seen := measureOneStatementRun[T](t, dbh, ctx, statement, params)
		timings = append(timings, elapsed)
		rows = seen
	}

	slices.Sort(timings)

	return timings, rows
}

// measureOneStatementRun runs one statement to completion and returns the
// elapsed milliseconds and the number of decoded rows.
func measureOneStatementRun[T any](
	t *testing.T,
	dbh driver.Database,
	ctx context.Context,
	statement string,
	params map[string]any,
) (float64, int) {
	t.Helper()
	started := time.Now()
	cursor, queryErr := dbh.Query(ctx, statement, params)
	require.NoError(t, queryErr, "expect the measured statement to run")
	rows := 0
	for cursor.HasMore() {
		var row T
		_, readErr := cursor.ReadDocument(ctx, &row)
		require.NoError(t, readErr, "expect the measured row to decode")
		rows++
	}
	require.NoError(t, cursor.Close(), "expect the cursor to close")
	elapsed := time.Since(started)

	return float64(elapsed.Microseconds()) / 1000, rows
}

// p95Of returns the p95 sample of sorted timings.
func p95Of(timings []float64) float64 {
	return timings[int(math.Ceil(float64(len(timings))*0.95))-1]
}

// medianOf returns the median sample of sorted timings.
func medianOf(timings []float64) float64 {
	return timings[len(timings)/2]
}
