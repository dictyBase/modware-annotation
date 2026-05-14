package arangodb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildListAnnoBindVars(t *testing.T) {
	t.Parallel()
	const (
		anno   = "annotation"
		cv     = "cv"
		cvterm = "cvterm"
		graph  = "anno_cvterm_graph"
		limit  = int64(10)
	)

	t.Run(
		"FirstFilter includes anno_collection and excludes cvterm_collection",
		func(t *testing.T) {
			t.Parallel()
			assert := require.New(t)
			vars := buildListAnnoBindVars(FirstFilter, anno, cv, cvterm, graph, limit, 0)
			assert.Equal(anno, vars["@anno_collection"])
			assert.Equal(cv, vars[cvCollectionBind])
			assert.NotContains(vars, cvtermCollectionBind)
			assert.NotContains(vars, "cursor")
			assert.Equal(limit+1, vars["limit"])
		},
	)

	t.Run(
		"SecondFilter excludes anno_collection and includes cvterm_collection",
		func(t *testing.T) {
			t.Parallel()
			assert := require.New(t)
			vars := buildListAnnoBindVars(SecondFilter, anno, cv, cvterm, graph, limit, 0)
			assert.NotContains(vars, "@anno_collection")
			assert.Equal(cv, vars[cvCollectionBind])
			assert.Equal(cvterm, vars[cvtermCollectionBind])
			assert.NotContains(vars, "cursor")
		},
	)

	t.Run(
		"BothFilters includes anno_collection and excludes cvterm_collection",
		func(t *testing.T) {
			t.Parallel()
			assert := require.New(t)
			vars := buildListAnnoBindVars(BothFilters, anno, cv, cvterm, graph, limit, 0)
			assert.Equal(anno, vars["@anno_collection"])
			assert.Equal(cv, vars[cvCollectionBind])
			assert.NotContains(vars, cvtermCollectionBind)
		},
	)

	t.Run("cursor key is present when cursor is non-zero", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		vars := buildListAnnoBindVars(FirstFilter, anno, cv, cvterm, graph, limit, 12345)
		assert.Equal(int64(12345), vars["cursor"])
	})

	t.Run("cursor key is absent when cursor is zero", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		vars := buildListAnnoBindVars(SecondFilter, anno, cv, cvterm, graph, limit, 0)
		assert.NotContains(vars, "cursor")
	})

	t.Run(
		"SecondFilter with cursor includes cvterm_collection and cursor but not anno_collection",
		func(t *testing.T) {
			t.Parallel()
			assert := require.New(t)
			vars := buildListAnnoBindVars(SecondFilter, anno, cv, cvterm, graph, 5, 99999)
			assert.NotContains(vars, "@anno_collection")
			assert.Equal(cvterm, vars[cvtermCollectionBind])
			assert.Equal(int64(99999), vars["cursor"])
			assert.Equal(int64(6), vars["limit"])
		},
	)

	t.Run("anno_cvterm_graph is always present", func(t *testing.T) {
		t.Parallel()
		assert := require.New(t)
		for _, stype := range []StatementType{FirstFilter, SecondFilter, BothFilters} {
			vars := buildListAnnoBindVars(stype, anno, cv, cvterm, graph, limit, 0)
			assert.Equal(graph, vars["anno_cvterm_graph"], "graph missing for type %s", stype)
		}
	})
}
