package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsAllStopwords(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	assert.True(IsAllStopwords(""))
	assert.True(IsAllStopwords("   "))
	assert.True(IsAllStopwords("the"))
	assert.True(IsAllStopwords("the and of in"))
	assert.False(IsAllStopwords("the fruiting body"))
	assert.False(IsAllStopwords("axea1"))
	assert.False(IsAllStopwords("---"))
}
