package service

import (
	"context"
	"testing"
)

func TestCreateFeatureAnnotation(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testCreateValidFeature(params)
	testCreateMissingFields(params)
	testCreateDuplicateFeature(params)
}

func TestGetFeatureAnnotation(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testGetExistingFeature(params)
	testGetNonExistentFeature(params)
	testGetFeatureWithInvalidID(params)
}

func TestUpdateFeatureAnnotation(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testUpdateExistingFeature(params)
	testUpdateNonExistentFeature(params)
	testUpdateWithInvalidData(params)
}
