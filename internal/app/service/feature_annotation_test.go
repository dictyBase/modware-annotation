package service

import (
	"context"
	"testing"
	// Assuming assertGrpcError uses this
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

func TestGetFeatureAnnotationByName(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testGetExistingFeatureByName(params)
	testGetNonExistentFeatureByName(params)
	testGetFeatureWithEmptyName(params)
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

func TestListFeatureAnnotationsByPubmedId(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testListByPubmedIdValid(params)
	testListByPubmedIdNotFound(params)
	testListByPubmedIdInvalid(params)
}

func TestListFeatureAnnotationsByDOI(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testListByDOIValid(params)
	testListByDOINotFound(params)
	testListByDOIInvalid(params)
}
