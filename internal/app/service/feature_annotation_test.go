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

func TestAddTags(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testAddTagsSuccess(params)
	testAddTagsSingleTag(params)
	testAddTagsMultipleTags(params)
	testAddTagsAppendToExisting(params)
	testAddTagsEmptyRequest(params)
	testAddTagsDefaultTimestamps(params)
	testAddTagsProvidedTimestamps(params)
	testAddTagsNonExistentFeature(params)
	testAddTagsInvalidRequest(params)
}

func TestSetTags(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testSetTagsSuccess(params)
	testSetTagsSingleTag(params)
	testSetTagsMultipleTags(params)
	testSetTagsReplaceExisting(params)
	testSetTagsEmptyRequest(params)
	testSetTagsDefaultTimestamps(params)
	testSetTagsProvidedTimestamps(params)
	testSetTagsNonExistentFeature(params)
	testSetTagsInvalidRequest(params)
}

func TestRemoveTags(t *testing.T) {
	t.Parallel()
	client, assert := setup(t)
	ctx := context.Background()
	params := &testParams{
		t:      t,
		ctx:    ctx,
		client: client,
		assert: assert,
	}
	testRemoveTagsSuccess(params)
	testRemoveTagsSingleTag(params)
	testRemoveTagsMultipleTags(params)
	testRemoveTagsPartialMatch(params)
	testRemoveTagsNonExistentTag(params)
	testRemoveTagsEmptyProperties(params)
	testRemoveTagsNonExistentFeature(params)
	testRemoveTagsInvalidRequest(params)
}
