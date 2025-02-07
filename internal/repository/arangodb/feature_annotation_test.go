package arangodb

import (
	"slices"
	"testing"
	"time"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Add test feature annotation first
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding test feature annotation")

	got, err := repo.GetFeatureAnnotation(added.AnnoId)
	asrt.NoError(err, "expected no error getting feature annotation")
	validateBasicFields(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        got,
		base:       feat,
	})
	validateDbLinks(validateDbLinksParams{
		t:          t,
		assertions: asrt,
		got:        got.DbLinks,
		expected:   feat.Attributes.Dblinks,
	})
	validateProperties(validatePropertiesParams{
		t:          t,
		assertions: asrt,
		got:        got.Properties,
		expected:   feat.Attributes.Properties,
	})

	_, err = repo.GetFeatureAnnotation("non_existent_id")
	asrt.Error(err, "expected error for non-existent feature annotation")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func TestAddFeatureAnnotationBasic(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	baseFeat := &feature.NewFeatureAnnotation{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
	}
	for _, nfeat := range getBasicTestCases() {
		baseFeat.Attributes = nfeat.Attributes
		baseFeat.Id = nfeat.Id
		doc, err := repo.AddFeatureAnnotation(baseFeat)
		asrt.NoError(err, "expected no error adding feature annotation")
		validateBasicFields(validateFeatureAnnotationParams{
			t:          t,
			assertions: asrt,
			got:        doc,
			base:       baseFeat,
		})
	}
}

func TestAddFeatureAnnotationFull(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	feat := getCombinedFeatureDoc(getBaseFeatureDoc, getMultiPropertyTestCase)
	doc, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding feature annotation")
	validateBasicFields(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        doc,
		base:       feat,
	})
	validateDbLinks(validateDbLinksParams{
		t:          t,
		assertions: asrt,
		got:        doc.DbLinks,
		expected:   feat.Attributes.Dblinks,
	})
	validateProperties(validatePropertiesParams{
		t:          t,
		assertions: asrt,
		got:        doc.Properties,
		expected:   feat.Attributes.Properties,
	})
}

func TestAddFeatureAnnotationMultiProperty(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	feat := getCombinedFeatureDoc(getBaseFeatureDoc, getMultiPropertyTestCase)
	doc, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding feature annotation")
	validateBasicFields(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        doc,
		base:       feat,
	})
	validateProperties(validatePropertiesParams{
		t:          t,
		assertions: asrt,
		got:        doc.Properties,
		expected:   feat.Attributes.Properties,
	})
}

func TestAddDuplicateFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create base feature annotation
	feat := &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "gene name",
		},
	}

	// Add first feature annotation
	_, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding first feature annotation")

	// Attempt to add duplicate feature annotation
	_, err = repo.AddFeatureAnnotation(feat)
	asrt.Error(err, "expected error when adding duplicate feature annotation")
	asrt.Contains(
		err.Error(),
		"unique constraint violated",
		"expected duplicate error message",
	)
}

func TestRemoveFeatureAnnotation(t *testing.T) {
	t.Parallel()
	for _, testCase := range getRemoveTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var identifier string
			assert, repo := setUpFeatureTest(t)
			t.Cleanup(func() { _ = repo.Dbh().Drop() })
			if testCase.wantErr {
				identifier = "non_existent_id"
			} else {
				doc, err := repo.AddFeatureAnnotation(getFullFeatureDoc())
				assert.NoError(err, "expected no error adding test feature annotation")
				identifier = doc.AnnoId
			}
			err := repo.RemoveFeatureAnnotation(identifier, testCase.purge)
			if testCase.wantErr {
				assert.Error(
					err,
					"expected error removing non-existent feature annotation",
				)

				return
			}
			assert.NoError(err, "expected no error removing feature annotation")
			_, err = repo.GetFeatureAnnotation(identifier)
			assert.Error(
				err,
				"expected error getting removed feature annotation",
			)
			assert.True(
				repository.IsAnnotationNotFound(err),
				"should be annotation not found error",
			)
		})
	}
}

func TestUpdateExistingFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	added, err := repo.AddFeatureAnnotation(getCompleteFeatureDoc())
	asrt.NoError(err, "expected no error adding initial feature annotation")

	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoId,
		UpdatedBy: "updater@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:     "updated name",
			Synonyms: []string{"new_syn1", "new_syn2"},
		},
	}

	doc, err := repo.EditFeatureAnnotation(update)
	asrt.NoError(err)
	asrt.Equal(update.UpdatedBy, doc.UpdatedBy)
	asrt.Equal("updated name", doc.Name)
	// Combined synonyms check
	expectedSynonyms := slices.Concat(
		added.Synonyms,
		update.Attributes.Synonyms,
	)
	slices.Sort(expectedSynonyms)
	slices.Sort(doc.Synonyms)
	asrt.ElementsMatch(
		expectedSynonyms,
		doc.Synonyms,
		"should have combined synonyms",
	)
}
	}
}

func TestAddTagToExistingFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tag request
	tagReq := &feature.AddTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyCreate{
			Tag:       "test_tag",
			Value:     "test_value",
			CreatedBy: "tester@example.org",
		},
	}

	// Add tag
	updated, err := repo.AddTag(tagReq)
	asrt.NoError(err, "should successfully add tag")
	asrt.Len(
		updated.Properties,
		len(feat.Attributes.Properties)+1,
		"should have one more tag",
	)

	// Verify added tag
	found := false
	for _, prop := range updated.Properties {
		if prop.Tag == tagReq.Tag.Tag {
			found = true
			asrt.Equal(tagReq.Tag.Value, prop.Value, "should match tag value")
			asrt.Equal(
				tagReq.Tag.CreatedBy,
				prop.CreatedBy,
				"should match created by",
			)
			asrt.False(
				prop.CreatedAt.IsZero(),
				"should have creation timestamp",
			)
			asrt.False(prop.UpdatedAt.IsZero(), "should have update timestamp")
		}
	}
	asrt.True(found, "should find added tag")

	// Verify other fields remain unchanged
	asrt.Equal(added.Name, updated.Name, "name should remain unchanged")
	asrt.Equal(
		added.CreatedBy,
		updated.CreatedBy,
		"created_by should remain unchanged",
	)
}

func TestAddTagToNonExistentFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Attempt to add tag to non-existent feature
	_, err := repo.AddTag(&feature.AddTagRequest{
		Id: "DDB_G0000000",
		Tag: &feature.TagPropertyCreate{
			Tag:       "test_tag",
			Value:     "test_value",
			CreatedBy: "tester@example.org",
		},
	})

	asrt.Error(err, "should return error for non-existent feature")
	asrt.True(repository.IsAnnotationNotFound(err), "should be not found error")
}

func TestUpdateExistingTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Setup initial feature with tag
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should create test feature")

	tagReq := &feature.AddTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyCreate{
			Tag:       "update_test",
			Value:     "initial",
			CreatedBy: "tester@example.org",
		},
	}
	tagged, err := repo.AddTag(tagReq)
	asrt.NoError(err, "should add initial tag")

	// Update request
	updateReq := &feature.UpdateTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyUpdate{
			Tag:       "update_test",
			Value:     "updated",
			UpdatedBy: "updater@example.org",
		},
	}

	// Execute update
	updated, err := repo.UpdateTag(updateReq)
	asrt.NoError(err, "should successfully update tag")

	// Verify changes
	var found bool
	for _, prop := range updated.Properties {
		if prop.Tag == "update_test" {
			found = true
			asrt.Equal("updated", prop.Value, "should update value")
			asrt.Equal(
				"updater@example.org",
				prop.UpdatedBy,
				"should update modifier",
			)
			asrt.Equal(
				"tester@example.org",
				prop.CreatedBy,
				"should preserve creator",
			)
			asrt.False(prop.UpdatedAt.IsZero(), "should set update timestamp")
		}
	}
	asrt.True(found, "should find updated tag")
	asrt.Equal(tagged.Name, updated.Name, "should preserve feature name")
}

func TestUpdateNonExistentTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create feature without tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should create test feature")

	// Attempt to update missing tag
	_, err = repo.UpdateTag(&feature.UpdateTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyUpdate{
			Tag:       "ghost_tag",
			Value:     "new_value",
			UpdatedBy: "tester@example.org",
		},
	})

	asrt.Error(err, "should return error for missing tag")
}

func TestRemoveTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create feature with tag
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should create base feature")

	// Add test tag
	tagReq := &feature.AddTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyCreate{
			Tag:       "remove_me",
			Value:     "temp_value",
			CreatedBy: "tester@example.org",
		},
	}
	tagged, err := repo.AddTag(tagReq)
	asrt.NoError(err, "should add test tag")
	asrt.Len(
		tagged.Properties,
		len(feat.Attributes.Properties)+1,
		"should have initial tag",
	)

	// Remove tag
	err = repo.RemoveTag(&feature.RemoveTagRequest{
		Id:  added.AnnoId,
		Tag: "remove_me",
	})
	asrt.NoError(err, "should successfully remove tag")

	// Verify removal by fetching updated document
	updated, err := repo.GetFeatureAnnotation(added.AnnoId)
	asrt.NoError(err, "should fetch updated document")
	// Check tag removal
	var found bool
	for _, prop := range updated.Properties {
		if prop.Tag == "remove_me" {
			found = true
		}
	}
	asrt.False(found, "removed tag should not exist in properties")
	asrt.Len(
		updated.Properties,
		len(tagged.Properties)-1,
		"should reduce properties count by 1",
	)
	asrt.Equal(added.Name, updated.Name, "should preserve feature name")
	asrt.Equal(added.CreatedBy, updated.CreatedBy, "should preserve created_by")
}

func TestRemoveNonExistentTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create feature without tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should create test feature")

	// Attempt to remove tag
	err = repo.RemoveTag(&feature.RemoveTagRequest{
		Id:  added.AnnoId,
		Tag: "ghost_tag",
	})
	asrt.Error(err, "should return error for missing tag")
}
