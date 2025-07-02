package arangodb

import (
	"slices"
	"testing"
	"time"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Add test feature annotation first
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding test feature annotation")

	got, err := repo.GetFeatureAnnotation(added.AnnoId)
	asrt.NoError(err, "expected no error getting feature annotation")

	// Use the consolidated validation helper
	validateCompleteFeatureAnnotation(validateCompleteFeatureParams{
		t:          t,
		assertions: asrt,
		got:        got,
		expected:   feat,
	})

	// Check specific length conditions for this test case if needed (optional)
	asrt.NotEmpty(
		got.Pubmed,
		"should have pubmed ids in result for this test case")
	asrt.NotEmpty(
		got.Publications,
		"should have publications in result for this test case",
	)

	_, err = repo.GetFeatureAnnotation("non_existent_id")
	asrt.Error(err, "expected error for non-existent feature annotation")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func TestGetFeatureAnnotationByName(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// 1. Success Case
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(
		err,
		"expected no error adding test feature annotation for name lookup",
	)

	// Retrieve by name
	gotByName, err := repo.GetFeatureAnnotationByName(feat.Attributes.Name)
	asrt.NoError(
		err,
		"expected no error getting feature annotation by name",
	)
	// Verify the retrieved document matches the added one (using ID for simplicity)
	asrt.Equal(
		added.AnnoId,
		gotByName.AnnoId,
		"retrieved document ID should match added document ID",
	)
	asrt.Equal(
		feat.Attributes.Name,
		gotByName.Name,
		"retrieved document name should match",
	)
	// Optionally, use the full validation helper if needed
	validateCompleteFeatureAnnotation(validateCompleteFeatureParams{
		t:          t,
		assertions: asrt,
		got:        gotByName,
		expected:   feat,
	})

	// 2. Not Found Case
	nonExistentName := "non_existent_feature_name"
	_, err = repo.GetFeatureAnnotationByName(nonExistentName)
	asrt.Error(
		err,
		"expected error for non-existent feature annotation name",
	)
	asrt.True(
		repository.IsFeatureNameNotFound(err),
		"should be FeatureNameNotFoundError",
	)
}

func TestAddFeatureAnnotationBasic(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
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
	t.Cleanup(cleanupDB(repo))
	// Use the complete doc which includes both pubmed and publications
	feat := getCompleteFeatureDoc()
	doc, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding feature annotation")

	// Use the consolidated validation helper
	validateCompleteFeatureAnnotation(validateCompleteFeatureParams{
		t:          t,
		assertions: asrt,
		got:        doc,
		expected:   feat,
	})

	// Check specific length conditions for this test case if needed (optional)
	asrt.NotEmpty(
		doc.Pubmed,
		"should have pubmed ids in result for this test case")
	asrt.NotEmpty(
		doc.Publications,
		"should have publications in result for this test case",
	)
}

func TestAddFeatureAnnotationMultiProperty(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
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
	t.Cleanup(cleanupDB(repo))

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
	t.Cleanup(cleanupDB(repo))
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

func TestUpdateNonExistentFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	update := &feature.FeatureAnnotationUpdate{
		Id:        "non_existent_id",
		UpdatedBy: "updater@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "will not update",
		},
	}

	_, err := repo.EditFeatureAnnotation(update)
	asrt.Error(err)
	asrt.True(repository.IsAnnotationNotFound(err))
}

func TestAddPropertiesToExistingFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	added, err := repo.AddFeatureAnnotation(getCompleteFeatureDoc())
	asrt.NoError(err, "expected no error adding initial feature annotation")

	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoId,
		UpdatedBy: "updater@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Properties: []*feature.TagProperty{
				{
					Tag:       "description",
					Value:     "updated description",
					CreatedBy: "creator3@email.com",
					UpdatedBy: "updater@email.com",
					CreatedAt: timestamppb.New(time.Now()),
					UpdatedAt: timestamppb.New(time.Now()),
				},
			},
		},
	}

	doc, err := repo.EditFeatureAnnotation(update)
	asrt.NoError(err)
	asrt.Len(doc.Properties, 2)

	// Create expected properties by combining original + new
	expectedProperties := slices.Concat(
		added.Properties,
		collection.Map(update.Attributes.Properties, convertProperty),
	)
	slices.SortFunc(expectedProperties, sortTagProperties)
	slices.SortFunc(doc.Properties, sortTagProperties)
	asrt.ElementsMatch(
		expectedProperties,
		doc.Properties,
		"should have combined properties",
	)
}

func TestUpdatePublications_AppendDOI(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Start with a feature that has DOIs
	initialDoc := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(initialDoc)
	asrt.NoError(err, "expected no error adding initial feature annotation")
	asrt.NotEmpty(added.Publications, "Initial document should have DOIs")

	newDOIs := []string{"doi:10.1000/new1", "doi:10.1000/new2"}
	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoId,
		UpdatedBy: "doi_updater@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Publications: newDOIs,
		},
	}

	doc, err := repo.EditFeatureAnnotation(update)
	asrt.NoError(err)

	// Combine initial and new DOIs for expected result
	expectedDOIs := slices.Concat(added.Publications, newDOIs)
	slices.Sort(expectedDOIs)
	slices.Sort(doc.Publications)
	asrt.ElementsMatch(
		expectedDOIs,
		doc.Publications,
		"Publications (DOIs) should contain both initial and newly added DOIs",
	)
	asrt.Equal(update.UpdatedBy, doc.UpdatedBy, "UpdatedBy should be updated")
}

func TestUpdatePublications_AppendPubmed(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Start with a feature that has Pubmed IDs
	initialDoc := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(initialDoc)
	asrt.NoError(err, "expected no error adding initial feature annotation")
	asrt.NotEmpty(added.Pubmed, "Initial document should have Pubmed IDs")

	newPubmedIDs := []string{"76543", "4839439"}
	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoId,
		UpdatedBy: "pubmed_updater@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Pubmed: newPubmedIDs,
		},
	}

	doc, err := repo.EditFeatureAnnotation(update)
	asrt.NoError(err)

	// Combine initial and new Pubmed IDs for expected result
	expectedPubmedIDs := slices.Concat(added.Pubmed, newPubmedIDs)
	slices.Sort(expectedPubmedIDs)
	slices.Sort(doc.Pubmed)
	asrt.ElementsMatch(
		expectedPubmedIDs,
		doc.Pubmed,
		"Pubmed IDs should contain both initial and newly added IDs",
	)
	asrt.Equal(update.UpdatedBy, doc.UpdatedBy, "UpdatedBy should be updated")
	// Verify DOIs are also appended (as per DOI append logic)
	expectedDOIs := added.Publications
	slices.Sort(expectedDOIs)
	slices.Sort(doc.Publications)
	asrt.ElementsMatch(
		expectedDOIs,
		doc.Publications,
		"DOIs should remain unchanged",
	)
}

func TestUpdatePublications_AddInitial(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Start with a feature that has NO publications
	initialDoc := getFullFeatureDoc() // Base doc has no pubs
	added, err := repo.AddFeatureAnnotation(initialDoc)
	asrt.NoError(err, "expected no error adding initial feature annotation")
	asrt.Empty(added.Publications, "Initial document should have no DOIs")
	asrt.Empty(added.Pubmed, "Initial document should have no Pubmed IDs")

	newDOIs := []string{"doi:10.1000/new1", "doi:10.1000/new2"}
	newPubmedIDs := []string{"2039439", "934833"}
	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoId,
		UpdatedBy: "initial_pub_adder@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Publications: newDOIs,
			Pubmed:       newPubmedIDs,
		},
	}

	doc, err := repo.EditFeatureAnnotation(update)
	asrt.NoError(err)
	asrt.ElementsMatch(
		collection.Sorted(newDOIs),
		collection.Sorted(doc.Publications),
		"DOIs should be added",
	)
	asrt.ElementsMatch(
		collection.Sorted(newPubmedIDs),
		collection.Sorted(doc.Pubmed),
		"Pubmed IDs should be added",
	)
	asrt.Equal(update.UpdatedBy, doc.UpdatedBy, "UpdatedBy should be updated")
}

// TestUpdatePublications_Simultaneous verifies that updating both DOIs and
// Pubmed IDs simultaneously appends the new IDs to their respective existing lists.
func TestUpdatePublications_Simultaneous(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Start with a feature that HAS publications
	initialDoc := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(initialDoc)
	asrt.NoError(err, "expected no error adding initial feature annotation")
	asrt.NotEmpty(added.Publications, "Initial document should have DOIs")
	asrt.NotEmpty(added.Pubmed, "Initial document should have Pubmed IDs")

	newDOIs := []string{"doi:10.1000/new1", "doi:10.1000/new2"}
	newPubmedIDs := []string{"2039439", "934833"}
	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoId,
		UpdatedBy: "simul_updater@email.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Publications: newDOIs,
			Pubmed:       newPubmedIDs,
		},
	}

	doc, err := repo.EditFeatureAnnotation(update)
	asrt.NoError(err)

	// Calculate expected combined lists
	expectedDOIs := slices.Concat(added.Publications, newDOIs)
	expectedPubmedIDs := slices.Concat(added.Pubmed, newPubmedIDs)

	// Sort for comparison
	slices.Sort(expectedDOIs)
	slices.Sort(doc.Publications)
	slices.Sort(expectedPubmedIDs)
	slices.Sort(doc.Pubmed)

	asrt.ElementsMatch(
		expectedDOIs,
		doc.Publications,
		"DOIs should contain both initial and newly added IDs",
	)
	asrt.ElementsMatch(
		expectedPubmedIDs,
		doc.Pubmed,
		"Pubmed IDs should contain both initial and newly added IDs",
	)
	asrt.Equal(update.UpdatedBy, doc.UpdatedBy, "UpdatedBy should be updated")
}

func TestUpdateFeatureAnnotation_InvalidInput(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Add a feature first so we have a valid ID
	added, err := repo.AddFeatureAnnotation(getBaseFeatureDoc())
	asrt.NoError(err, "expected no error adding base feature annotation")

	// Create an update request missing the required UpdatedBy field
	update := &feature.FeatureAnnotationUpdate{
		Id: added.AnnoId,
		// UpdatedBy: "missing@email.com", // Intentionally missing
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "updated name",
		},
	}

	_, err = repo.EditFeatureAnnotation(update)
	// Expecting a validation error from stepValidateInput
	// The exact error type/message might depend on the validator used.
	// Checking for any error is a basic start. A more specific check
	// for a validation error type would be better if available.
	asrt.Error(err, "Expected an error due to missing UpdatedBy field")
	// Example of a more specific check if using a validation library:
	// var validationErr *validator.ValidationErrors
	// asrt.ErrorAs(err, &validationErr, "Expected a validation error")
}

func TestAddTag_WithDefaultTimestamp(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tag request without timestamp
	tagReq := &feature.AddTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyCreate{
			Tag:       "test_tag_default",
			Value:     "test_value",
			CreatedBy: "tester@example.org",
		},
	}

	// Add tag
	updated, err := repo.AddTag(tagReq)
	asrt.NoError(err, "should successfully add tag")
	asrt.Len(
		updated.Properties,
		len(added.Properties)+1,
		"should have one more tag",
	)

	// Verify added tag
	found, otk := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == tagReq.Tag.Tag
		},
	)
	asrt.True(otk, "should find the newly added tag")
	asrt.Equal(tagReq.Tag.Value, found.Value, "should match tag value")
	asrt.Equal(
		tagReq.Tag.CreatedBy,
		found.CreatedBy,
		"should match created by",
	)
	asrt.WithinDuration(
		time.Now(),
		found.CreatedAt,
		2*time.Second,
		"CreatedAt should be recent",
	)
	asrt.Equal(
		found.CreatedAt,
		found.UpdatedAt,
		"UpdatedAt should match CreatedAt",
	)
}

func TestAddTag_WithProvidedTimestamp(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	specTs := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Microsecond)

	// Create tag request with a specific timestamp
	tagReq := &feature.AddTagRequest{
		Id: added.AnnoId,
		Tag: &feature.TagPropertyCreate{
			Tag:       "test_tag_provided",
			Value:     "test_value_provided",
			CreatedBy: "tester@example.org",
			CreatedAt: timestamppb.New(specTs),
		},
	}

	// Add tag
	updated, err := repo.AddTag(tagReq)
	asrt.NoError(err, "should successfully add tag with provided timestamp")
	asrt.Len(
		updated.Properties,
		len(added.Properties)+1,
		"should have one more tag",
	)

	// Verify added tag
	found, ok := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == tagReq.Tag.Tag
		},
	)
	asrt.True(ok, "should find the newly added tag")
	asrt.Equal(tagReq.Tag.Value, found.Value, "should match tag value")
	asrt.Equal(
		specTs,
		found.CreatedAt,
		"CreatedAt should match provided timestamp",
	)
	asrt.Equal(
		specTs,
		found.UpdatedAt,
		"UpdatedAt should match provided timestamp on creation",
	)
}

func TestAddTagToNonExistentFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

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
	t.Cleanup(cleanupDB(repo))

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
func seedAnnotationWithTags(
	t *testing.T,
	repo repository.FeatureAnnotationRepository,
) *model.FeatureAnnotationDoc {
	t.Helper()
	assert := require.New(t)
	newFeat, err := repo.AddFeatureAnnotation(&feature.NewFeatureAnnotation{
		Id:        "DDB_G0285921",
		CreatedBy: "test@test.com",
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "pkaR",
			Properties: []*feature.TagProperty{
				{
					Tag:       "baz",
					Value:     "quax",
					CreatedBy: "test@test.com",
				},
				{
					Tag:       "foo",
					Value:     "bar",
					CreatedBy: "test@test.com",
				},
			},
		},

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
	t.Cleanup(cleanupDB(repo))

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
	t.Cleanup(cleanupDB(repo))

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
	t.Cleanup(cleanupDB(repo))

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

func TestListByPublicationId_SuccessPubmed(t *testing.T) {
	t.Parallel()
	testListByPublicationIdSuccess(&testListByPublicationIdSuccessParams{
		t:                    t,
		pubID:                "PMID:12345",
		source:               pubmedSource, // Use constant
		featureIDFieldPrefix: "DDB_G000000",
		setPubFunc:           func(attrs *feature.FeatureAnnotationAttributes, ids []string) { attrs.Pubmed = ids },
		unrelatedPubID:       "PMID:67890",
		errorMsgSuffix:       "Pubmed ID",
	})
}

func TestListByPublicationId_SuccessDOI(t *testing.T) {
	t.Parallel()
	testListByPublicationIdSuccess(&testListByPublicationIdSuccessParams{
		t:                    t,
		pubID:                "doi:10.1234/journal.1",
		source:               doiSource, // Use constant
		featureIDFieldPrefix: "DDB_G000001",
		setPubFunc:           func(attrs *feature.FeatureAnnotationAttributes, ids []string) { attrs.Publications = ids },
		unrelatedPubID:       "doi:10.5678/journal.2",
		errorMsgSuffix:       "DOI",
	})
}

func TestListByPublicationId_NotFoundIncorrectID(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Setup: Create a feature linked to a known pubmed ID
	pubID := "PMID:11111"
	source := "pubmed"
	feat1 := getFullFeatureDoc()
	feat1.Id = "DDB_G0000021"
	feat1.Attributes.Pubmed = []string{pubID}
	_, err := repo.AddFeatureAnnotation(feat1)
	asrt.NoError(err, "Failed to add feature 1")
	// Action: Call with a non-existent ID
	nonExistentPubID := "PMID:99999"
	_, err = repo.ListByPublicationId(nonExistentPubID, source)
	asrt.Error(err, "Expected an error for non-existent publication ID")
	asrt.True(
		repository.IsPublicationAnnotationNotFound(err),
		"Error should be PublicationAnnotationNotFoundError",
	)
}

func TestListByPublicationId_NotFoundIncorrectSource(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	pubID := "PMID:22222"
	incorrectSource := "doi"
	feat1 := getFullFeatureDoc()
	feat1.Id = "DDB_G0000031"
	feat1.Attributes.Pubmed = []string{"pubID"}
	_, err := repo.AddFeatureAnnotation(feat1)
	asrt.NoError(err, "Failed to add feature 1")
	// Action: Call with the correct ID but incorrect source ("doi")
	_, err = repo.ListByPublicationId(pubID, incorrectSource)
	asrt.Error(err, "Expected an error for incorrect source")
	asrt.True(
		repository.IsPublicationAnnotationNotFound(err),
		"Error should be PublicationAnnotationNotFoundError",
	)
}

func TestListByPublicationId_NotFoundObsolete(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	// Setup: Create an obsolete feature linked to a publication
	pubID := "PMID:44444"
	source := "pubmed"

	feat1 := getFullFeatureDoc()
	feat1.Id = "DDB_G0000041"
	feat1.Attributes.Pubmed = []string{pubID}
	feat1.IsObsolete = true
	_, err := repo.AddFeatureAnnotation(feat1)
	asrt.NoError(err, "Failed to add feature 1")

	// Action: Call ListByPublicationId for the publication
	_, err = repo.ListByPublicationId(pubID, source)
	asrt.Error(
		err,
		"Expected an error as only obsolete annotations are linked",
	)
	asrt.True(
		repository.IsPublicationAnnotationNotFound(err),
		"Error should be PublicationAnnotationNotFoundError",
	)
}
