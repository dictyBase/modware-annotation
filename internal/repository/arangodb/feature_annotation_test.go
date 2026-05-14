package arangodb

import (
	"slices"
	"testing"
	"time"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
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

	got, err := repo.GetFeatureAnnotation(added.AnnoID)
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
		added.AnnoID,
		gotByName.AnnoID,
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
				identifier = doc.AnnoID
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
		Id:        added.AnnoID,
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
		update.Attributes.Synonyms, //nolint:staticcheck // Test uses deprecated field for backward compatibility
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

func TestReplacePropertiesInExistingFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	added, err := repo.AddFeatureAnnotation(getCompleteFeatureDoc())
	asrt.NoError(err, "expected no error adding initial feature annotation")

	update := &feature.FeatureAnnotationUpdate{
		Id:        added.AnnoID,
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
	asrt.Len(doc.Properties, 1)

	// Properties should be replaced, not appended
	//nolint:staticcheck // Test uses deprecated field for backward compatibility
	expectedProperties := collection.Map(update.Attributes.Properties, convertProperty)
	slices.SortFunc(expectedProperties, sortTagProperties)
	slices.SortFunc(doc.Properties, sortTagProperties)
	asrt.ElementsMatch(
		expectedProperties,
		doc.Properties,
		"should have replaced properties",
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
		Id:        added.AnnoID,
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
		Id:        added.AnnoID,
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
		Id:        added.AnnoID,
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
		Id:        added.AnnoID,
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
		Id: added.AnnoID,
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

	// Add tag
	updated, err := repo.AddTag(&feature.AddTagRequest{
		Id: added.AnnoID,
		Tag: &feature.TagPropertyCreate{
			Tag:       "test_tag_default",
			Value:     "test_value",
			CreatedBy: "tester@example.org",
		},
	})
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
			return p.Tag == "test_tag_default"
		},
	)
	asrt.True(otk, "should find the newly added tag")
	asrt.Equal("test_value", found.Value, "should match tag value")
	asrt.Equal(
		"tester@example.org",
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

	specTS := time.
		Now().
		Add(-48 * time.Hour).
		UTC().
		Truncate(time.Microsecond)

	// Add tag
	updated, err := repo.AddTag(&feature.AddTagRequest{
		Id: added.AnnoID,
		Tag: &feature.TagPropertyCreate{
			Tag:       "test_tag_provided",
			Value:     "test_value_provided",
			CreatedBy: "tester@example.org",
			CreatedAt: timestamppb.New(specTS),
		},
	})
	asrt.NoError(err, "should successfully add tag with provided timestamp")
	asrt.Len(
		updated.Properties,
		len(added.Properties)+1,
		"should have one more tag",
	)

	// Verify added tag
	found, otk := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "test_tag_provided"
		},
	)
	asrt.True(otk, "should find the newly added tag")
	asrt.Equal("test_value_provided", found.Value, "should match tag value")
	asrt.Equal(
		specTS,
		found.CreatedAt,
		"CreatedAt should match provided timestamp",
	)
	asrt.Equal(
		specTS,
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
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be not found error",
	)
}

func TestUpdateTag_SuccessDefaultTimestamp(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Seed a feature with tags
	feat := seedAnnotationWithTags(t, repo)
	createdTag, otk := collection.Find(
		feat.Properties,
		func(p model.TagPropertyDoc) bool { return p.Tag == "foo" },
	)
	asrt.True(otk, "tag 'foo' should be found in the seeded properties")

	//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
	updReq := &feature.UpdateTagRequest{
		Id: feat.AnnoID,
		//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
		Tag: &feature.TagPropertyUpdate{
			Tag:       "foo",
			Value:     "new-bar",
			UpdatedBy: "update@test.com",
		},
	}
	updatedFeat, err := repo.UpdateTag(updReq)
	asrt.NoError(err)
	asrt.NotNil(updatedFeat)
	asrt.Len(updatedFeat.Properties, 2)

	updatedTag, otk := collection.Find(
		updatedFeat.Properties,
		func(p model.TagPropertyDoc) bool { return p.Tag == "foo" },
	)
	asrt.True(otk, "could not find tag foo in updated feature")

	asrt.Equal("new-bar", updatedTag.Value)
	asrt.Equal("update@test.com", updatedTag.UpdatedBy)
	asrt.Equal(createdTag.CreatedBy, updatedTag.CreatedBy)
	asrt.Equal(createdTag.CreatedAt, updatedTag.CreatedAt)
	asrt.WithinDuration(time.Now(), updatedTag.UpdatedAt, 12*time.Second)
	asrt.NotEqual(createdTag.UpdatedAt, updatedTag.UpdatedAt)
}

func TestUpdateTag_SuccessExplicitTimestamp(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	feat := seedAnnotationWithTags(t, repo)
	customTime := time.Now().Add(-24 * time.Hour).UTC()
	//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
	updReq := &feature.UpdateTagRequest{
		Id: feat.AnnoID,
		//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
		Tag: &feature.TagPropertyUpdate{
			Tag:       "baz",
			Value:     "new-quax",
			UpdatedBy: "update2@test.com",
			UpdatedAt: timestamppb.New(customTime),
		},
	}
	updatedFeat, err := repo.UpdateTag(updReq)
	asrt.NoError(err)
	asrt.NotNil(updatedFeat)

	updatedTag, ok := collection.Find(
		updatedFeat.Properties,
		func(p model.TagPropertyDoc) bool { return p.Tag == "baz" },
	)
	asrt.True(ok, "could not find tag baz in updated feature")

	asrt.Equal(
		customTime.Round(time.Microsecond),
		updatedTag.UpdatedAt.Round(time.Microsecond),
	)
}

func TestUpdateTag_FailNonExistentTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	feat := seedAnnotationWithTags(t, repo)
	//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
	updReq := &feature.UpdateTagRequest{
		Id: feat.AnnoID,
		//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
		Tag: &feature.TagPropertyUpdate{
			Tag: "non-existent-tag",
		},
	}
	_, err := repo.UpdateTag(updReq)
	asrt.Error(err)
	asrt.ErrorContains(err, "tag non-existent-tag not found")
}

func TestUpdateTag_FailNonExistentFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
	updReq := &feature.UpdateTagRequest{
		Id: "non-existent-id",
		//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
		Tag: &feature.TagPropertyUpdate{
			Tag: "foo",
		},
	}
	_, err := repo.UpdateTag(updReq)
	asrt.Error(err)
	var nfErr *repository.AnnoNotFoundError
	asrt.ErrorAs(err, &nfErr)
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
	tagged, err := repo.AddTag(&feature.AddTagRequest{
		Id: added.AnnoID,
		Tag: &feature.TagPropertyCreate{
			Tag:       "remove_me",
			Value:     "temp_value",
			CreatedBy: "tester@example.org",
		},
	})
	asrt.NoError(err, "should add test tag")
	asrt.Len(
		tagged.Properties,
		len(feat.Attributes.Properties)+1,
		"should have initial tag",
	)

	// Remove tag
	//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
	err = repo.RemoveTag(&feature.RemoveTagRequest{
		Id:  added.AnnoID,
		Tag: "remove_me",
	})
	asrt.NoError(err, "should successfully remove tag")

	// Verify removal by fetching updated document
	updated, err := repo.GetFeatureAnnotation(added.AnnoID)
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
	//nolint:staticcheck // SA1019: Test for deprecated functionality during transition period
	err = repo.RemoveTag(&feature.RemoveTagRequest{
		Id:  added.AnnoID,
		Tag: "ghost_tag",
	})
	asrt.Error(err, "should return error for missing tag")
}

func TestListByPublicationID_SuccessPubmed(t *testing.T) {
	t.Parallel()
	testListByPublicationIDSuccess(&testListByPublicationIDSuccessParams{
		t:                    t,
		pubID:                "PMID:12345",
		source:               pubmedSource, // Use constant
		featureIDFieldPrefix: "DDB_G000000",
		setPubFunc:           func(attrs *feature.FeatureAnnotationAttributes, ids []string) { attrs.Pubmed = ids },
		unrelatedPubID:       "PMID:67890",
		errorMsgSuffix:       "Pubmed ID",
	})
}

func TestListByPublicationID_SuccessDOI(t *testing.T) {
	t.Parallel()
	testListByPublicationIDSuccess(&testListByPublicationIDSuccessParams{
		t:                    t,
		pubID:                "doi:10.1234/journal.1",
		source:               doiSource, // Use constant
		featureIDFieldPrefix: "DDB_G000001",
		setPubFunc:           func(attrs *feature.FeatureAnnotationAttributes, ids []string) { attrs.Publications = ids },
		unrelatedPubID:       "doi:10.5678/journal.2",
		errorMsgSuffix:       "DOI",
	})
}

func TestListByPublicationID_NotFoundIncorrectID(t *testing.T) {
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
	_, err = repo.ListByPublicationID(nonExistentPubID, source)
	asrt.Error(err, "Expected an error for non-existent publication ID")
	asrt.True(
		repository.IsPublicationAnnotationNotFound(err),
		"Error should be PublicationAnnotationNotFoundError",
	)
}

func TestListByPublicationID_NotFoundIncorrectSource(t *testing.T) {
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
	_, err = repo.ListByPublicationID(pubID, incorrectSource)
	asrt.Error(err, "Expected an error for incorrect source")
	asrt.True(
		repository.IsPublicationAnnotationNotFound(err),
		"Error should be PublicationAnnotationNotFoundError",
	)
}

func TestListByPublicationID_NotFoundObsolete(t *testing.T) {
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
	_, err = repo.ListByPublicationID(pubID, source)
	asrt.Error(
		err,
		"Expected an error as only obsolete annotations are linked",
	)
	asrt.True(
		repository.IsPublicationAnnotationNotFound(err),
		"Error should be PublicationAnnotationNotFoundError",
	)
}

func TestAddTags_Success(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	originalTagCount := len(added.Properties)

	// Create multiple tags request
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "category",
			value:     "enzyme",
			createdBy: "tester1@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "organism",
			value:     "dictyostelium",
			createdBy: "tester2@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "priority",
			value:     "high",
			createdBy: "tester1@example.org",
		}),
	}

	// Add tags
	updated, err := repo.AddTags(createAddTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully add multiple tags")
	asrt.Len(
		updated.Properties,
		originalTagCount+len(newTags),
		"should have original tags plus new tags",
	)

	// Verify each added tag
	for _, expectedTag := range newTags {
		found, otk := collection.Find(
			updated.Properties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == expectedTag.Tag && p.Value == expectedTag.Value
			},
		)
		asrt.True(otk, "should find tag %s", expectedTag.Tag)
		asrt.Equal(
			expectedTag.CreatedBy,
			found.CreatedBy,
			"should match created by for tag %s",
			expectedTag.Tag,
		)
		asrt.WithinDuration(
			time.Now(),
			found.CreatedAt,
			2*time.Second,
			"CreatedAt should be recent for tag %s",
			expectedTag.Tag,
		)
	}
}

//nolint:dupl // Intentional duplication with TestSetTags_DefaultTimestamps - both need to test same timestamp behavior
func TestAddTags_DefaultTimestamps(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tags without timestamps
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "auto_timestamp1",
			value:     "value1",
			createdBy: "tester@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "auto_timestamp2",
			value:     "value2",
			createdBy: "tester@example.org",
		}),
	}

	// Add tags
	updated, err := repo.AddTags(
		createAddTagsRequest(
			added.AnnoID,
			newTags,
		),
	)
	asrt.NoError(err, "should successfully add tags with default timestamps")

	// Verify timestamps are auto-generated and recent
	for _, expectedTag := range newTags {
		found, otk := collection.Find(
			updated.Properties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		asrt.True(otk, "should find tag %s", expectedTag.Tag)
		asrt.WithinDuration(
			time.Now(),
			found.CreatedAt,
			2*time.Second,
			"CreatedAt should be recent for tag %s",
			expectedTag.Tag,
		)
		asrt.Equal(
			found.CreatedAt,
			found.UpdatedAt,
			"UpdatedAt should match CreatedAt for new tag %s",
			expectedTag.Tag,
		)
	}
}

func TestAddTags_ProvidedTimestamps(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tags with specific timestamps
	specTS1 := time.Now().
		Add(-48 * time.Hour).
		UTC().Truncate(time.Microsecond)
	specTS2 := time.Now().
		Add(-24 * time.Hour).
		UTC().Truncate(time.Microsecond)
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "provided_timestamp1",
			value:     "value1",
			createdBy: "tester@example.org",
			timestamp: &specTS1,
		}),
		createTestTag(createTestTagParams{
			tag:       "provided_timestamp2",
			value:     "value2",
			createdBy: "tester@example.org",
			timestamp: &specTS2,
		}),
	}

	updated, err := repo.AddTags(createAddTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(
		err,
		"should successfully add tags with provided timestamps",
	)

	// Verify provided timestamps are preserved
	expectedTimestamps := []time.Time{specTS1, specTS2}
	for idx, expectedTag := range newTags {
		found, otk := collection.Find(
			updated.Properties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		asrt.True(otk, "should find tag %s", expectedTag.Tag)
		asrt.Equal(
			expectedTimestamps[idx],
			found.CreatedAt,
			"CreatedAt should match provided timestamp for tag %s",
			expectedTag.Tag,
		)
		asrt.Equal(
			expectedTimestamps[idx],
			found.UpdatedAt,
			"UpdatedAt should match provided timestamp for tag %s",
			expectedTag.Tag,
		)
	}
}

func TestAddTags_SingleTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	originalTagCount := len(added.Properties)

	// Add tag
	updated, err := repo.AddTags(
		createAddTagsRequest(
			added.AnnoID,
			[]*feature.TagPropertyCreate{
				createTestTag(createTestTagParams{
					tag:       "single_tag",
					value:     "single_value",
					createdBy: "tester@example.org",
				}),
			},
		),
	)
	asrt.NoError(
		err,
		"should successfully add single tag via AddTags",
	)
	asrt.Len(
		updated.Properties,
		originalTagCount+1,
		"should have one additional tag",
	)

	// Verify single tag was added
	found, otk := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "single_tag"
		},
	)
	asrt.True(otk, "should find the single tag")
	asrt.Equal("single_value", found.Value, "should match tag value")
	asrt.Equal(
		"tester@example.org",
		found.CreatedBy,
		"should match created by",
	)
}

func TestAddTags_AppendToExisting(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	originalTags := make([]model.TagPropertyDoc, len(added.Properties))
	copy(originalTags, added.Properties)

	// Create new tags with different names to ensure no conflicts
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "new_category",
			value:     "new_value",
			createdBy: "new_tester@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "additional_info",
			value:     "extra_data",
			createdBy: "new_tester@example.org",
		}),
	}

	updated, err := repo.AddTags(createAddTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(
		err,
		"should successfully append tags to existing properties",
	)
	asrt.Len(
		updated.Properties,
		len(originalTags)+len(newTags),
		"should have original tags plus new tags",
	)

	// Verify original tags are preserved
	verifyOriginalTagsPreserved(verifyOriginalTagsPreservedParams{
		t:             t,
		asrt:          asrt,
		allProperties: updated.Properties,
		originalTags:  originalTags,
	})

	// Verify new tags were added
	verifyNewTagsAdded(verifyNewTagsAddedParams{
		t:             t,
		asrt:          asrt,
		allProperties: updated.Properties,
		newTags:       newTags,
	})
}

func TestAddTags_NonExistentFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Attempt to add tags to non-existent feature
	_, err := repo.AddTags(createAddTagsRequest("DDB_G0000000",
		[]*feature.TagPropertyCreate{
			createTestTag(createTestTagParams{
				tag:       "test_tag",
				value:     "test_value",
				createdBy: "tester@example.org",
			}),
		}))
	asrt.Error(err, "should return error for non-existent feature")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func TestAddTags_EmptyRequest(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	originalTagCount := len(added.Properties)

	// Add empty tags (should succeed but not change anything)
	updated, err := repo.AddTags(
		createAddTagsRequest(
			added.AnnoID,
			[]*feature.TagPropertyCreate{},
		),
	)
	asrt.NoError(err, "should handle empty tags request gracefully")
	asrt.Len(
		updated.Properties,
		originalTagCount,
		"should not change property count with empty request",
	)

	// Verify no changes to existing properties
	slices.SortFunc(updated.Properties, sortTagProperties)
	originalProperties := make([]model.TagPropertyDoc, len(added.Properties))
	copy(originalProperties, added.Properties)
	slices.SortFunc(originalProperties, sortTagProperties)

	asrt.ElementsMatch(
		originalProperties,
		updated.Properties,
		"properties should remain unchanged",
	)
}

//nolint:dupl // Intentional duplication with TestSetTags_VerifyTagProperties - both need to test same property validation
func TestAddTags_VerifyTagProperties(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	// Create tags with all properties specified
	specTS := time.Now().Add(-12 * time.Hour).UTC().
		Truncate(time.Microsecond)
	newTags := []*feature.TagPropertyCreate{
		{
			Tag:       "comprehensive_tag",
			Value:     "comprehensive_value",
			CreatedBy: "comprehensive_tester@example.org",
			CreatedAt: timestamppb.New(specTS),
		},
	}
	updated, err := repo.AddTags(createAddTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully add comprehensive tag")
	// Verify all tag properties are correctly stored
	found, otk := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "comprehensive_tag"
		},
	)
	asrt.True(otk, "should find comprehensive tag")
	asrt.Equal(
		"comprehensive_tag",
		found.Tag,
		"should store tag name correctly",
	)
	asrt.Equal(
		"comprehensive_value",
		found.Value,
		"should store tag value correctly",
	)
	asrt.Equal(
		"comprehensive_tester@example.org",
		found.CreatedBy,
		"should store created by correctly",
	)
	asrt.Equal(
		specTS,
		found.CreatedAt,
		"should store created at correctly",
	)
	asrt.Equal(
		"comprehensive_tester@example.org",
		found.UpdatedBy,
		"should set updated by to created by",
	)
	asrt.Equal(
		specTS,
		found.UpdatedAt,
		"should set updated at to created at",
	)
}

func TestAddTags_VerifyTimestamps(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	specTS := time.
		Now().
		Add(-6 * time.Hour).
		UTC().Truncate(time.Microsecond)

	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "default_ts_tag",
			value:     "default_value",
			createdBy: "tester@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "provided_ts_tag",
			value:     "provided_value",
			createdBy: "tester@example.org",
			timestamp: &specTS,
		}),
	}
	updated, err := repo.AddTags(createAddTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(
		err,
		"should successfully add tags with mixed timestamps",
	)
	// Verify default timestamp tag
	defaultTag, found := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "default_ts_tag"
		},
	)
	asrt.True(found, "should find default timestamp tag")
	asrt.WithinDuration(
		time.Now(),
		defaultTag.CreatedAt,
		2*time.Second,
		"default timestamp should be recent",
	)

	providedTag, found := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "provided_ts_tag"
		},
	)
	asrt.True(found, "should find provided timestamp tag")
	asrt.Equal(
		specTS,
		providedTag.CreatedAt,
		"provided timestamp should match exactly",
	)
}

func TestSetTags_Success(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature with existing tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.NotEmpty(added.Properties, "feature should have initial properties")
	originalTags := make([]model.TagPropertyDoc, len(added.Properties))
	copy(originalTags, added.Properties)

	// Create new tags to replace existing ones
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "category",
			value:     "enzyme",
			createdBy: "setter@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "priority",
			value:     "high",
			createdBy: "setter@example.org",
		}),
	}

	// Replace tags
	updated, err := repo.SetTags(createSetTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully set tags")

	// Verify complete replacement
	verifyTagsCompletelyReplaced(verifyTagsCompletelyReplacedParams{
		t:            t,
		asrt:         asrt,
		result:       updated.Properties,
		originalTags: originalTags,
		newTags:      newTags,
	})
}

func TestSetTags_ReplaceExistingTags(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with multiple existing tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Add more tags first
	moreTagsAdded, err := repo.AddTags(createAddTagsRequest(
		added.AnnoID,
		[]*feature.TagPropertyCreate{
			createTestTag(createTestTagParams{
				tag:       "extra1",
				value:     "value1",
				createdBy: "extra@example.org",
			}),
			createTestTag(createTestTagParams{
				tag:       "extra2",
				value:     "value2",
				createdBy: "extra@example.org",
			}),
		},
	))
	asrt.NoError(err, "should add extra tags")
	asrt.Greater(len(moreTagsAdded.Properties), 2, "should have multiple tags")
	originalTags := make([]model.TagPropertyDoc, len(moreTagsAdded.Properties))
	copy(originalTags, moreTagsAdded.Properties)

	// Create completely different new tags
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "organism",
			value:     "dictyostelium",
			createdBy: "replacer@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "method",
			value:     "experimental",
			createdBy: "replacer@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "confidence",
			value:     "high",
			createdBy: "replacer@example.org",
		}),
	}

	// Replace all tags
	updated, err := repo.SetTags(createSetTagsRequest(
		moreTagsAdded.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully replace all tags")

	// Verify complete replacement
	verifyTagsCompletelyReplaced(verifyTagsCompletelyReplacedParams{
		t:            t,
		asrt:         asrt,
		result:       updated.Properties,
		originalTags: originalTags,
		newTags:      newTags,
	})
}

func TestSetTags_EmptyTagSet(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with existing tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.NotEmpty(added.Properties, "feature should have initial properties")

	// Replace with empty tag set
	updated, err := repo.SetTags(createSetTagsRequest(
		added.AnnoID,
		[]*feature.TagPropertyCreate{},
	))
	asrt.NoError(err, "should successfully set empty tags")
	asrt.Empty(
		updated.Properties,
		"should have no tags after setting empty array",
	)
}

//nolint:dupl // Intentional duplication with TestAddTags_DefaultTimestamps - both need to test same timestamp behavior
func TestSetTags_DefaultTimestamps(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create base feature
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tags without timestamps
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "auto_timestamp1",
			value:     "value1",
			createdBy: "setter@example.org",
		}),
		createTestTag(createTestTagParams{
			tag:       "auto_timestamp2",
			value:     "value2",
			createdBy: "setter@example.org",
		}),
	}

	// Set tags
	updated, err := repo.SetTags(createSetTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully set tags with default timestamps")

	// Verify timestamps are auto-generated and recent
	for _, expectedTag := range newTags {
		found, otk := collection.Find(
			updated.Properties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		asrt.True(otk, "should find tag %s", expectedTag.Tag)
		asrt.WithinDuration(
			time.Now(),
			found.CreatedAt,
			2*time.Second,
			"CreatedAt should be recent for tag %s",
			expectedTag.Tag,
		)
		asrt.Equal(
			found.CreatedAt,
			found.UpdatedAt,
			"UpdatedAt should match CreatedAt for new tag %s",
			expectedTag.Tag,
		)
	}
}

func TestSetTags_ProvidedTimestamps(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tags with specific timestamps
	specTS1 := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Microsecond)
	specTS2 := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Microsecond)
	newTags := []*feature.TagPropertyCreate{
		createTestTag(createTestTagParams{
			tag:       "provided_timestamp1",
			value:     "value1",
			createdBy: "setter@example.org",
			timestamp: &specTS1,
		}),
		createTestTag(createTestTagParams{
			tag:       "provided_timestamp2",
			value:     "value2",
			createdBy: "setter@example.org",
			timestamp: &specTS2,
		}),
	}

	updated, err := repo.SetTags(createSetTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully set tags with provided timestamps")

	// Verify provided timestamps are preserved
	expectedTimestamps := []time.Time{specTS1, specTS2}
	for idx, expectedTag := range newTags {
		found, otk := collection.Find(
			updated.Properties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == expectedTag.Tag
			},
		)
		asrt.True(otk, "should find tag %s", expectedTag.Tag)
		asrt.Equal(
			expectedTimestamps[idx],
			found.CreatedAt,
			"CreatedAt should match provided timestamp for tag %s",
			expectedTag.Tag,
		)
		asrt.Equal(
			expectedTimestamps[idx],
			found.UpdatedAt,
			"UpdatedAt should match provided timestamp for tag %s",
			expectedTag.Tag,
		)
	}
}

func TestSetTags_NonExistentFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Attempt to set tags on non-existent feature
	_, err := repo.SetTags(createSetTagsRequest("DDB_G0000000",
		[]*feature.TagPropertyCreate{
			createTestTag(createTestTagParams{
				tag:       "test_tag",
				value:     "test_value",
				createdBy: "setter@example.org",
			}),
		}))
	asrt.Error(err, "should return error for non-existent feature")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

//nolint:dupl // Intentional duplication with TestAddTags_VerifyTagProperties - both need to test same property validation
func TestSetTags_VerifyTagProperties(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")

	// Create tags with all properties specified
	specTS := time.Now().Add(-12 * time.Hour).UTC().Truncate(time.Microsecond)
	newTags := []*feature.TagPropertyCreate{
		{
			Tag:       "comprehensive_tag",
			Value:     "comprehensive_value",
			CreatedBy: "comprehensive_setter@example.org",
			CreatedAt: timestamppb.New(specTS),
		},
	}

	updated, err := repo.SetTags(createSetTagsRequest(
		added.AnnoID,
		newTags,
	))
	asrt.NoError(err, "should successfully set comprehensive tag")

	// Verify all tag properties are correctly stored
	found, otk := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "comprehensive_tag"
		},
	)
	asrt.True(otk, "should find comprehensive tag")
	asrt.Equal(
		"comprehensive_tag",
		found.Tag,
		"should store tag name correctly",
	)
	asrt.Equal(
		"comprehensive_value",
		found.Value,
		"should store tag value correctly",
	)
	asrt.Equal(
		"comprehensive_setter@example.org",
		found.CreatedBy,
		"should store created by correctly",
	)
	asrt.Equal(specTS, found.CreatedAt, "should store created at correctly")
	asrt.Equal(
		"comprehensive_setter@example.org",
		found.UpdatedBy,
		"should set updated by to created by",
	)
	asrt.Equal(specTS, found.UpdatedAt, "should set updated at to created at")
}

func TestSetTags_SingleTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with multiple existing tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.NotEmpty(added.Properties, "feature should have initial properties")

	// Replace with single tag
	updated, err := repo.SetTags(createSetTagsRequest(
		added.AnnoID,
		[]*feature.TagPropertyCreate{
			createTestTag(createTestTagParams{
				tag:       "single_tag",
				value:     "single_value",
				createdBy: "setter@example.org",
			}),
		},
	))
	asrt.NoError(err, "should successfully set single tag")
	asrt.Len(updated.Properties, 1, "should have exactly one tag")

	// Verify single tag was set correctly
	found, otk := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "single_tag"
		},
	)
	asrt.True(otk, "should find the single tag")
	asrt.Equal("single_value", found.Value, "should match tag value")
	asrt.Equal("setter@example.org", found.CreatedBy, "should match created by")
}

func TestRemoveTags_Success(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with multiple tags
	feat := getBaseFeatureDoc()
	feat.Attributes.Properties = []*feature.TagProperty{
		{
			Tag:       "category",
			Value:     "enzyme",
			CreatedBy: "creator@example.org",
		},
		{
			Tag:       "priority",
			Value:     "high",
			CreatedBy: "creator@example.org",
		},
		{
			Tag:       "status",
			Value:     "active",
			CreatedBy: "creator@example.org",
		},
	}
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	originalTagCount := len(added.Properties)
	asrt.Equal(
		3,
		originalTagCount,
		"feature should have three tags for this test",
	)

	// Find a tag to remove
	tagToRemove := added.Properties[0]

	// Remove the tag
	updated, err := repo.RemoveTags(createRemoveTagsRequest(
		added.AnnoID,
		tagToRemove.Tag,
		tagToRemove.Value,
	))
	asrt.NoError(err, "should successfully remove tag")

	// Verify tag was removed
	verifyTagRemoved(verifyTagRemovedParams{
		t:          t,
		asrt:       asrt,
		properties: updated.Properties,
		tag:        tagToRemove.Tag,
		value:      tagToRemove.Value,
	})

	// Verify other tags were preserved
	verifyOtherTagsPreserved(verifyOtherTagsPreservedParams{
		t:            t,
		asrt:         asrt,
		original:     added.Properties,
		updated:      updated.Properties,
		removedTag:   tagToRemove.Tag,
		removedValue: tagToRemove.Value,
	})
}

func TestRemoveTags_RemoveLastTag(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with a single tag
	feat := getBaseFeatureDoc()
	feat.Attributes.Properties = []*feature.TagProperty{
		{
			Tag:       "only_tag",
			Value:     "only_value",
			CreatedBy: "creator@example.org",
		},
	}
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.Len(added.Properties, 1, "feature should have exactly one tag")

	// Remove the only tag
	updated, err := repo.RemoveTags(createRemoveTagsRequest(
		added.AnnoID,
		"only_tag",
		"only_value",
	))
	asrt.NoError(err, "should successfully remove last tag")
	asrt.Empty(updated.Properties, "feature should have no tags after removal")
}

func TestRemoveTags_RemoveMultipleMatches(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with duplicate tag/value pairs
	feat := getBaseFeatureDoc()
	feat.Attributes.Properties = []*feature.TagProperty{
		{
			Tag:       "duplicate",
			Value:     "value",
			CreatedBy: "creator1@example.org",
		},
		{
			Tag:       "duplicate",
			Value:     "value",
			CreatedBy: "creator2@example.org",
		},
		{
			Tag:       "other",
			Value:     "different",
			CreatedBy: "creator3@example.org",
		},
	}
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.Len(added.Properties, 3, "feature should have three tags")

	// Remove all instances of duplicate tag/value
	updated, err := repo.RemoveTags(createRemoveTagsRequest(
		added.AnnoID,
		"duplicate",
		"value",
	))
	asrt.NoError(err, "should successfully remove duplicate tags")

	// Verify all duplicate tags were removed
	verifyTagRemoved(verifyTagRemovedParams{
		t:          t,
		asrt:       asrt,
		properties: updated.Properties,
		tag:        "duplicate",
		value:      "value",
	})

	// Verify the other tag remains
	asrt.Len(updated.Properties, 1, "should have one tag remaining")
	_, found := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "other" && p.Value == "different"
		},
	)
	asrt.True(found, "other tag should be preserved")
}

func TestRemoveTags_NonExistentFeature(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Attempt to remove tags from non-existent feature
	_, err := repo.RemoveTags(createRemoveTagsRequest(
		"DDB_G0000000",
		"any_tag",
		"any_value",
	))
	asrt.Error(err, "should return error for non-existent feature")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func TestRemoveTags_TagNotFound(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with some tags
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	originalProperties := slices.Clone(added.Properties)

	// Try to remove a tag that doesn't exist
	updated, err := repo.RemoveTags(createRemoveTagsRequest(
		added.AnnoID,
		"nonexistent_tag",
		"nonexistent_value",
	))
	asrt.NoError(err, "should succeed even when tag doesn't exist")

	// Verify all original tags are still present
	asrt.Len(
		updated.Properties,
		len(originalProperties),
		"should have same number of tags",
	)
	for _, originalTag := range originalProperties {
		_, found := collection.Find(
			updated.Properties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == originalTag.Tag && p.Value == originalTag.Value
			},
		)
		asrt.True(
			found,
			"original tag '%s' should be preserved",
			originalTag.Tag,
		)
	}
}

func TestRemoveTags_PartialMatch(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with specific tags
	feat := getBaseFeatureDoc()
	feat.Attributes.Properties = []*feature.TagProperty{
		{
			Tag:       "category",
			Value:     "enzyme",
			CreatedBy: "creator@example.org",
		},
		{
			Tag:       "category",
			Value:     "protein",
			CreatedBy: "creator@example.org",
		},
	}
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.Len(added.Properties, 2, "feature should have two tags")

	// Try to remove tag with correct name but wrong value
	updated, err := repo.RemoveTags(createRemoveTagsRequest(
		added.AnnoID,
		"category",
		"wrong_value",
	))
	asrt.NoError(err, "should succeed even with partial match")

	// Verify all original tags are still present (no match found)
	asrt.Len(updated.Properties, 2, "should still have both tags")

	// Verify specific tags are preserved
	_, foundEnzyme := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "category" && p.Value == "enzyme"
		},
	)
	asrt.True(foundEnzyme, "enzyme tag should be preserved")

	_, foundProtein := collection.Find(
		updated.Properties,
		func(p model.TagPropertyDoc) bool {
			return p.Tag == "category" && p.Value == "protein"
		},
	)
	asrt.True(foundProtein, "protein tag should be preserved")
}

func TestRemoveTags_EmptyProperties(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(cleanupDB(repo))

	// Create feature with no properties
	feat := getBaseFeatureDoc()
	feat.Attributes.Properties = []*feature.TagProperty{}
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "should successfully add test feature")
	asrt.Empty(added.Properties, "feature should have no tags")

	// Try to remove a tag from empty properties
	updated, err := repo.RemoveTags(createRemoveTagsRequest(
		added.AnnoID,
		"any_tag",
		"any_value",
	))
	asrt.NoError(err, "should succeed with empty properties")
	asrt.Empty(updated.Properties, "should still have no tags")
}
