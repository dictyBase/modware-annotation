package arangodb

import (
	"slices"
	"strings"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/pipe"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type validateDBLinksParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        []model.DBLinkDoc
	expected   []*feature.DbLink
}

type validatePropertiesParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        []model.TagPropertyDoc
	expected   []*feature.TagProperty
}

type removeFeatureTestCase struct {
	name    string
	purge   bool
	wantErr bool
}

type validateFeatureAnnotationParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        *model.FeatureAnnotationDoc
	base       *feature.NewFeatureAnnotation
}

type validateCompleteFeatureParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        *model.FeatureAnnotationDoc
	expected   *feature.NewFeatureAnnotation // Contains base info + attributes
}

// testListByPublicationIDSuccessParams defines the parameters for the
// testListByPublicationIDSuccess helper function.
type testListByPublicationIDSuccessParams struct {
	t                    *testing.T
	pubID                string
	source               string
	featureIDFieldPrefix string
	setPubFunc           func(*feature.FeatureAnnotationAttributes, []string)
	unrelatedPubID       string
	errorMsgSuffix       string
}

type assertListByPublicationResultsParams struct {
	t       *testing.T
	asrt    *require.Assertions
	results []*model.FeatureAnnotationDoc
	added1  *model.FeatureAnnotationDoc
	added2  *model.FeatureAnnotationDoc
}

type verifyOriginalTagsPreservedParams struct {
	t             *testing.T
	asrt          *require.Assertions
	allProperties []model.TagPropertyDoc
	originalTags  []model.TagPropertyDoc
}

type verifyNewTagsAddedParams struct {
	t             *testing.T
	asrt          *require.Assertions
	allProperties []model.TagPropertyDoc
	newTags       []*feature.TagPropertyCreate
}

type createTestTagParams struct {
	tag       string
	value     string
	createdBy string
	timestamp *time.Time
}

type verifyNoOriginalTagsRemainParams struct {
	t            *testing.T
	asrt         *require.Assertions
	result       []model.TagPropertyDoc
	originalTags []model.TagPropertyDoc
}

type verifyTagsCompletelyReplacedParams struct {
	t            *testing.T
	asrt         *require.Assertions
	result       []model.TagPropertyDoc
	originalTags []model.TagPropertyDoc
	newTags      []*feature.TagPropertyCreate
}

type verifyTagRemovedParams struct {
	t          *testing.T
	asrt       *require.Assertions
	properties []model.TagPropertyDoc
	tag        string
	value      string
}

type verifyOtherTagsPreservedParams struct {
	t            *testing.T
	asrt         *require.Assertions
	original     []model.TagPropertyDoc
	updated      []model.TagPropertyDoc
	removedTag   string
	removedValue string
}

type featFn func() *feature.NewFeatureAnnotation

func getBaseFeatureDoc() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		Id:        "DDB_G0000001", // Add a default ID
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{ // Initialize Attributes
			Name: "base_feature", // Add a default Name
		},
	}
}

func getCombinedFeatureDoc(
	baseFn featFn,
	advFn featFn,
) *feature.NewFeatureAnnotation {
	baseDoc := baseFn()
	feat := advFn()
	baseDoc.Attributes = feat.Attributes
	baseDoc.Id = feat.Id

	return baseDoc
}

func getFullFeatureDoc() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:     "original name",
			Synonyms: []string{"syn1", "syn2"},
		},
	}
}

func getCompleteFeatureDoc() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:         "gene name",
			Synonyms:     []string{"synonym1", "synonym2"},
			Publications: []string{"pub1", "pub2"},
			Pubmed:       []string{"123", "456"},
			Dblinks: []*feature.DbLink{
				{
					PrimaryId: "DDB_G0285425",
					Database:  "dictyBase",
					Version:   1,
					Linktype:  "gene",
					Url:       "http://dictybase.org/gene/DDB_G0285425",
					Label:     "gene page",
				},
			},
			Properties: []*feature.TagProperty{
				{
					Tag:       "description",
					Value:     "test gene",
					CreatedBy: "creator3@email.com",
					UpdatedBy: "updater@email.com",
					CreatedAt: timestamppb.New(time.Now()),
					UpdatedAt: timestamppb.New(time.Now()),
				},
			},
		},
	}
}

func getMultiPropertyTestCase() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		Id: "DDB_G0285426",
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:   "sgene",
			Pubmed: []string{"123456", "456234"},
			Properties: []*feature.TagProperty{
				{
					Tag:       "description",
					Value:     "test description",
					CreatedBy: "creator1@email.com",
					CreatedAt: timestamppb.New(time.Now()),
					UpdatedAt: timestamppb.New(time.Now()),
				},
				{
					Tag:       "note",
					Value:     "test note",
					CreatedBy: "creator2@email.com",
					UpdatedBy: "updater@email.com",
					CreatedAt: timestamppb.New(time.Now()),
				},
				{
					Tag:       "status",
					Value:     "active",
					CreatedBy: "creator3@email.com",
					UpdatedBy: "updater@email.com",
					CreatedAt: timestamppb.New(time.Now()),
					UpdatedAt: timestamppb.New(time.Now()),
				},
			},
		},
	}
}

func getBasicTestCases() []*feature.NewFeatureAnnotation {
	return []*feature.NewFeatureAnnotation{
		{
			Attributes: &feature.FeatureAnnotationAttributes{
				Name: "required fields gene",
			},
			Id: "DDB_G0285428",
		},
		{
			Attributes: &feature.FeatureAnnotationAttributes{
				Name:       "no properties gene",
				Properties: []*feature.TagProperty{},
			},
			Id: "DDB_G0285429",
		},
	}
}

func setUpFeatureTest(
	t *testing.T,
) (*require.Assertions, repository.FeatureAnnotationRepository) {
	t.Helper()
	tra, err := testarango.NewTestArangoFromEnv(true)
	if err != nil {
		t.Fatalf("unable to construct new TestArango instance %s", err)
	}
	assert := require.New(t)
	repo, err := NewFeatureAnnoRepo(
		GetConnectParamsFromDB(tra),
		&FeatureCollectionParams{
			Feature: "feature_test",
			Pub:     "pub_test",
			Edge:    "feature_pub_test",
			Graph:   "feature_graph",
		},
	)
	assert.NoErrorf(
		err,
		"expect no error connecting to feature repository, received %s",
		err,
	)

	return assert, repo
}

func validateProperties(params validatePropertiesParams) {
	params.t.Helper()
	params.assertions.Equal(
		len(params.expected),
		len(params.got),
		"should have same number of properties",
	)
	for idx, prop := range params.expected {
		params.assertions.Equal(
			prop.Tag,
			params.got[idx].Tag,
			"should have matching tag",
		)
		params.assertions.Equal(
			prop.Value,
			params.got[idx].Value,
			"should have matching value",
		)
		params.assertions.Equal(
			prop.CreatedBy,
			params.got[idx].CreatedBy,
			"should have matching creator",
		)
		if prop.UpdatedBy != "" {
			params.assertions.Equal(
				prop.UpdatedBy,
				params.got[idx].UpdatedBy,
				"should have matching updater",
			)
		} else {
			params.assertions.Equal(
				params.got[idx].UpdatedBy,
				params.got[idx].CreatedBy,
				"should match creator and updater",
			)
		}
	}
}

// compareTagProperties implements sorting for TagPropertyDoc slices by tag and
// value using case-insensitive comparison.

func validateDBLinks(params validateDBLinksParams) {
	params.t.Helper()
	params.assertions.Equal(
		len(params.expected),
		len(params.got),
		"should have same number of dblinks",
	)
	for idx, link := range params.expected {
		params.assertions.Equal(
			link.PrimaryId,
			params.got[idx].PrimaryID,
			"should have matching primary ID",
		)
		params.assertions.Equal(
			link.Database,
			params.got[idx].Database,
			"should have matching database",
		)
		params.assertions.Equal(
			link.Version,
			params.got[idx].Version,
			"should have matching version",
		)
		params.assertions.Equal(
			link.Linktype,
			params.got[idx].LinkType,
			"should have matching link type",
		)
		params.assertions.Equal(
			link.Url,
			params.got[idx].URL,
			"should have matching URL",
		)
		params.assertions.Equal(
			link.Label,
			params.got[idx].Label,
			"should have matching label",
		)
	}
}

func validateBasicFields(params validateFeatureAnnotationParams) {
	params.t.Helper()
	params.assertions.Regexp(
		`^DDB_G\d+`,
		params.got.AnnoID,
		"should have matching IDs",
	)
	params.assertions.Equal(
		params.base.CreatedBy,
		params.got.CreatedBy,
		"should have matching creator",
	)
	params.assertions.Regexp(
		`^[a-zA-Z0-9\s-]*$`,
		params.got.Name,
		"should have matching name",
	)
	params.assertions.Equal(
		params.base.CreatedAt.AsTime(),
		params.got.CreatedAt,
		"should have matching created date",
	)
	params.assertions.Equal(
		params.got.CreatedAt,
		params.got.UpdatedAt,
		"should have matching created and updated at",
	)
}

// validateCompleteFeatureAnnotation checks all standard fields of a feature annotation document
// against the expected input data.
func validateCompleteFeatureAnnotation(params validateCompleteFeatureParams) {
	params.t.Helper()

	// Validate basic fields like ID, creator, timestamps, name
	validateBasicFields(validateFeatureAnnotationParams{
		t:          params.t,
		assertions: params.assertions,
		got:        params.got,
		base:       params.expected,
	})

	// Validate associated database links
	validateDBLinks(validateDBLinksParams{
		t:          params.t,
		assertions: params.assertions,
		got:        params.got.DBLinks,
		expected:   params.expected.Attributes.Dblinks,
	})

	// Validate associated properties (tags)
	validateProperties(validatePropertiesParams{
		t:          params.t,
		assertions: params.assertions,
		got:        params.got.Properties,
		expected:   params.expected.Attributes.Properties,
	})

	// Validate Pubmed IDs
	params.assertions.ElementsMatch(
		params.expected.Attributes.Pubmed,
		params.got.Pubmed,
		"should match pubmed ids",
	)

	// Validate Publications (DOIs, etc.)
	params.assertions.ElementsMatch(
		params.expected.Attributes.Publications,
		params.got.Publications,
		"should match publications",
	)
}

func sortTagProperties(a, b model.TagPropertyDoc) int {
	return strings.Compare(
		strings.ToLower(a.Tag),
		strings.ToLower(b.Tag),
	)
}

func getRemoveTestCases() []removeFeatureTestCase {
	return []removeFeatureTestCase{
		{
			name:    "should soft delete feature annotation",
			purge:   false,
			wantErr: false,
		},
		{
			name:    "should purge feature annotation",
			purge:   true,
			wantErr: false,
		},
		{
			name:    "should return error for non-existent ID",
			purge:   false,
			wantErr: true,
		},
	}
}

func assertListByPublicationResults(
	params assertListByPublicationResultsParams,
) {
	params.t.Helper()
	params.asrt.Len(
		params.results,
		2,
		"Should retrieve exactly 2 feature annotations",
	)
	retrievedIDs := collection.Map(
		params.results,
		func(doc *model.FeatureAnnotationDoc) string {
			return doc.AnnoID
		},
	)
	expectedIDs := []string{params.added1.AnnoID, params.added2.AnnoID}
	slices.Sort(retrievedIDs)
	slices.Sort(expectedIDs)
	params.asrt.Equal(
		expectedIDs,
		retrievedIDs,
		"Retrieved feature IDs should match the linked ones",
	)
}

func testListByPublicationIDSuccess(
	params *testListByPublicationIDSuccessParams,
) {
	params.t.Helper() // Mark as helper
	asrt, repo := setUpFeatureTest(params.t)
	params.t.Cleanup(cleanupDB(repo))

	// Create features linked to the target pubID
	feat1 := getFullFeatureDoc()
	feat1.Id = params.featureIDFieldPrefix + "1"
	params.setPubFunc(feat1.Attributes, []string{params.pubID})
	added1, err := repo.AddFeatureAnnotation(feat1)
	asrt.NoError(err, "Failed to add feature 1")

	feat2 := getFullFeatureDoc()
	feat2.Id = params.featureIDFieldPrefix + "2"
	params.setPubFunc(feat2.Attributes, []string{params.pubID})
	added2, err := repo.AddFeatureAnnotation(feat2)
	asrt.NoError(err, "Failed to add feature 2")

	// Create unrelated feature
	feat3 := getFullFeatureDoc()
	feat3.Id = params.featureIDFieldPrefix + "3"
	params.setPubFunc(
		feat3.Attributes,
		[]string{params.unrelatedPubID},
	) // Use a different pub ID
	_, err = repo.AddFeatureAnnotation(feat3)
	asrt.NoError(err, "Failed to add unrelated feature 3")

	// Action: Call ListByPublicationId
	results, err := repo.ListByPublicationID(params.pubID, params.source)
	asrt.NoError(err, "Expected no error retrieving by "+params.errorMsgSuffix)

	// Assertions (common logic extracted)
	assertListByPublicationResults(assertListByPublicationResultsParams{
		t:       params.t,
		asrt:    asrt,
		results: results,
		added1:  added1,
		added2:  added2,
	})
}

func cleanupDB(repo repository.FeatureAnnotationRepository) func() {
	return func() {
		_ = repo.Dbh().Drop()
	}
}

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
	})
	assert.NoError(err)
	model, err := repo.GetFeatureAnnotation(newFeat.AnnoID)
	assert.NoError(err)
	return model
}

func verifyOriginalTagsPreserved(params verifyOriginalTagsPreservedParams) {
	params.t.Helper()
	for _, originalTag := range params.originalTags {
		found, otk := collection.Find(
			params.allProperties,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == originalTag.Tag && p.Value == originalTag.Value
			},
		)
		params.asrt.True(
			otk,
			"should preserve original tag %s",
			originalTag.Tag,
		)
		params.asrt.Equal(
			originalTag.CreatedBy,
			found.CreatedBy,
			"should preserve original created by",
		)
		params.asrt.Equal(
			originalTag.CreatedAt,
			found.CreatedAt,
			"should preserve original created at",
		)
	}
}

// propertyDocToTag extracts the tag name from a TagPropertyDoc.
func propertyDocToTag(p model.TagPropertyDoc) string {
	return p.Tag
}

// tagCreateToTag extracts the tag name from a TagPropertyCreate.
func tagCreateToTag(p *feature.TagPropertyCreate) string {
	return p.Tag
}

// propertyDocToValue extracts the value from a TagPropertyDoc.
func propertyDocToValue(p model.TagPropertyDoc) string {
	return p.Value
}

// tagCreateToValue extracts the value from a TagPropertyCreate.
func tagCreateToValue(p *feature.TagPropertyCreate) string {
	return p.Value
}

// verifyNewTagsAdded checks if new tags were correctly added to the properties
// list.
func verifyNewTagsAdded(params verifyNewTagsAddedParams) {
	params.t.Helper()
	expectedTags := pipe.Pipe2(
		params.newTags,
		collection.CurriedMap(tagCreateToTag),
		collection.Sorted,
	)
	actualTags := pipe.Pipe2(
		params.allProperties,
		collection.CurriedMap(propertyDocToTag),
		collection.Sorted,
	)

	expectedValues := pipe.Pipe2(
		params.newTags,
		collection.CurriedMap(tagCreateToValue),
		collection.Sorted,
	)
	actualValues := pipe.Pipe2(
		params.allProperties,
		collection.CurriedMap(propertyDocToValue),
		collection.Sorted,
	)

	params.asrt.True(
		collection.AllExist(actualTags, expectedTags),
		"newly added tags should match expected",
	)
	params.asrt.True(
		collection.AllExist(
			actualValues,
			expectedValues,
		),
		"newly added values should match expected",
	)
}

// createAddTagsRequest creates an AddTagsRequest with the provided tags for testing purposes.
func createAddTagsRequest(
	featureID string,
	tags []*feature.TagPropertyCreate,
) *feature.AddTagsRequest {
	return &feature.AddTagsRequest{
		Id:   featureID,
		Tags: tags,
	}
}

// createSetTagsRequest creates a SetTagsRequest with the provided tags for testing purposes.
func createSetTagsRequest(
	featureID string,
	tags []*feature.TagPropertyCreate,
) *feature.SetTagsRequest {
	return &feature.SetTagsRequest{
		Id:   featureID,
		Tags: tags,
	}
}

// createTestTag creates a TagPropertyCreate for testing with optional timestamp.
func createTestTag(params createTestTagParams) *feature.TagPropertyCreate {
	tagCreate := &feature.TagPropertyCreate{
		Tag:       params.tag,
		Value:     params.value,
		CreatedBy: params.createdBy,
	}

	if params.timestamp != nil {
		tagCreate.CreatedAt = timestamppb.New(*params.timestamp)
	}

	return tagCreate
}

// verifyNoOriginalTagsRemain checks that none of the original tags are present in the result.
func verifyNoOriginalTagsRemain(params verifyNoOriginalTagsRemainParams) {
	params.t.Helper()
	for _, originalTag := range params.originalTags {
		_, found := collection.Find(
			params.result,
			func(p model.TagPropertyDoc) bool {
				return p.Tag == originalTag.Tag && p.Value == originalTag.Value
			},
		)
		params.asrt.False(
			found,
			"original tag %s should not be present after SetTags",
			originalTag.Tag,
		)
	}
}

// verifyTagsCompletelyReplaced checks that original tags are gone and new tags are present.
func verifyTagsCompletelyReplaced(params verifyTagsCompletelyReplacedParams) {
	params.t.Helper()

	// Verify original tags are completely removed
	verifyNoOriginalTagsRemain(verifyNoOriginalTagsRemainParams{
		t:            params.t,
		asrt:         params.asrt,
		result:       params.result,
		originalTags: params.originalTags,
	})

	// Verify new tags are all present
	verifyNewTagsAdded(verifyNewTagsAddedParams{
		t:             params.t,
		asrt:          params.asrt,
		allProperties: params.result,
		newTags:       params.newTags,
	})

	// Verify exact count
	params.asrt.Len(
		params.result,
		len(params.newTags),
		"should have exactly the number of new tags",
	)
}

// createRemoveTagsRequest creates a RemoveTagsRequest for testing purposes.
func createRemoveTagsRequest(id, tag, value string) *feature.RemoveTagsRequest {
	return &feature.RemoveTagsRequest{
		Id:    id,
		Tag:   tag,
		Value: value,
	}
}

// verifyTagRemoved checks that a specific tag/value pair has been removed from
// the properties.
func verifyTagRemoved(params verifyTagRemovedParams) {
	params.t.Helper()
	tagSet := mapset.NewSet(
		collection.Map(params.properties, propertyDocToTag)...)
	valueSet := mapset.NewSet(
		collection.Map(params.properties, propertyDocToValue)...)
	params.asrt.False(
		tagSet.ContainsOne(params.tag),
		"tag %s should be removed",
		params.tag,
	)
	params.asrt.False(
		valueSet.ContainsOne(params.value),
		"value %s should be removed",
		params.value,
	)
}

// verifyOtherTagsPreserved checks that all tags except the removed one are
// preserved.
func verifyOtherTagsPreserved(params verifyOtherTagsPreservedParams) {
	params.t.Helper()
	origFilTags := pipe.Pipe3(
		params.original,
		collection.CurriedMap(propertyDocToTag),
		collection.CurriedFilter(func(tag string) bool {
			return tag != params.removedTag
		}),
		collection.Sorted,
	)
	updTags := pipe.Pipe2(
		params.updated,
		collection.CurriedMap(propertyDocToTag),
		collection.Sorted,
	)
	origFilVals := pipe.Pipe3(
		params.original,
		collection.CurriedMap(propertyDocToValue),
		collection.CurriedFilter(func(val string) bool {
			return val != params.removedValue
		}),
		collection.Sorted,
	)
	updVals := pipe.Pipe2(
		params.updated,
		collection.CurriedMap(propertyDocToValue),
		collection.Sorted,
	)
	params.asrt.ElementsMatch(origFilTags, updTags)
	params.asrt.ElementsMatch(origFilVals, updVals)
}
