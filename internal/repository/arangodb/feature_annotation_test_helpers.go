package arangodb

import (
	"strings"
	"testing"
	"time"

	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type verifyEditSuccessParams struct {
	t        *testing.T
	asrt     *require.Assertions
	tce      editFeatureTestCase
	initial  *model.FeatureAnnotationDoc
	modified *model.FeatureAnnotationDoc
}

type validateDbLinksParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        []model.DbLinkDoc
	expected   []*feature.DbLink
}

type validatePropertiesParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        []model.TagPropertyDoc
	expected   []*feature.TagProperty
}

type editFeatureTestCase struct {
	name    string
	update  *feature.FeatureAnnotationUpdate
	wantErr bool
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
	key        string
}

type featureTestCase struct {
	name      string
	attrs     *feature.FeatureAnnotationAttributes
	id        string
	wantErr   bool
	updatedBy string
}

type featFn func() *feature.NewFeatureAnnotation

func getTestIdentifier(
	wantErr bool,
	repo repository.FeatureAnnotationRepository,
	assert *require.Assertions,
) string {
	if wantErr {
		return "non_existent_id"
	}
	initial, err := repo.AddFeatureAnnotation(getFullFeatureDoc())
	assert.NoError(err, "expected no error adding test feature annotation")

	return initial.AnnoId
}

func getBaseFeatureDoc() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
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
			Name: "sgene",
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

func getUpdaterTestCases() []featureTestCase {
	return []featureTestCase{
		{
			name: "success with explicit updater",
			attrs: &feature.FeatureAnnotationAttributes{
				Name: "explicit updater gene",
			},
			id:        "DDB_G0285427",
			updatedBy: "updater@email.com",
		},
		{
			name: "success with implicit updater (same as creator)",
			attrs: &feature.FeatureAnnotationAttributes{
				Name: "implicit updater gene",
			},
			id: "DDB_G0285432",
		},
	}
}

func getFullFeatureTestCase() *feature.NewFeatureAnnotation {
	return &feature.NewFeatureAnnotation{
		Id: "DDB_G0285425",
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
				{
					PrimaryId: "DDB_G0285420",
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
					CreatedBy: "tester@email.com",
					UpdatedBy: "updater@email.com",
					CreatedAt: timestamppb.New(time.Now()),
					UpdatedAt: timestamppb.New(time.Now()),
				},
				{
					Tag:       "description",
					Value:     "test gene",
					CreatedBy: "tester@email.com",
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

func validateDbLinks(params validateDbLinksParams) {
	params.t.Helper()
	params.assertions.Equal(
		len(params.expected),
		len(params.got),
		"should have same number of dblinks",
	)
	for idx, link := range params.expected {
		params.assertions.Equal(
			link.PrimaryId,
			params.got[idx].PrimaryId,
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

func validateArrayFields(params validateFeatureAnnotationParams) {
	params.t.Helper()
	params.assertions.ElementsMatch(
		params.base.Attributes.Synonyms,
		params.got.Synonyms,
		"should have matching synonyms",
	)
	params.assertions.ElementsMatch(
		params.base.Attributes.Publications,
		params.got.Publications,
		"should have matching publications",
	)
	params.assertions.ElementsMatch(
		params.base.Attributes.Pubmed,
		params.got.Pubmed,
		"should have matching pubmed IDs",
	)
}

func validateUpdater(params validateFeatureAnnotationParams) {
	params.t.Helper()
	if len(params.base.UpdatedBy) > 0 {
		params.assertions.Equal(
			params.base.UpdatedBy,
			params.got.UpdatedBy,
			"should have matching updater when explicitly set",
		)
	} else {
		params.assertions.Equal(
			params.base.CreatedBy,
			params.got.UpdatedBy,
			"should have updater same as creator when not explicitly set",
		)
	}
}

func validateFeatureAnnotation(params validateFeatureAnnotationParams) {
	params.t.Helper()
	// Always validate basic fields
	validateBasicFields(params)
	validateUpdater(params)

	// Only validate array fields if they exist
	if len(params.base.Attributes.Synonyms) > 0 ||
		len(params.base.Attributes.Publications) > 0 ||
		len(params.base.Attributes.Pubmed) > 0 {
		validateArrayFields(params)
	}

	// Only validate DbLinks if they exist
	if len(params.base.Attributes.Dblinks) > 0 {
		validateDbLinks(validateDbLinksParams{
			t:          params.t,
			assertions: params.assertions,
			got:        params.got.DbLinks,
			expected:   params.base.Attributes.Dblinks,
		})
	}

	// Only validate Properties if they exist
	if len(params.base.Attributes.Properties) > 0 {
		validateProperties(validatePropertiesParams{
			t:          params.t,
			assertions: params.assertions,
			got:        params.got.Properties,
			expected:   params.base.Attributes.Properties,
		})
	}
}

func getEditFeatureTestCases(identifier string) []editFeatureTestCase {
	return []editFeatureTestCase{
		{
			name: "should update existing feature annotation",
			update: &feature.FeatureAnnotationUpdate{
				Id:        identifier,
				UpdatedBy: "updater@email.com",
				Attributes: &feature.FeatureAnnotationAttributes{
					Name:     "updated name",
					Synonyms: []string{"new_syn1", "new_syn2"},
				},
			},
			wantErr: false,
		},
		{
			name: "should fail with non-existent ID",
			update: &feature.FeatureAnnotationUpdate{
				Id:        "non_existent_id",
				UpdatedBy: "updater@email.com",
				Attributes: &feature.FeatureAnnotationAttributes{
					Name: "will not update",
				},
			},
			wantErr: true,
		},
		{
			name: "should add new property to existing feature annotation",
			update: &feature.FeatureAnnotationUpdate{
				Id:        identifier,
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
						{
							Tag:       "note",
							Value:     "test note",
							CreatedBy: "creator@email.com",
							UpdatedBy: "updater@email.com",
							CreatedAt: timestamppb.New(time.Now()),
							UpdatedAt: timestamppb.New(time.Now()),
						},
					},
				},
			},
			wantErr: false,
		},
	}
}

func verifyEditError(t *testing.T, asrt *require.Assertions, err error) {
	t.Helper()
	asrt.Error(err, "expected error editing feature annotation")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func verifyEditSuccess(params verifyEditSuccessParams) {
	params.t.Helper()
	params.asrt.Equal(
		params.modified.AnnoId,
		params.initial.AnnoId,
		"IDs should match",
	)
	params.asrt.Equal(
		params.modified.UpdatedBy,
		params.tce.update.UpdatedBy,
		"updater should match",
	)
	params.asrt.Equal(
		params.modified.Name,
		params.tce.update.Attributes.Name,
		"names should match",
	)
	// Original fields should be preserved
	params.asrt.Equal(params.modified.Key, params.initial.Key)

	if len(params.initial.Synonyms) > 0 {
		modsym := slices.Concat(
			params.initial.Synonyms,
			params.tce.update.Attributes.Synonyms,
		)
		slices.Sort(modsym)
		slices.Sort(params.modified.Synonyms)
		params.asrt.ElementsMatch(
			modsym,
			params.modified.Synonyms,
			"synonyms should match",
		)
	}
	// Validate properties if updated
	if params.tce.update.Attributes.Properties != nil {
		modprops := slices.Concat(
			params.initial.Properties,
			collection.Map(
				params.tce.update.Attributes.Properties,
				convertProperty,
			),
		)
		slices.SortFunc(modprops, compareTagProperties)
		slices.SortFunc(params.modified.Properties, compareTagProperties)
		params.asrt.ElementsMatch(
			modprops,
			params.modified.Properties,
			"properties should match after update",
		)
	}
}

func validateBasicFields(params validateFeatureAnnotationParams) {
	params.t.Helper()
	params.assertions.Regexp(
		`^DDB_G\d+`,
		params.got.AnnoId,
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

func cleanupDB(repo repository.FeatureAnnotationRepository) func() {
	return func() {
		_ = repo.Dbh().Drop()
	}
}
