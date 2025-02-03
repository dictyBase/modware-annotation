package arangodb

import (
	"slices"
	"testing"
	"time"

	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type verifyEditSuccessParams struct {
	t       *testing.T
	asrt    *require.Assertions
	tce     editFeatureTestCase
	doc     *model.FeatureAnnotationDoc
	baseDoc *feature.NewFeatureAnnotation
	added   *model.FeatureAnnotationDoc
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

func getFeatureTestCases() []featureTestCase {
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
		{
			name: "success with all fields",
			attrs: &feature.FeatureAnnotationAttributes{
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
						CreatedBy: "tester@email.com",
						UpdatedBy: "updater@email.com",
						CreatedAt: timestamppb.New(time.Now()),
						UpdatedAt: timestamppb.New(time.Now()),
					},
				},
			},
			id: "DDB_G0285425",
		},
		{
			name: "success with multiple tag properties",
			attrs: &feature.FeatureAnnotationAttributes{
				Name: "multi-property gene",
				Properties: []*feature.TagProperty{
					{
						Tag:       "description",
						Value:     "test description",
						CreatedBy: "creator1@email.com",
						CreatedAt: timestamppb.New(time.Now()),
					},
					{
						Tag:       "note",
						Value:     "test note",
						CreatedBy: "creator2@email.com",
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
			id: "DDB_G0285426",
		},
		{
			name: "success with only required fields",
			attrs: &feature.FeatureAnnotationAttributes{
				Name: "required fields gene",
			},
			id: "DDB_G0285428",
		},
		{
			name: "success with empty properties array",
			attrs: &feature.FeatureAnnotationAttributes{
				Name:       "no properties gene",
				Properties: []*feature.TagProperty{},
			},
			id: "DDB_G0285425",
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
		}
		// Verify timestamps exist
		params.assertions.False(
			params.got[idx].CreatedAt.IsZero(),
			"should have non-zero created_at timestamp",
		)
		if prop.UpdatedBy != "" {
			params.assertions.False(
				params.got[idx].UpdatedAt.IsZero(),
				"should have non-zero updated_at timestamp when updater exists",
			)
		}
	}
}

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

func validateFeatureAnnotation(params validateFeatureAnnotationParams) {
	params.t.Helper()
	params.assertions.Equal(
		params.key,
		params.got.Key,
		"should have matching keys",
	)
	params.assertions.Equal(
		params.base.Id,
		params.got.AnnoId,
		"should have matching IDs",
	)
	params.assertions.Equal(
		params.base.CreatedBy,
		params.got.CreatedBy,
		"should have matching creator",
	)
	params.assertions.Equal(
		params.base.Attributes.Name,
		params.got.Name,
		"should have matching name",
	)
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
	// Validate UpdatedBy field
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
	validateDbLinks(validateDbLinksParams{
		t:          params.t,
		assertions: params.assertions,
		got:        params.got.DbLinks,
		expected:   params.base.Attributes.Dblinks,
	})
	validateProperties(validatePropertiesParams{
		t:          params.t,
		assertions: params.assertions,
		got:        params.got.Properties,
		expected:   params.base.Attributes.Properties,
	})
}

func getBaseFeatureDoc() *feature.NewFeatureAnnotation {
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

func getEditFeatureTestCases(id string) []editFeatureTestCase {
	return []editFeatureTestCase{
		{
			name: "should update existing feature annotation",
			update: &feature.FeatureAnnotationUpdate{
				Id:        id,
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
		params.tce.update.Id,
		params.doc.AnnoId,
		"IDs should match",
	)
	params.asrt.Equal(
		params.tce.update.UpdatedBy,
		params.doc.UpdatedBy,
		"updater should match",
	)
	params.asrt.Equal(
		params.tce.update.Attributes.Name,
		params.doc.Name,
		"names should match",
	)
	params.asrt.ElementsMatch(
		slices.Concat(
			params.added.Synonyms,
			params.tce.update.Attributes.Synonyms,
		),
		params.doc.Synonyms,
		"synonyms should match",
	)
	// Original fields should be preserved
	params.asrt.Equal(params.baseDoc.CreatedBy, params.doc.CreatedBy)
	params.asrt.Equal(params.added.Key, params.doc.Key)
}
