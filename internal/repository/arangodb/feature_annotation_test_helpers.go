package arangodb

import (
	"testing"

	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
)

type validateFeatureAnnotationParams struct {
	t          *testing.T
	assertions *require.Assertions
	got        *model.FeatureAnnotationDoc
	base       *feature.NewFeatureAnnotation
	key        string
}

type featureTestCase struct {
	name    string
	attrs   *feature.FeatureAnnotationAttributes
	id      string
	wantErr bool
}

func getFeatureTestCases() []featureTestCase {
	return []featureTestCase{
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
						Tag:   "description",
						Value: "test gene",
					},
				},
			},
			id: "DDB_G0285425",
		},
		{
			name: "success with only required fields",
			attrs: &feature.FeatureAnnotationAttributes{
				Name: "required fields gene",
			},
			id: "DDB_G0285428",
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
		getConnectParamsFromDb(tra),
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

func validateProperties(
	t *testing.T,
	asrt *require.Assertions,
	got []model.TagPropertyDoc,
	expected []*feature.TagProperty,
) {
	t.Helper()
	asrt.Equal(
		len(expected),
		len(got),
		"should have same number of properties",
	)
	for i, prop := range expected {
		asrt.Equal(prop.Tag, got[i].Tag, "should have matching tag")
		asrt.Equal(prop.Value, got[i].Value, "should have matching value")
	}
}

func validateDbLinks(
	t *testing.T,
	asrt *require.Assertions,
	got []model.DbLinkDoc,
	expected []*feature.DbLink,
) {
	t.Helper()
	asrt.Equal(len(expected), len(got), "should have same number of dblinks")
	for idx, link := range expected {
		asrt.Equal(
			link.PrimaryId,
			got[idx].PrimaryId,
			"should have matching primary ID",
		)
		asrt.Equal(
			link.Database,
			got[idx].Database,
			"should have matching database",
		)
		asrt.Equal(
			link.Version,
			got[idx].Version,
			"should have matching version",
		)
		asrt.Equal(
			link.Linktype,
			got[idx].LinkType,
			"should have matching link type",
		)
		asrt.Equal(link.Url, got[idx].URL, "should have matching URL")
		asrt.Equal(link.Label, got[idx].Label, "should have matching label")
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
		params.got.Id,
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
	validateDbLinks(
		params.t,
		params.assertions,
		params.got.DbLinks,
		params.base.Attributes.Dblinks,
	)
	validateProperties(
		params.t,
		params.assertions,
		params.got.Properties,
		params.base.Attributes.Properties,
	)
}
