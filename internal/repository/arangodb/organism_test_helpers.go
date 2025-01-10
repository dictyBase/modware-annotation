package arangodb

import (
	"testing"
	"time"

	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/arangomanager/testarango"
	"github.com/dictyBase/go-genproto/dictybaseapis/organism"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type validateOrganismParams struct {
	assertions *require.Assertions
	got        *model.OrganismDoc
	key        string
	baseOrg    *organism.NewOrganism
}

type updateOrganismParams struct {
	t      *testing.T
	asrt   *require.Assertions
	repo   repository.OrganismRepository
	id     string
	params *organism.OrganismUpdate
}

func getFullUpdateParams() *organism.OrganismUpdate {
	return &organism.OrganismUpdate{
		UpdatedBy: "another@email.com",
		Attributes: &organism.OrganismAttributes{
			Species: "purpureum", Genus: "Dictyostelium",
			CommonName: "purple slime mold", Abbreviation: "dpur",
		},
	}
}

func getPartialUpdateParams() *organism.OrganismUpdate {
	return &organism.OrganismUpdate{
		UpdatedBy: "partial@email.com",
		Attributes: &organism.OrganismAttributes{
			CommonName: "new common name",
		},
	}
}

func getNotFoundUpdateParams() *organism.OrganismUpdate {
	return &organism.OrganismUpdate{
		Id: "non_existent_id", UpdatedBy: "mock@email.com",
		Attributes: &organism.OrganismAttributes{Species: "new species"},
	}
}

func updateOrganism(params updateOrganismParams) *model.OrganismDoc {
	params.t.Helper()
	params.params.Id = params.id
	updated, err := params.repo.EditOrganism(params.params)
	params.asrt.NoError(err, "expected no error updating organism")

	return updated
}

func validateFullUpdate(
	asrt *require.Assertions,
	updated *model.OrganismDoc,
	originalKey string,
) {
	params := getFullUpdateParams()
	asrt.Equal(params.Attributes.Species, updated.Species)
	asrt.Equal(params.Attributes.Genus, updated.Genus)
	asrt.Equal(params.Attributes.CommonName, updated.CommonName)
	asrt.Equal(params.Attributes.Abbreviation, updated.Abbreviation)
	asrt.Equal(params.UpdatedBy, updated.UpdatedBy)
	asrt.Equal(originalKey, updated.Key)
}

func validatePartialUpdate(
	asrt *require.Assertions,
	updated *model.OrganismDoc,
) {
	asrt.Equal("new common name", updated.CommonName)
	asrt.Equal("purpureum", updated.Species)
	asrt.Equal("Dictyostelium", updated.Genus)
	asrt.Equal("dpur", updated.Abbreviation)
	asrt.Equal("partial@email.com", updated.UpdatedBy)
}

func validateOrganism(params validateOrganismParams) {
	params.assertions.Equal(
		params.key,
		params.got.Key,
		"should have matching keys",
	)
	params.assertions.Equal(
		params.baseOrg.Attributes.Species,
		params.got.Species,
		"should have matching species",
	)
	params.assertions.Equal(
		params.baseOrg.Attributes.Genus,
		params.got.Genus,
		"should have matching genus",
	)
	params.assertions.Equal(
		params.baseOrg.Attributes.CommonName,
		params.got.CommonName,
		"should have matching common name",
	)
	params.assertions.Equal(
		params.baseOrg.Attributes.Abbreviation,
		params.got.Abbreviation,
		"should have matching abbreviation",
	)
	params.assertions.Equal(
		params.baseOrg.CreatedBy,
		params.got.CreatedBy,
		"should have matching creator",
	)
}

func setupTestOrganism(
	t *testing.T,
	asrt *require.Assertions,
	repo repository.OrganismRepository,
) *model.OrganismDoc {
	t.Helper()
	baseOrg := &organism.NewOrganism{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &organism.OrganismAttributes{
			Species: "discoideum", Genus: "Dictyostelium",
			CommonName: "slime mold", Abbreviation: "ddis",
		},
	}
	added, err := repo.AddOrganism(baseOrg)
	asrt.NoError(err, "expected no error adding test organism")

	return added
}

func getOrganismTestCases(baseOrg *organism.NewOrganism) []struct {
	name     string
	attrs    *organism.OrganismAttributes
	wantErr  bool
	validate func(*require.Assertions, *model.OrganismDoc)
} {
	return []struct {
		name     string
		attrs    *organism.OrganismAttributes
		wantErr  bool
		validate func(*require.Assertions, *model.OrganismDoc)
	}{
		{
			name: "success",
			attrs: &organism.OrganismAttributes{
				Species:      "discoideum",
				Genus:        "Dictyostelium",
				CommonName:   "slime mold",
				Abbreviation: "ddis",
			},
			validate: func(asrt *require.Assertions, mdl *model.OrganismDoc) {
				asrt.Equal("discoideum", mdl.Species)
				asrt.Equal("Dictyostelium", mdl.Genus)
				asrt.Equal("slime mold", mdl.CommonName)
				asrt.Equal("ddis", mdl.Abbreviation)
				asrt.Equal(baseOrg.CreatedBy, mdl.CreatedBy)
				asrt.False(mdl.NotFound)
			},
		}, {
			name: "minimal",
			attrs: &organism.OrganismAttributes{
				Species: "aurelia",
				Genus:   "Polysphondylium",
			},
			validate: func(asrt *require.Assertions, mdl *model.OrganismDoc) {
				asrt.Equal("aurelia", mdl.Species)
				asrt.Equal("Polysphondylium", mdl.Genus)
				asrt.Empty(mdl.CommonName)
				asrt.Empty(mdl.Abbreviation)
				asrt.Equal(baseOrg.CreatedBy, mdl.CreatedBy)
				asrt.False(mdl.NotFound)
			},
		},
	}
}

func setUpOrganismTest(
	t *testing.T,
) (*require.Assertions, repository.OrganismRepository) {
	t.Helper()
	tra, err := testarango.NewTestArangoFromEnv(true)
	require.NoError(t, err, "unable to construct new TestArango instance")
	assert := require.New(t)
	repo, err := NewOrganismRepo(
		getConnectParamsFromDb(tra),
		&OrganismCollectionParams{Organism: "organism"},
	)
	assert.NoErrorf(
		err,
		"expect no error connecting to organism repository, received %s",
		err,
	)

	return assert, repo
}

func getTestOrganisms() []*organism.NewOrganism {
	return []*organism.NewOrganism{
		{
			CreatedBy: "mock@email.com",
			CreatedAt: timestamppb.New(time.Now()),
			Attributes: &organism.OrganismAttributes{
				Species: "discoideum",
				Genus:   "Dictyostelium",
			},
		},
		{
			CreatedBy: "mock@email.com",
			CreatedAt: timestamppb.New(time.Now()),
			Attributes: &organism.OrganismAttributes{
				Species: "purpureum",
				Genus:   "Dictyostelium",
			},
		},
		{
			CreatedBy: "mock@email.com",
			CreatedAt: timestamppb.New(time.Now()),
			Attributes: &organism.OrganismAttributes{
				Species: "fasciculatum",
				Genus:   "Polysphondylium",
			},
		},
	}
}

func getConnectParamsFromDb(tra *testarango.TestArango) *manager.ConnectParams {
	return &manager.ConnectParams{
		User:     tra.User,
		Pass:     tra.Pass,
		Database: tra.Database,
		Host:     tra.Host,
		Port:     tra.Port,
		Istls:    false,
	}
}
