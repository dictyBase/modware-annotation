package arangodb

import (
	"testing"
	"time"

	"github.com/dictyBase/arangomanager/testarango"
	"github.com/dictyBase/go-genproto/dictybaseapis/organism"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func TestAddOrganism(t *testing.T) {
	t.Parallel()
	baseOrg := &organism.NewOrganism{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
	}
	for _, tcs := range getOrganismTestCases(baseOrg) {
		t.Run(tcs.name, func(t *testing.T) {
			t.Parallel()
			asrt, repo := setUpOrganismTest(t)
			t.Cleanup(func() { _ = repo.Dbh().Drop() })
			baseOrg.Attributes = tcs.attrs
			doc, err := repo.AddOrganism(baseOrg)
			if tcs.wantErr {
				asrt.Error(err)

				return
			}
			asrt.NoError(err)
			tcs.validate(asrt, doc)
		})
	}
}

func TestAddDuplicateOrganism(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create base organism
	baseOrg := &organism.NewOrganism{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &organism.OrganismAttributes{
			Species: "discoideum",
			Genus:   "Dictyostelium",
		},
	}

	// Add first organism
	_, err := repo.AddOrganism(baseOrg)
	asrt.NoError(err, "expected no error adding first organism")

	// Attempt to add duplicate organism
	_, err = repo.AddOrganism(baseOrg)
	asrt.Error(err, "expected error when adding duplicate organism")
	asrt.Contains(
		err.Error(),
		"organism Dictyostelium discoideum already exists",
		"expected duplicate organism error message",
	)
}
