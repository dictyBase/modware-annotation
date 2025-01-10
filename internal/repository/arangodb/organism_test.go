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

//nolint:tparallel
func TestGetOrganism(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	baseOrg := &organism.NewOrganism{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &organism.OrganismAttributes{
			Species:      "discoideum",
			Genus:        "Dictyostelium",
			CommonName:   "slime mold",
			Abbreviation: "ddis",
		},
	}
	added, err := repo.AddOrganism(baseOrg)
	asrt.NoError(err, "expected no error adding test organism")

	//nolint:paralleltest
	t.Run("success", func(t *testing.T) {
		got, err := repo.GetOrganism(added.Key)
		asrt.NoError(err, "expected no error getting organism")
		validateOrganism(validateOrganismParams{
			assertions: asrt,
			got:        got,
			key:        added.Key,
			baseOrg:    baseOrg,
		})
	})

	//nolint:paralleltest
	t.Run("not found", func(t *testing.T) {
		_, err := repo.GetOrganism("non_existent_id")
		asrt.Error(err, "expected error for non-existent organism")
		asrt.True(
			repository.IsOrganismNotFound(err),
			"should be organism not found error",
		)
	})
}

//nolint:tparallel
func TestGetOrganismByName(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Add test organism first
	baseOrg := &organism.NewOrganism{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &organism.OrganismAttributes{
			Species:      "discoideum",
			Genus:        "Dictyostelium",
			CommonName:   "slime mold",
			Abbreviation: "ddis",
		},
	}
	added, err := repo.AddOrganism(baseOrg)
	asrt.NoError(err, "expected no error adding test organism")

	//nolint:paralleltest
	t.Run("success", func(t *testing.T) {
		got, err := repo.GetOrganismByName(
			baseOrg.Attributes.Genus,
			baseOrg.Attributes.Species,
		)
		asrt.NoError(err, "expected no error getting organism by name")
		validateOrganism(validateOrganismParams{
			assertions: asrt,
			got:        got,
			key:        added.Key,
			baseOrg:    baseOrg,
		})
	})

	//nolint:paralleltest
	t.Run("not found", func(t *testing.T) {
		_, err := repo.GetOrganismByName("NonExistent", "Species")
		asrt.Error(err, "expected error for non-existent organism")
		asrt.True(
			repository.IsOrganismNotFound(err),
			"should be organism not found error",
		)
		asrt.Contains(
			err.Error(),
			"NonExistent Species",
			"error should contain the non-existent organism name",
		)
	})
}

//nolint:tparallel
func TestEditOrganism(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	added := setupTestOrganism(t, asrt, repo)

	//nolint:paralleltest
	t.Run("success - full update", func(t *testing.T) {
		updated := updateOrganism(updateOrganismParams{
			t:      t,
			asrt:   asrt,
			repo:   repo,
			id:     added.Key,
			params: getFullUpdateParams(),
		})
		validateFullUpdate(asrt, updated, added.Key)
	})

	//nolint:paralleltest
	t.Run("success - partial update", func(t *testing.T) {
		updated := updateOrganism(updateOrganismParams{
			t:      t,
			asrt:   asrt,
			repo:   repo,
			id:     added.Key,
			params: getPartialUpdateParams(),
		})
		validatePartialUpdate(asrt, updated)
	})

	//nolint:paralleltest
	t.Run("not found", func(t *testing.T) {
		_, err := repo.EditOrganism(getNotFoundUpdateParams())
		asrt.Error(err, "expected error for non-existent organism")
		asrt.True(repository.IsOrganismNotFound(err))
	})
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
