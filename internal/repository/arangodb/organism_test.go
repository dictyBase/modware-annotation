package arangodb

import (
	"testing"
	"time"

	"github.com/dictyBase/go-genproto/dictybaseapis/organism"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
