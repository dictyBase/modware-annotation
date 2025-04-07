package arangodb

import (
	"fmt"
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

	t.Run("success", func(t *testing.T) {
		// test depends on shared data
		got, err := repo.GetOrganism(added.Key)
		asrt.NoError(err, "expected no error getting organism")
		validateOrganism(validateOrganismParams{
			assertions: asrt,
			got:        got,
			key:        added.Key,
			baseOrg:    baseOrg,
		})
	})

	t.Run("not found", func(t *testing.T) {
		// test depends on shared data
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

	t.Run("success", func(t *testing.T) {
		// test depends on shared data
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

	t.Run("not found", func(t *testing.T) {
		// test depends on shared data
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

	t.Run("success - full update", func(t *testing.T) {
		// test depends on shared data
		updated := updateOrganism(updateOrganismParams{
			t:      t,
			asrt:   asrt,
			repo:   repo,
			id:     added.Key,
			params: getFullUpdateParams(),
		})
		validateFullUpdate(asrt, updated, added.Key)
	})

	t.Run("success - partial update", func(t *testing.T) {
		// test depends on shared data
		updated := updateOrganism(updateOrganismParams{
			t:      t,
			asrt:   asrt,
			repo:   repo,
			id:     added.Key,
			params: getPartialUpdateParams(),
		})
		validatePartialUpdate(asrt, updated)
	})

	t.Run("not found", func(t *testing.T) {
		// test depends on shared data
		_, err := repo.EditOrganism(getNotFoundUpdateParams())
		asrt.Error(err, "expected error for non-existent organism")
		asrt.True(repository.IsOrganismNotFound(err))
	})
}

//nolint:tparallel
func TestRemoveOrganism(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	added := setupTestOrganism(t, asrt, repo)

	t.Run("success", func(t *testing.T) {
		// test depends on shared data
		err := repo.RemoveOrganism(added.Key)
		asrt.NoError(err, "expected no error removing organism")

		// Verify organism was removed
		_, err = repo.GetOrganism(added.Key)
		asrt.Error(err, "expected error getting removed organism")
		asrt.True(
			repository.IsOrganismNotFound(err),
			"should be organism not found error",
		)
	})

	t.Run("not found", func(t *testing.T) {
		// test depends on shared data
		err := repo.RemoveOrganism("non_existent_id")
		asrt.Error(err, "expected error removing non-existent organism")
		asrt.True(
			repository.IsOrganismNotFound(err),
			"should be organism not found error",
		)
	})
}

//nolint:tparallel
func TestListOrganisms(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	organisms := getTestOrganisms()
	for _, org := range organisms {
		_, err := repo.AddOrganism(org)
		asrt.NoError(err, "expected no error adding test organism")
	}
	t.Run("success", func(t *testing.T) {
		olist, err := repo.ListOrganisms()
		asrt.NoError(err, "expected no error listing organisms")
		asrt.Len(
			olist,
			len(organisms),
			"should return correct number of organisms",
		)
		expectedOrgs := make(map[string]*organism.NewOrganism)
		for _, org := range organisms {
			key := fmt.Sprintf(
				"%s_%s",
				org.Attributes.Genus,
				org.Attributes.Species,
			)
			expectedOrgs[key] = org
		}
		for _, org := range olist {
			key := fmt.Sprintf("%s_%s", org.Genus, org.Species)
			expected, ok := expectedOrgs[key]
			asrt.True(
				ok,
				"should find matching organism for %s %s",
				org.Genus,
				org.Species,
			)
			asrt.Equal(
				expected.Attributes.Species,
				org.Species,
				"should have matching species",
			)
			asrt.Equal(
				expected.Attributes.Genus,
				org.Genus,
				"should have matching genus",
			)
			asrt.Equal(
				expected.CreatedBy,
				org.CreatedBy,
				"should have matching creator",
			)
			asrt.False(org.NotFound, "organism should exist")
		}
	})
}

func TestClearOrganisms(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpOrganismTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Add test organisms
	organisms := getTestOrganisms()
	for _, org := range organisms {
		_, err := repo.AddOrganism(org)
		asrt.NoError(err, "expected no error adding test organism")
	}

	// Verify organisms were added
	list, err := repo.ListOrganisms()
	asrt.NoError(err, "expected no error listing organisms")
	asrt.Len(list, len(organisms), "should have correct number of organisms")

	// Clear organisms
	err = repo.ClearOrganisms()
	asrt.NoError(err, "expected no error clearing organisms")

	_, err = repo.GetOrganismByName("Dictyostelium", "discoideum")
	asrt.Error(err, "expected error getting cleared organism")
	asrt.True(
		repository.IsOrganismNotFound(err),
		"should be organism not found error",
	)
	_, err = repo.GetOrganismByName("Polysphondylium", "fasciculatum")
	asrt.Error(err, "expected error getting cleared organism")
	asrt.True(
		repository.IsOrganismNotFound(err),
		"should be organism not found error",
	)
}
