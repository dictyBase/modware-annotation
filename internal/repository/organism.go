package repository

import (
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/organism"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// OrganismRepository is an interface for accessing organism data
// from its data sources.
type OrganismRepository interface {
	// GetOrganism retrieves an organism by ID
	GetOrganism(id string) (*model.OrganismDoc, error)
	// GetOrganismByName retrieves an organism by scientific name (genus + species)
	GetOrganismByName(genus, species string) (*model.OrganismDoc, error)
	// AddOrganism creates a new organism
	AddOrganism(doc *organism.NewOrganism) (*model.OrganismDoc, error)
	// EditOrganism updates an existing organism
	EditOrganism(doc *organism.OrganismUpdate) (*model.OrganismDoc, error)
	// RemoveOrganism deletes an organism
	RemoveOrganism(id string) error
	// ListOrganisms provides a paginated list of organisms along with optional filtering
	ListOrganisms(
		cursor int64,
		limit int64,
		filter string,
	) ([]*model.OrganismDoc, error)
	// ClearOrganisms removes all organisms
	ClearOrganisms() error
	// Dbh returns the underlying database handler
	Dbh() *manager.Database
}
