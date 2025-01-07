package model

import (
	"time"

	driver "github.com/arangodb/go-driver"
)

type OrganismDoc struct {
	driver.DocumentMeta
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	CreatedBy    string    `json:"created_by"`
	UpdatedBy    string    `json:"updated_by"`
	Abbreviation string    `json:"abbreviation,omitempty"`
	CommonName   string    `json:"common_name,omitempty"`
	Species      string    `json:"species"`
	Genus        string    `json:"genus"`
	NotFound     bool
}
