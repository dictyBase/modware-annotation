package model

import (
	"time"

	driver "github.com/arangodb/go-driver"
)

// OrganismDoc represents an organism document stored in ArangoDB.
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

// Schema returns a JSON schema definition for OrganismDoc as []byte.
func Schema() []byte {
	return []byte(`{
        "type": "object",
        "properties": {
            "created_at": {
                "type": "string",
		"format": "date-time"
            },
            "updated_at": {
                "type": "string",
		"format": "date-time"
            },
            "created_by": {
                "type": "string",
                "format": "email"
            },
            "updated_by": {
                "type": "string",
                "format": "email"
            },
            "abbreviation": {
                "type": "string"
            },
            "common_name": {
                "type": "string"
            },
            "species": {
                "type": "string"
            },
            "genus": {
                "type": "string"
            }
        },
	"required": ["genus", "species"]
    }`)
}
