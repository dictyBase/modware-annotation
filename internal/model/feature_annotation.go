package model

import (
	"time"

	driver "github.com/arangodb/go-driver"
)

// DbLinkDoc represents a link to an external database identifier.
type DbLinkDoc struct {
	PrimaryId string `json:"primary_id"`
	Version   int64  `json:"version"`
	Database  string `json:"database"`
	LinkType  string `json:"linktype"`
	URL       string `json:"url"`
	Label     string `json:"label"`
}

// TagPropertyDoc represents a key-value pair for custom attributes.
type TagPropertyDoc struct {
	Tag   string `json:"tag"`
	Value string `json:"value"`
}

// FeatureAnnotationDoc represents a feature annotation document in the
// database.
type FeatureAnnotationDoc struct {
	driver.DocumentMeta
	Type         string           `json:"feature_type,omitempty"`
	Id           string           `json:"id"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
	CreatedBy    string           `json:"created_by"`
	UpdatedBy    string           `json:"updated_by"`
	Name         string           `json:"name,omitempty"`
	Synonyms     []string         `json:"synonyms,omitempty"`
	Publications []string         `json:"publications,omitempty"`
	Pubmed       []string         `json:"pubmed,omitempty"`
	DbLinks      []DbLinkDoc      `json:"dblinks,omitempty"`
	Properties   []TagPropertyDoc `json:"properties,omitempty"`
	IsObsolete   bool             `json:"is_obsolete"`
	Version      int64            `json:"version"`
	NotFound     bool             `json:"-"`
}

        "id": {"type": "string"},
        "created_at": {"type": "string", "format": "date-time"},
        "updated_at": {"type": "string", "format": "date-time"},
        "created_by": {"type": "string", "format": "email"},
        "updated_by": {"type": "string", "format": "email"},
        "name": {"type": "string"},
        "synonyms": {
            "type": "array",
            "items": {"type": "string"}
        },
        "publications": {
            "type": "array",
            "items": {"type": "string"}
        },
        "pubmed": {
            "type": "array",
            "items": {"type": "string"}
        },
        "dbxrefs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dbxref_id": {"type": "string"},
                    "version": {"type": "integer"},
                    "database": {"type": "string"}
                },
                "required": ["dbxref_id", "database"]
            }
        },
        "is_obsolete": {"type": "boolean"},
        "version": {"type": "integer"}
    },
    "required": ["id", "version"]
}`)
}
