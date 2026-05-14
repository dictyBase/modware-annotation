// Package model defines data structures for annotations.
package model

import (
	"encoding/json"
	"fmt"
	"time"

	driver "github.com/arangodb/go-driver"
)

// DBLinkDoc represents a link to an external database identifier.
type DBLinkDoc struct {
	PrimaryID string `json:"primary_id"`
	Database  string `json:"database"`
	Version   int64  `json:"version"`
	LinkType  string `json:"linktype,omitempty"`
	URL       string `json:"url,omitempty"`
	Label     string `json:"label,omitempty"`
}

// TagPropertyDoc represents a key-value pair for custom attributes.
type TagPropertyDoc struct {
	Tag       string    `json:"tag"`
	Value     string    `json:"value"`
	CreatedBy string    `json:"created_by"`
	UpdatedBy string    `json:"updated_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// FeatureAnnotationDoc represents a feature annotation document in the
// database.
type FeatureAnnotationDoc struct {
	driver.DocumentMeta
	Type         string           `json:"feature_type,omitempty"`
	AnnoID       string           `json:"feature_id"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
	CreatedBy    string           `json:"created_by"`
	UpdatedBy    string           `json:"updated_by"`
	Name         string           `json:"name"`
	Synonyms     []string         `json:"synonyms,omitempty"`
	Publications []string         `json:"publications,omitempty"`
	Pubmed       []string         `json:"pubmed,omitempty"`
	DBLinks      []DBLinkDoc      `json:"dblinks,omitempty"`
	Properties   []TagPropertyDoc `json:"properties,omitempty"`
	IsObsolete   bool             `json:"is_obsolete"`
	NotFound     bool             `json:"-"`
}

// PubSchema returns a JSON schema containing three mandatory fields:
// id (string), created_at (date-time), and updated_at (date-time).
func PubSchema() ([]byte, error) {
	schema := `{
        		"type": "object",
        		"properties": {
            		"id": { "type": "string" },
            		"created_at": { "type": "string", "format": "date-time" },
            		"updated_at": { "type": "string", "format": "date-time" }
        	},
        	"required": ["id", "created_at", "updated_at"],
        	"additionalProperties": false
    	}`

	return []byte(schema), nil
}

// FeatureAnnotationSchema returns a JSON schema for validating feature annotations.
func FeatureAnnotationSchema() ([]byte, error) {
	baseSchema := `{
        "type": "object",
        "properties": %s,
        "required": ["feature_id", "name", "created_at", "created_by", "updated_at", "updated_by"],
        "additionalProperties": true
    }`

	properties := map[string]any{
		"feature_type": map[string]string{"type": "string"},
		"feature_id":   map[string]string{"type": "string"},
		"created_at": map[string]string{
			"type":   "string",
			"format": "date-time",
		},
		"updated_at": map[string]string{
			"type":   "string",
			"format": "date-time",
		},
		"created_by": map[string]string{"type": "string", "format": "email"},
		"updated_by": map[string]string{"type": "string", "format": "email"},
		"name":       map[string]string{"type": "string"},
		"synonyms": map[string]any{
			"type":  "array",
			"items": map[string]string{"type": "string"},
		},
		"dblinks":     getDBLinksSchema(),
		"properties":  getPropertiesSchema(),
		"is_obsolete": map[string]string{"type": "boolean"},
	}

	// Convert properties to JSON and handle potential error
	propsJSON, err := json.Marshal(properties)
	if err != nil {
		return []byte(""),
			fmt.Errorf(
				"failed to marshal feature annotation schema: %v",
				err,
			)
	}

	return fmt.Appendf(nil, baseSchema, string(propsJSON)), nil
}

func getDBLinksSchema() map[string]any {
	return map[string]any{
		"type": "array",
		"items": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"primary_id": map[string]string{"type": "string"},
				"version":    map[string]string{"type": "integer"},
				"database":   map[string]string{"type": "string"},
				"linktype":   map[string]string{"type": "string"},
				"url":        map[string]string{"type": "string"},
				"label":      map[string]string{"type": "string"},
			},
			"required": []string{"primary_id", "database", "version"},
		},
	}
}

func getPropertiesSchema() map[string]any {
	return map[string]any{
		"type": "array",
		"items": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"tag":   map[string]string{"type": "string"},
				"value": map[string]string{"type": "string"},
				"created_by": map[string]string{
					"type":   "string",
					"format": "email",
				},
				"updated_by": map[string]string{
					"type":   "string",
					"format": "email",
				},
				"created_at": map[string]string{
					"type":   "string",
					"format": "date-time",
				},
				"updated_at": map[string]string{
					"type":   "string",
					"format": "date-time",
				},
			},
			"required": []string{
				"tag",
				"value",
				"created_by",
				"created_at",
				"updated_by",
				"updated_at",
			},
		},
	}
}
