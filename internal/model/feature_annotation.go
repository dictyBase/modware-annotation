package model

import (
	"encoding/json"
	"fmt"
	"time"

	driver "github.com/arangodb/go-driver"
)

// DbLinkDoc represents a link to an external database identifier.
type DbLinkDoc struct {
	PrimaryId string `json:"primary_id"`
	Database  string `json:"database"`
	Version   int64  `json:"version"`
	LinkType  string `json:"linktype,omitempty"`
	URL       string `json:"url,omitempty"`
	Label     string `json:"label,omitempty"`
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
	NotFound     bool             `json:"-"`
}

// FeatureAnnotationSchema returns a JSON schema for validating feature annotations.
func FeatureAnnotationSchema() ([]byte, error) {
	baseSchema := `{
        "type": "object",
        "properties": %s,
        "required": ["id", "name", "created_at", "created_by"]
    }`

	properties := map[string]interface{}{
		"feature_type": map[string]string{"type": "string"},
		"id":           map[string]string{"type": "string"},
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
		"synonyms": map[string]interface{}{
			"type":  "array",
			"items": map[string]string{"type": "string"},
		},
		"publications": map[string]interface{}{
			"type":  "array",
			"items": map[string]string{"type": "string"},
		},
		"pubmed": map[string]interface{}{
			"type":  "array",
			"items": map[string]string{"type": "string"},
		},
		"dblinks":     getDbLinksSchema(),
		"properties":  getPropertiesSchema(),
		"is_obsolete": map[string]string{"type": "boolean"},
	}

	// Convert properties to JSON and handle potential error
	propsJSON, err := json.Marshal(properties)
	if err != nil {
		return []byte(
				"",
			), fmt.Errorf(
				"failed to marshal feature annotation schema: %v",
				err,
			)
	}

	return []byte(fmt.Sprintf(baseSchema, string(propsJSON))), nil
}

func getDbLinksSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
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

func getPropertiesSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"tag":   map[string]string{"type": "string"},
				"value": map[string]string{"type": "string"},
			},
			"required": []string{"tag", "value"},
		},
	}
}
