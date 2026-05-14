package model

import (
	"errors"
	"fmt"
	"time"

	driver "github.com/arangodb/go-driver"
)

// UploadStatus represents the result status of an OBO file upload operation.
type UploadStatus int

// Created, Updated, and Failed are the possible upload status values.
const (
	Created UploadStatus = iota
	Updated
	Failed
)

// AnnoTag represents an ontology term used as an annotation tag.
type AnnoTag struct {
	Name       string `json:"name"`
	ID         string `json:"id"`
	IsObsolete bool   `json:"is_obsolete"`
	Ontology   string `json:"ontology"`
}

// AnnoDoc represents a single tagged annotation document stored in ArangoDB.
type AnnoDoc struct {
	driver.DocumentMeta
	Value         string    `json:"value"`
	EditableValue string    `json:"editable_value"`
	CreatedBy     string    `json:"created_by"`
	EnrtyID       string    `json:"entry_id"`
	Rank          int64     `json:"rank"`
	IsObsolete    bool      `json:"is_obsolete"`
	Version       int64     `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
	Ontology      string    `json:"ontology,omitempty"`
	Tag           string    `json:"tag,omitempty"`
	CvtID         string    `json:"cvtid,omitempty"`
	NotFound      bool
}

// AnnoGroup represents a group of annotation documents.
type AnnoGroup struct {
	AnnoDocs  []*AnnoDoc `json:"annotations"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	GroupID   string     `json:"group_id"`
}

// DBGroup represents the database-level structure of an annotation group.
type DBGroup struct {
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Group     []string  `json:"group"`
	GroupID   string    `json:"_key,omitempty"`
}

// UniqueModel returns a new slice containing only the unique elements of the input.
func UniqueModel[T comparable](slice []T) []T {
	result := make([]T, 0)
	seen := make(map[T]bool)
	for _, item := range slice {
		if _, ok := seen[item]; !ok {
			result = append(result, item)
			seen[item] = true
		}
	}

	return result
}

// DocToIDs extracts the document keys from a slice of AnnoDoc pointers.
func DocToIDs(ml []*AnnoDoc) []string {
	str := make([]string, 0)
	for _, m := range ml {
		str = append(str, m.Key)
	}

	return str
}

// ConvToModel converts a generic interface value to an AnnoDoc model.
func ConvToModel(i interface{}) (*AnnoDoc, error) {
	cmap, isok := i.(map[string]interface{})
	if !isok {
		return &AnnoDoc{}, errors.New("error in typecasting")
	}
	adoc := &AnnoDoc{
		Value:         cmap["value"].(string),
		EditableValue: cmap["editable_value"].(string),
		CreatedBy:     cmap["created_by"].(string),
		EnrtyID:       cmap["entry_id"].(string),
		Rank:          int64(cmap["rank"].(float64)),
		IsObsolete:    cmap["is_obsolete"].(bool),
		Version:       int64(cmap["version"].(float64)),
	}
	dstr, isok := cmap["created_at"].(string)
	if !isok {
		return &AnnoDoc{}, errors.New("error in typecasting")
	}
	t, err := time.Parse(time.RFC3339, dstr)
	if err != nil {
		return adoc, fmt.Errorf("error in parsing time %s", err)
	}
	adoc.CreatedAt = t
	adoc.Key = cmap["_key"].(string)
	adoc.Rev = cmap["_rev"].(string)

	return adoc, nil
}
