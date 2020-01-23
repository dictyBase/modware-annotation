package model

import (
	"time"

	driver "github.com/arangodb/go-driver"
)

type AnnoTag struct {
	Name       string `json:"name"`
	ID         string `json:"id"`
	IsObsolete bool   `json:"is_obsolete"`
	Ontology   string `json:"ontology"`
}

type AnnoDoc struct {
	driver.DocumentMeta
	Value         string    `json:"value"`
	EditableValue string    `json:"editable_value"`
	CreatedBy     string    `json:"created_by"`
	EnrtyId       string    `json:"entry_id"`
	Rank          int64     `json:"rank"`
	IsObsolete    bool      `json:"is_obsolete"`
	Version       int64     `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
	Ontology      string    `json:"ontology,omitempty"`
	Tag           string    `json:"tag,omitempty"`
	CvtId         string    `json:"cvtid,omitempty"`
	NotFound      bool
}

type AnnoGroup struct {
	AnnoDocs  []*AnnoDoc `json:"annotations"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	GroupId   string     `json:"group_id"`
}

type DbGroup struct {
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Group     []string  `json:"group"`
	GroupId   string    `json:"_key,omitempty"`
}
