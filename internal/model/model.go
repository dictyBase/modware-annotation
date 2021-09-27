package model

import (
	"time"

	driver "github.com/arangodb/go-driver"
)

type UploadStatus int

const (
	Created UploadStatus = iota
	Updated
	Failed
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

func UniqueModel(a []*AnnoDoc) []*AnnoDoc {
	var ml []*AnnoDoc
	h := make(map[string]int)
	for _, m := range a {
		if _, ok := h[m.Key]; ok {
			continue
		}
		ml = append(ml, m)
		h[m.Key] = 1
	}
	return ml
}

func DocToIds(ml []*AnnoDoc) []string {
	var s []string
	for _, m := range ml {
		s = append(s, m.Key)
	}
	return s
}

func ConvToModel(i interface{}) (*AnnoDoc, error) {
	c := i.(map[string]interface{})
	m := &AnnoDoc{
		Value:         c["value"].(string),
		EditableValue: c["editable_value"].(string),
		CreatedBy:     c["created_by"].(string),
		EnrtyId:       c["entry_id"].(string),
		Rank:          int64(c["rank"].(float64)),
		IsObsolete:    c["is_obsolete"].(bool),
		Version:       int64(c["version"].(float64)),
	}
	dstr := c["created_at"].(string)
	t, err := time.Parse(time.RFC3339, dstr)
	if err != nil {
		return m, err
	}
	m.CreatedAt = t
	m.DocumentMeta.Key = c["_key"].(string)
	m.DocumentMeta.Rev = c["_rev"].(string)
	return m, nil
}
