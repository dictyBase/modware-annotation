package model

import (
	"errors"
	"fmt"
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
	mdoc := make([]*AnnoDoc, 0)
	hmap := make(map[string]int)
	for _, m := range a {
		if _, ok := hmap[m.Key]; ok {
			continue
		}
		mdoc = append(mdoc, m)
		hmap[m.Key] = 1
	}

	return mdoc
}

func DocToIds(ml []*AnnoDoc) []string {
	str := make([]string, 0)
	for _, m := range ml {
		str = append(str, m.Key)
	}

	return str
}

func ConvToModel(i interface{}) (*AnnoDoc, error) {
	cmap, isok := i.(map[string]interface{})
	if !isok {
		return &AnnoDoc{}, errors.New("error in typecasting")
	}
	m := &AnnoDoc{
		Value:         cmap["value"].(string),
		EditableValue: cmap["editable_value"].(string),
		CreatedBy:     cmap["created_by"].(string),
		EnrtyId:       cmap["entry_id"].(string),
		Rank:          int64(cmap["rank"].(float64)),
		IsObsolete:    cmap["is_obsolete"].(bool),
		Version:       int64(cmap["version"].(float64)),
	}
	dstr := cmap["created_at"].(string)
	t, err := time.Parse(time.RFC3339, dstr)
	if err != nil {
		return m, fmt.Errorf("error in parsing time %s", err)
	}
	m.CreatedAt = t
	m.DocumentMeta.Key = cmap["_key"].(string)
	m.DocumentMeta.Rev = cmap["_rev"].(string)

	return m, nil
}
