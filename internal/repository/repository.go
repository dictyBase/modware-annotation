package repository

import (
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// TaggedAnnotationRepository is an interface for accessing annotation
// data from its data sources
type TaggedAnnotationRepository interface {
	GetAnnotationById(id string) (*model.AnnoDoc, error)
	GetAnnotationByEntry(req *annotation.EntryAnnotationRequest) (*model.AnnoDoc, error)
	AddAnnotation(na *annotation.NewTaggedAnnotation) (*model.AnnoDoc, error)
	EditAnnotation(ua *annotation.TaggedAnnotationUpdate) (*model.AnnoDoc, error)
	RemoveAnnotation(id string) error
	ListAnnotations(cursor int64, limit int64) ([]*model.AnnoDoc, error)
	Clear() error
}
