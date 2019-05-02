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
	ClearAnnotations() error
	Clear() error
	// AddAnnotationGroup creates a new annotation group
	AddAnnotationGroup([]string) (string, []*model.AnnoDoc, error)
	// GetAnnotationGroup retrieves an annotation group
	GetAnnotationGroup(groudId string) (*model.AnnoGroup, error)
	// GetAnnotationGroupByEntry retrieves an annotation group associated with an entry
	GetAnnotationGroupByEntry(req *annotation.EntryAnnotationRequest) ([]*model.AnnoDoc, error)
	// AppendToAnnotationGroup adds new annotations to an existing group
	AppendToAnnotationGroup(groupId string, idslice ...string) ([]*model.AnnoDoc, error)
	// DeleteAnnotationGroup deletes an annotation group
	RemoveAnnotationGroup(groupId string) error
	// RemoveFromAnnotationGroup remove annotations from an existing group
	RemoveFromAnnotationGroup(groupId string, idslice ...string) ([]*model.AnnoDoc, error)
	// ListAnnotationGroup provides a paginated list of annotation groups along
	// with optional filtering
	ListAnnotationGroup(cursor, limit int64, filter string) ([]*model.AnnoGroupList, error)
}
