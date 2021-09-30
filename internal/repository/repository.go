package repository

import (
	"io"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// TaggedAnnotationRepository is an interface for accessing annotation
// data from its data sources
type TaggedAnnotationRepository interface {
	// GetAnnotationById retrieves an annotation
	GetAnnotationByID(id string) (*model.AnnoDoc, error)
	GetAnnotationByEntry(req *annotation.EntryAnnotationRequest) (*model.AnnoDoc, error)
	AddAnnotation(na *annotation.NewTaggedAnnotation) (*model.AnnoDoc, error)
	EditAnnotation(ua *annotation.TaggedAnnotationUpdate) (*model.AnnoDoc, error)
	RemoveAnnotation(id string, purge bool) error
	// ListAnnotationGroup provides a paginated list of annotation along
	// with optional filtering
	ListAnnotations(cursor int64, limit int64, filter string) ([]*model.AnnoDoc, error)
	ClearAnnotations() error
	Clear() error
	// AddAnnotationGroup creates a new annotation group
	AddAnnotationGroup(idslice ...string) (*model.AnnoGroup, error)
	// GetAnnotationGroup retrieves an annotation group
	GetAnnotationGroup(groupID string) (*model.AnnoGroup, error)
	// AppendToAnnotationGroup adds new annotations to an existing group
	AppendToAnnotationGroup(groupID string, idslice ...string) (*model.AnnoGroup, error)
	// DeleteAnnotationGroup deletes an annotation group
	RemoveAnnotationGroup(groupID string) error
	// RemoveFromAnnotationGroup remove annotations from an existing group
	RemoveFromAnnotationGroup(groupID string, idslice ...string) (*model.AnnoGroup, error)
	// ListAnnotationGroup provides a paginated list of annotation groups along
	// with optional filtering
	ListAnnotationGroup(cursor, limit int64, filter string) ([]*model.AnnoGroup, error)
	// GetAnnotationTag retrieves tag information
	GetAnnotationTag(name, ontology string) (*model.AnnoTag, error)
	LoadOboJSON(r io.Reader) (model.UploadStatus, error)
}
