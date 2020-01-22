package arangodb

import "github.com/dictyBase/go-genproto/dictybaseapis/annotation"

type createParams struct {
	attr *annotation.NewTaggedAnnotationAttributes
	id   string
	tag  string
}

// CollectionParams are the arangodb collections required for storing
// annotations
type CollectionParams struct {
	// Term is the collection for storing ontology term
	Term string `validate:"required"`
	// Relationship is the edge collection for storing term relationship
	Relationship string `validate:"required"`
	// GraphInfo is the collection for storing ontology metadata
	GraphInfo string `validate:"required"`
	// OboGraph is the named graph for connecting term and relationship collections
	OboGraph string `validate:"required"`
	// Annotation is the collection for storing annotation
	Annotation string `validate:"required"`
	// AnnoGroup is the collection for grouping annotations
	AnnoGroup string `validate:"required"`
	// AnnoTerm is the edge collection annotation with a named tag(ontology
	// term)
	AnnoTerm string `validate:"required"`
	// AnnoVersion is the edge collection for connecting different versions of
	// annotations
	AnnoVersion string `validate:"required"`
	// AnnoTagGraph is the named graph for connecting annotation
	// with the ontology
	AnnoTagGraph string `validate:"required"`
	// AnnoVerGraph is the named graph for connecting different
	// version of annotations
	AnnoVerGraph string `validate:"required"`
}
