package arangodb

import "github.com/dictyBase/go-genproto/dictybaseapis/annotation"

type createParams struct {
	attr *annotation.NewTaggedAnnotationAttributes
	id   string
	tag  string
}

// CollectionParams are the arangodb collections required for storing
// annotations.
type CollectionParams struct {
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
	// AnnoIndexes is a slice of fields to use as persistent indexes for the
	// Annotation collection
	AnnoIndexes []string `validate:"required"`
}
