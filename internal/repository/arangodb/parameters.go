package arangodb

import "github.com/dictyBase/go-genproto/dictybaseapis/annotation"

type createParams struct {
	attr *annotation.NewTaggedAnnotationAttributes
	id   string
	tag  string
}

// OrganismCollectionParams are the arangodb collections required for storing
// organisms.
type OrganismCollectionParams struct {
	// Organism is the collection for storing organisms
	Organism string `validate:"required"`
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

// FeatureCollectionParams contains the parameters for feature annotation
// collections.
type FeatureCollectionParams struct {
	// Feature is the collection for storing feature annotations
	Feature string `validate:"required"`
	Pub     string `validate:"required"`
	Edge    string `validate:"required"`
	// Graph is the name of the graph connecting features and publications
	Graph string `validate:"required"`
}
