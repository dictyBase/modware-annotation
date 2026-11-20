// Package repository defines interfaces and error types for annotation data storage.
package repository

import (
	"errors"
	"fmt"
)

// AnnoNotFoundError is returned when an annotation with the given ID does not exist.
type AnnoNotFoundError struct {
	ID string
}

func (ae *AnnoNotFoundError) Error() string {
	return fmt.Sprintf("annotation id %s not found", ae.ID)
}

// GroupNotFoundError is returned when an annotation group with the given ID does not exist.
type GroupNotFoundError struct {
	ID string
}

func (ge *GroupNotFoundError) Error() string {
	return fmt.Sprintf("group id %s not found", ge.ID)
}

// FeatureNameNotFoundError indicates that a feature annotation with the given name was not found.
type FeatureNameNotFoundError struct {
	Name string
}

// Error returns the error message for FeatureNameNotFoundError.
func (fnf *FeatureNameNotFoundError) Error() string {
	return fmt.Sprintf("feature annotation with name %s not found", fnf.Name)
}

// IsAnnotationNotFound reports whether err is an AnnoNotFoundError.
func IsAnnotationNotFound(err error) bool {
	if _, ok := err.(*AnnoNotFoundError); ok {
		return true
	}

	return false
}

// IsFeatureNameNotFound checks if the error is a FeatureNameNotFoundError.
func IsFeatureNameNotFound(err error) bool {
	_, ok := err.(*FeatureNameNotFoundError)
	return ok
}

// PublicationAnnotationNotFoundError is returned when no annotations are found for a publication ID.
type PublicationAnnotationNotFoundError struct {
	ID     string
	Source string
}

func (panf *PublicationAnnotationNotFoundError) Error() string {
	return fmt.Sprintf(
		"no annotations found for publication ID %s with source %s",
		panf.ID,
		panf.Source,
	)
}

// IsPublicationAnnotationNotFound reports whether err is a PublicationAnnotationNotFoundError.
func IsPublicationAnnotationNotFound(err error) bool {
	_, ok := err.(*PublicationAnnotationNotFoundError)

	return ok
}

// IsGroupNotFound reports whether err is a GroupNotFoundError.
func IsGroupNotFound(err error) bool {
	if _, ok := err.(*GroupNotFoundError); ok {
		return true
	}

	return false
}

// AnnoListNotFoundError is returned when the annotation list is empty or not found.
type AnnoListNotFoundError struct{}

func (al *AnnoListNotFoundError) Error() string {
	return "annotation list not found"
}

// IsAnnotationListNotFound reports whether err is an AnnoListNotFoundError.
func IsAnnotationListNotFound(err error) bool {
	if _, ok := err.(*AnnoListNotFoundError); ok {
		return true
	}

	return false
}

// AnnoGroupListNotFoundError is returned when the annotation group list is empty or not found.
type AnnoGroupListNotFoundError struct{}

func (agl *AnnoGroupListNotFoundError) Error() string {
	return "annotation group list not found"
}

// IsAnnotationGroupListNotFound reports whether err is an AnnoGroupListNotFoundError.
func IsAnnotationGroupListNotFound(err error) bool {
	if _, ok := err.(*AnnoGroupListNotFoundError); ok {
		return true
	}

	return false
}

// AnnoTagNotFoundError is returned when an annotation tag is not found.
type AnnoTagNotFoundError struct {
	Tag      string
	Ontology string
}

// NewAnnoTagNotFoundError returns an AnnoTagNotFoundError for tag in ontology.
func NewAnnoTagNotFoundError(tag, ontology string) error {
	return &AnnoTagNotFoundError{Tag: tag, Ontology: ontology}
}

func (at *AnnoTagNotFoundError) Error() string {
	return fmt.Sprintf(
		"annotation tag %s not found in ontology %s",
		at.Tag,
		at.Ontology,
	)
}

// AnnoTagAmbiguousError is returned when a tag matches more than one
// ontology term, for example a synonym that two terms share.
type AnnoTagAmbiguousError struct {
	Tag      string
	Ontology string
}

// NewAnnoTagAmbiguousError returns an AnnoTagAmbiguousError for tag in ontology.
func NewAnnoTagAmbiguousError(tag, ontology string) error {
	return &AnnoTagAmbiguousError{Tag: tag, Ontology: ontology}
}

func (at *AnnoTagAmbiguousError) Error() string {
	return fmt.Sprintf(
		"annotation tag %s matches more than one term in ontology %s",
		at.Tag,
		at.Ontology,
	)
}

// IsAnnoTagAmbiguous reports whether err is an AnnoTagAmbiguousError.
func IsAnnoTagAmbiguous(err error) bool {
	var target *AnnoTagAmbiguousError

	return errors.As(err, &target)
}

// IsAnnoTagNotFound reports whether err is an AnnoTagNotFoundError.
func IsAnnoTagNotFound(err error) bool {
	var target *AnnoTagNotFoundError

	return errors.As(err, &target)
}

// OrganismNotFoundError is returned when an organism with the given ID does not exist.
type OrganismNotFoundError struct {
	ID string
}

func (onf *OrganismNotFoundError) Error() string {
	return fmt.Sprintf("organism id %s not found", onf.ID)
}

// IsOrganismNotFound reports whether err is an OrganismNotFoundError.
func IsOrganismNotFound(err error) bool {
	if _, ok := err.(*OrganismNotFoundError); ok {
		return true
	}

	return false
}

// ListNotFoundError is returned when a generic list is empty or not found.
type ListNotFoundError struct{}

func (lnf *ListNotFoundError) Error() string {
	return "list not found"
}

// IsListNotFound reports whether err is a ListNotFoundError.
func IsListNotFound(err error) bool {
	if _, ok := err.(*ListNotFoundError); ok {
		return true
	}

	return false
}
