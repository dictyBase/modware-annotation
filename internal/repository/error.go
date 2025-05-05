package repository

import (
	"fmt"
)

type AnnoNotFoundError struct {
	Id string
}

func (ae *AnnoNotFoundError) Error() string {
	return fmt.Sprintf("annotation id %s not found", ae.Id)
}

type GroupNotFoundError struct {
	Id string
}

func (ge *GroupNotFoundError) Error() string {
	return fmt.Sprintf("group id %s not found", ge.Id)
}

// FeatureNameNotFoundError indicates that a feature annotation with the given name was not found.
type FeatureNameNotFoundError struct {
	Name string
}

// Error returns the error message for FeatureNameNotFoundError.
func (fnf *FeatureNameNotFoundError) Error() string {
	return fmt.Sprintf("feature annotation with name %s not found", fnf.Name)
}

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

func IsPublicationAnnotationNotFound(err error) bool {
	_, ok := err.(*PublicationAnnotationNotFoundError)

	return ok
}

func IsGroupNotFound(err error) bool {
	if _, ok := err.(*GroupNotFoundError); ok {
		return true
	}

	return false
}

type AnnoListNotFoundError struct{}

func (al *AnnoListNotFoundError) Error() string {
	return "annotation list not found"
}

func IsAnnotationListNotFound(err error) bool {
	if _, ok := err.(*AnnoListNotFoundError); ok {
		return true
	}

	return false
}

type AnnoGroupListNotFoundError struct{}

func (agl *AnnoGroupListNotFoundError) Error() string {
	return "annotation group list not found"
}

func IsAnnotationGroupListNotFound(err error) bool {
	if _, ok := err.(*AnnoGroupListNotFoundError); ok {
		return true
	}

	return false
}

type AnnoTagNotFoundError struct {
	Tag string
}

func (at *AnnoTagNotFoundError) Error() string {
	return fmt.Sprintf("annotation tag %s not found", at.Tag)
}

func IsAnnoTagNotFound(err error) bool {
	if _, ok := err.(*AnnoTagNotFoundError); ok {
		return true
	}

	return false
}

type OrganismNotFoundError struct {
	ID string
}

func (onf *OrganismNotFoundError) Error() string {
	return fmt.Sprintf("organism id %s not found", onf.ID)
}

func IsOrganismNotFound(err error) bool {
	if _, ok := err.(*OrganismNotFoundError); ok {
		return true
	}

	return false
}

type ListNotFoundError struct{}

func (lnf *ListNotFoundError) Error() string {
	return "list not found"
}

func IsListNotFound(err error) bool {
	if _, ok := err.(*ListNotFoundError); ok {
		return true
	}

	return false
}
