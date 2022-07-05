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

func IsAnnotationNotFound(err error) bool {
	if _, ok := err.(*AnnoNotFoundError); ok {
		return true
	}

	return false
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
