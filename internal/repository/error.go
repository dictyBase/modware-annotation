package repository

import (
	"fmt"
)

type AnnoNotFound struct {
	Id string
}

func (ae *AnnoNotFound) Error() string {
	return fmt.Sprintf("annotation id %s not found", ae.Id)
}

type GroupNotFound struct {
	Id string
}

func (ge *GroupNotFound) Error() string {
	return fmt.Sprintf("group id %s not found", ge.Id)
}

func IsAnnotationNotFound(err error) bool {
	if _, ok := err.(*AnnoNotFound); ok {
		return true
	}
	return false
}

func IsGroupNotFound(err error) bool {
	if _, ok := err.(*GroupNotFound); ok {
		return true
	}
	return false
}

type AnnoListNotFound struct{}

func (al *AnnoListNotFound) Error() string {
	return "annotation list not found"
}

func IsAnnotationListNotFound(err error) bool {
	if _, ok := err.(*AnnoListNotFound); ok {
		return true
	}
	return false
}

type AnnoGroupListNotFound struct{}

func (agl *AnnoGroupListNotFound) Error() string {
	return "annotation group list not found"
}

func IsAnnotationGroupListNotFound(err error) bool {
	if _, ok := err.(*AnnoGroupListNotFound); ok {
		return true
	}
	return false
}

type AnnoTagNotFound struct{ Name string }

func (at *AnnoTagNotFound) Error() string {
	return fmt.Sprintf("annotation tag %s not found", at.Name)
}

func IsAnnoTagNotFound(err error) bool {
	if _, ok := err.(*AnnoTagNotFound); ok {
		return true
	}
	return false
}
