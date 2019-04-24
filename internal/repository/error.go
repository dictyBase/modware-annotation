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
