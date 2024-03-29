package arangodb

import (
	"errors"

	"github.com/dictyBase/modware-annotation/internal/model"
)

// StringPairWiseIterator is the container for iterator.
type StringPairWiseIterator struct {
	slice []string
	// keeps track of the first index
	firstIdx int
	// keeps track of the next index in the pair
	secondIdx int
	// last index of the slice
	lastIdx int
	// toogle the state for fetching the first pair
	firstPair bool
}

// NewStringPairWiseIterator is the constructor, returns error in case of empty or
// slice with single element.
func NewStringPairWiseIterator(mdl []string) (StringPairWiseIterator, error) {
	if len(mdl) <= 1 {
		return StringPairWiseIterator{}, errors.New("not enough element to fetch pairs")
	}

	return StringPairWiseIterator{
		slice:     mdl,
		firstIdx:  0,
		secondIdx: 1,
		lastIdx:   len(mdl) - 1,
		firstPair: true,
	}, nil
}

// NextStringPair moves the iteration to the next pair. If NextStringPair() returns true
// the pair could be retrieved by Pair() method. If it is called for the first
// time it points to the first pair.
func (p *StringPairWiseIterator) NextStringPair() bool {
	if p.firstPair {
		p.firstPair = false

		return true
	}
	if p.secondIdx == p.lastIdx {
		return false
	}
	p.firstIdx++
	p.secondIdx++

	return true
}

// StringPair retrieves the pair of elements from the slice.
func (p *StringPairWiseIterator) StringPair() (string, string) {
	return p.slice[p.firstIdx], p.slice[p.secondIdx]
}

// ModelAnnoDocPairWiseIterator is the container for iterator.
type ModelAnnoDocPairWiseIterator struct {
	slice []*model.AnnoDoc
	// keeps track of the first index
	firstIdx int
	// keeps track of the next index in the pair
	secondIdx int
	// last index of the slice
	lastIdx int
	// toogle the state for fetching the first pair
	firstPair bool
}

// NewModelAnnoDocPairWiseIterator is the constructor, returns error in case of empty or
// slice with single element.
func NewModelAnnoDocPairWiseIterator(mdl []*model.AnnoDoc) (ModelAnnoDocPairWiseIterator, error) {
	if len(mdl) <= 1 {
		return ModelAnnoDocPairWiseIterator{}, errors.New("not enough element to fetch pairs")
	}

	return ModelAnnoDocPairWiseIterator{
		slice:     mdl,
		firstIdx:  0,
		secondIdx: 1,
		lastIdx:   len(mdl) - 1,
		firstPair: true,
	}, nil
}

// NextModelAnnoDocPair moves the iteration to the next pair. If NextModelAnnoDocPair() returns true
// the pair could be retrieved by Pair() method. If it is called for the first
// time it points to the first pair.
func (p *ModelAnnoDocPairWiseIterator) NextModelAnnoDocPair() bool {
	if p.firstPair {
		p.firstPair = false

		return true
	}
	if p.secondIdx == p.lastIdx {
		return false
	}
	p.firstIdx++
	p.secondIdx++

	return true
}

// ModelAnnoDocPair retrieves the pair of elements from the slice.
func (p *ModelAnnoDocPairWiseIterator) ModelAnnoDocPair() (*model.AnnoDoc, *model.AnnoDoc) {
	return p.slice[p.firstIdx], p.slice[p.secondIdx]
}
