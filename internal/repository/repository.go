package repository

import (
	"context"
	"io"

	IOE "github.com/IBM/fp-go/v2/ioeither"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-obograph/storage"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// ListAnnotationsParams defines the parameters for listing annotations.
type ListAnnotationsParams struct {
	Cursor int64
	Limit  int64  `validate:"required"`
	Filter string `validate:"required"`
}

// AutocompleteQuery holds the input of an annotation autocomplete search.
// The repository normalizes Query and clamps Limit.
type AutocompleteQuery struct {
	// Query is the partial search text. The caller sends it untrimmed;
	// the repository trims it, lowercases it, and strips combining marks.
	Query string
	// Limit is the maximum number of suggestions. A value at or below 0
	// becomes 5. A value above 50 becomes 50.
	Limit int
}

// Suggestion is one annotation autocomplete match, deduplicated by EntryID.
type Suggestion struct {
	// EntryID is the strain identifier of the matched annotation, for
	// example DBS0236126.
	EntryID string
	// Field is the tag label that matched: "synonym" or "genotype".
	Field string
	// DisplayText is the stored annotation value shown in the user interface.
	DisplayText string
	// Score ranks the match. A prefix match scores above a fuzzy match.
	Score float64
}

// FullSearchQuery holds the input of an annotation full search.
// The repository normalizes Query, rejects all-stopword queries, and clamps Limit.
type FullSearchQuery struct {
	// Query is the search text. The caller sends it untrimmed;
	// the repository trims it, lowercases it, and strips combining marks.
	Query string
	// Limit is the maximum number of results. A value at or below 0
	// becomes 50. A value above 50 becomes 50.
	Limit int
}

// FullSearchResult is one annotation full search match, deduplicated by EntryID.
type FullSearchResult struct {
	// EntryID is the strain identifier of the matched annotation, for
	// example DBS0236126.
	EntryID string
	// Field is the canonical field label that matched (for example,
	// "synonym", "genotype", "phenotype", or "public_note").
	Field string
	// DisplayText is the stored annotation value or cvterm label shown
	// in the user interface.
	DisplayText string
	// Score ranks the match across the four bands (prefix >= 1000,
	// phrase >= 500, token >= 250, fuzzy >= 0).
	Score float64
}

// TaggedAnnotationRepository is an interface for accessing annotation
// data from its data sources.
type TaggedAnnotationRepository interface {
	// GetAnnotationById retrieves an annotation
	GetAnnotationByID(id string) (*model.AnnoDoc, error)
	GetAnnotationByEntry(
		req *annotation.EntryAnnotationRequest,
	) (*model.AnnoDoc, error)
	AddAnnotation(na *annotation.NewTaggedAnnotation) (*model.AnnoDoc, error)
	EditAnnotation(
		ua *annotation.TaggedAnnotationUpdate,
	) (*model.AnnoDoc, error)
	RemoveAnnotation(id string, purge bool) error
	// ListAnnotationGroup provides a paginated list of annotation along
	// with optional filtering
	ListAnnotations(ctx context.Context, params *ListAnnotationsParams) ([]*model.AnnoDoc, error)
	ClearAnnotations() error
	Clear() error
	// AddAnnotationGroup creates a new annotation group
	AddAnnotationGroup(idslice ...string) (*model.AnnoGroup, error)
	// GetAnnotationGroup retrieves an annotation group
	GetAnnotationGroup(groupID string) (*model.AnnoGroup, error)
	// AppendToAnnotationGroup adds new annotations to an existing group
	AppendToAnnotationGroup(
		groupID string,
		idslice ...string,
	) (*model.AnnoGroup, error)
	// DeleteAnnotationGroup deletes an annotation group
	RemoveAnnotationGroup(groupID string) error
	// RemoveFromAnnotationGroup remove annotations from an existing group
	RemoveFromAnnotationGroup(
		groupID string,
		idslice ...string,
	) (*model.AnnoGroup, error)
	// ListAnnotationGroup provides a paginated list of annotation groups along
	// with optional filtering
	ListAnnotationGroup(
		ctx context.Context,
		cursor, limit int64,
		filter string,
	) ([]*model.AnnoGroup, error)
	// GetAnnotationTag retrieves tag information
	GetAnnotationTag(name, ontology string) (*model.AnnoTag, error)
	// AutocompleteAnnotation returns at most Limit short suggestions for
	// the normalized query of params as a lazy IOEither pipeline.
	AutocompleteAnnotation(
		ctx context.Context,
		params *AutocompleteQuery,
	) IOE.IOEither[error, []*Suggestion]
	// SearchAnnotation returns at most Limit ranked results for the
	// normalized query of params as a lazy IOEither pipeline.
	SearchAnnotation(
		ctx context.Context,
		params *FullSearchQuery,
	) IOE.IOEither[error, []*FullSearchResult]
	Dbh() *manager.Database
	LoadOboJSON(r io.Reader) (*storage.UploadInformation, error)
}
