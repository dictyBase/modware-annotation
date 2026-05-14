package arangodb

import (
	"context"
	"fmt"
	"slices"
	"time"

	driver "github.com/arangodb/go-driver"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
)

//nolint:staticcheck // SA1019: Implementation for deprecated functionality during transition period
func (fann *featureAnnoRepo) UpdateTag(
	req *feature.UpdateTagRequest,
) (*model.FeatureAnnotationDoc, error) {
	//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
	doc, err := fann.GetFeatureAnnotation(req.Id)
	if err != nil {
		return nil, err
	}

	// Find tag index using IndexFunc
	//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
	idx := slices.IndexFunc(doc.Properties, findTagPredicate(req.Tag.Tag))
	if idx == -1 {
		//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
		return nil, fmt.Errorf("tag %s not found", req.Tag.Tag)
	}

	// Create updated properties slice
	newProps := make([]model.TagPropertyDoc, len(doc.Properties))
	copy(newProps, doc.Properties)
	updatedAt := time.Now()
	//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
	if req.Tag.UpdatedAt.IsValid() {
		//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
		updatedAt = req.Tag.UpdatedAt.AsTime()
	}
	newProps[idx] = model.TagPropertyDoc{
		//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
		Tag: req.Tag.Tag,
		//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
		Value:     req.Tag.Value,
		CreatedBy: doc.Properties[idx].CreatedBy, // Preserve original creator
		CreatedAt: doc.Properties[idx].CreatedAt, // Preserve creation time
		//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
		UpdatedBy: req.Tag.UpdatedBy,
		UpdatedAt: updatedAt,
	}

	// Perform partial update and return new document
	newDoc := &model.FeatureAnnotationDoc{}
	ctx := driver.WithReturnNew(context.Background(), newDoc)
	meta, err := fann.feature.UpdateDocument(
		ctx,
		doc.Key,
		map[string]any{"properties": newProps},
	)
	if err != nil {
		return nil, fmt.Errorf("error updating tag: %w", err)
	}
	newDoc.DocumentMeta = meta

	return newDoc, nil
}

//nolint:staticcheck // SA1019: Implementation for deprecated functionality during transition period
func (fann *featureAnnoRepo) RemoveTag(
	req *feature.RemoveTagRequest,
) error {
	//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
	doc, err := fann.GetFeatureAnnotation(req.Id)
	if err != nil {
		return err
	}
	// Find tag index using IndexFunc
	//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
	idx := slices.IndexFunc(doc.Properties, findTagPredicate(req.Tag))
	if idx == -1 {
		//nolint:staticcheck // SA1019: Accessing deprecated field during transition period
		return fmt.Errorf("tag %s not found", req.Tag)
	}

	// Create updated properties using Delete
	newProps := slices.Delete(doc.Properties, idx, idx+1)

	_, err = fann.feature.UpdateDocument(
		context.Background(),
		doc.Key,
		map[string]any{"properties": newProps},
	)
	if err != nil {
		return fmt.Errorf("error removing tag: %w", err)
	}

	return nil
}

func findTagPredicate(tag string) func(p model.TagPropertyDoc) bool {
	return func(p model.TagPropertyDoc) bool {
		return p.Tag == tag
	}
}
