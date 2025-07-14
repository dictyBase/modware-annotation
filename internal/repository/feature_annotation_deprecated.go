package repository

import (
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// DeprecatedFeatureAnnotationRepository contains deprecated methods during transition period.
type DeprecatedFeatureAnnotationRepository interface {
	//nolint:staticcheck // SA1019: Interface methods for deprecated functionality during transition period
	UpdateTag(
		req *feature.UpdateTagRequest,
	) (*model.FeatureAnnotationDoc, error)
	//nolint:staticcheck // SA1019: Interface methods for deprecated functionality during transition period
	RemoveTag(
		req *feature.RemoveTagRequest,
	) error
}
