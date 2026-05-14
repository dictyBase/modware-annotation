// Package message defines publisher interfaces for annotation messaging.
package message

import (
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
)

// Publisher manages publishing of message.
type Publisher interface {
	// Publis publishes the annotation object using the given subject
	Publish(subject string, ann *annotation.TaggedAnnotation) error
	// Close closes the connection to the underlying messaging server
	Close() error
}

// FeatureAnnotationPublisher manages publishing of feature annotation messages.
type FeatureAnnotationPublisher interface {
	// Publish publishes the feature annotation object using the given subject
	Publish(subject string, ann *feature.FeatureAnnotation) error
	// Close closes the connection to the underlying messaging server
	Close() error
}
