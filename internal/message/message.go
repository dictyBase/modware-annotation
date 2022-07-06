package message

import (
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
)

// Publisher manages publishing of message.
type Publisher interface {
	// Publis publishes the annotation object using the given subject
	Publish(subject string, ann *annotation.TaggedAnnotation) error
	// Close closes the connection to the underlying messaging server
	Close() error
}
