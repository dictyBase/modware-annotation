//nolint:dupl
package nats

import (
	"encoding/json"
	"fmt"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/message"
	gnats "github.com/nats-io/nats.go"
)

type featureAnnotationPublisher struct {
	conn *gnats.Conn
}

// NewFeatureAnnotationPublisher creates a new NATS-backed feature annotation message publisher.
func NewFeatureAnnotationPublisher(
	host, port string,
	options ...gnats.Option,
) (message.FeatureAnnotationPublisher, error) {
	nconn, err := gnats.Connect(
		fmt.Sprintf("nats://%s:%s", host, port),
		options...)
	if err != nil {
		return &featureAnnotationPublisher{}, fmt.Errorf(
			"error in connecting to nats server %s",
			err,
		)
	}

	return &featureAnnotationPublisher{conn: nconn}, nil
}

func (fnp *featureAnnotationPublisher) Publish(
	subj string,
	fann *feature.FeatureAnnotation,
) error {
	data, err := json.Marshal(fann)
	if err != nil {
		return fmt.Errorf("error in marshaling feature annotation %s", err)
	}
	if err := fnp.conn.Publish(subj, data); err != nil {
		return fmt.Errorf("error in publishing through nats %s", err)
	}

	return nil
}

func (fnp *featureAnnotationPublisher) Close() error {
	fnp.conn.Close()

	return nil
}
