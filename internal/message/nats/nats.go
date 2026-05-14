// Package nats provides NATS messaging publisher implementations.
//
//nolint:dupl
package nats

import (
	"encoding/json"
	"fmt"

	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/message"
	gnats "github.com/nats-io/nats.go"
)

type natsPublisher struct {
	conn *gnats.Conn
}

// NewPublisher creates a new NATS-backed annotation message publisher.
func NewPublisher(
	host, port string,
	options ...gnats.Option,
) (message.Publisher, error) {
	nconn, err := gnats.Connect(
		fmt.Sprintf("nats://%s:%s", host, port),
		options...)
	if err != nil {
		return &natsPublisher{}, fmt.Errorf(
			"error in connecting to nats server %s",
			err,
		)
	}

	return &natsPublisher{conn: nconn}, nil
}

func (n *natsPublisher) Publish(
	subj string,
	ann *annotation.TaggedAnnotation,
) error {
	data, err := json.Marshal(ann)
	if err != nil {
		return fmt.Errorf("error in marshaling annotation %s", err)
	}
	if err := n.conn.Publish(subj, data); err != nil {
		return fmt.Errorf("error in publishing through nats %s", err)
	}

	return nil
}

func (n *natsPublisher) Close() error {
	n.conn.Close()

	return nil
}
