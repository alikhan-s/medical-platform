package event

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type EventPublisher interface {
	Publish(ctx context.Context, subject string, payload any) error
}

type natsPublisher struct {
	conn *nats.Conn
}

func NewNATSPublisher(conn *nats.Conn) EventPublisher {
	return &natsPublisher{conn: conn}
}

func (p *natsPublisher) Publish(_ context.Context, subject string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event for subject %q: %w", subject, err)
	}
	return p.conn.Publish(subject, data)
}

type noopPublisher struct{}

func NewNoopPublisher() EventPublisher { return &noopPublisher{} }

func (p *noopPublisher) Publish(_ context.Context, subject string, _ any) error {
	log.Printf("WARN [event]: NATS unavailable — skipping publish on subject %q", subject)
	return nil
}
