package kafkabus

import (
	"context"

	platformevents "github.com/alvor-technologies/iag-platform-go/events"
)

const (
	Topic  = "iag.supply-chain"
	Source = "iag.traceability"
)

// Publisher emits selected domain events from iag-traceability.
type Publisher struct {
	producer *platformevents.Producer
}

func NewPublisher(brokers []string, clientID string) *Publisher {
	if len(brokers) == 0 || (len(brokers) == 1 && brokers[0] == "") {
		return &Publisher{}
	}
	return &Publisher{
		producer: platformevents.NewProducer(platformevents.ProducerConfig{
			Brokers:  brokers,
			ClientID: clientID,
		}),
	}
}

func (p *Publisher) Enabled() bool { return p != nil && p.producer != nil }

func (p *Publisher) EmitLotQRPublished(ctx context.Context, lotBusinessID, token, publicURL string) error {
	if !p.Enabled() {
		return nil
	}
	env := platformevents.NewEnvelope(Source, "scm.lot.qr_published", map[string]any{
		"lot_business_id": lotBusinessID,
		"public_token":    token,
		"public_url":      publicURL,
	})
	return p.producer.Publish(ctx, Topic, lotBusinessID, env)
}
