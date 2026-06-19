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

// EmitLotStoryUpdated announces that a lot's composed story projection was
// rebuilt, so downstream caches/CDNs can invalidate on content change (not just
// on publish/revoke).
func (p *Publisher) EmitLotStoryUpdated(ctx context.Context, lotBusinessID string) error {
	if !p.Enabled() {
		return nil
	}
	env := platformevents.NewEnvelope(Source, "scm.lot.story_updated", map[string]any{
		"lot_business_id": lotBusinessID,
	})
	return p.producer.Publish(ctx, Topic, lotBusinessID, env)
}

// EmitLotQRRevoked announces that a previously published QR has been revoked,
// so downstream consumers (and any caches/CDNs keyed off the event stream)
// can react. Previously revoke was silent on the bus.
func (p *Publisher) EmitLotQRRevoked(ctx context.Context, lotBusinessID, token string) error {
	if !p.Enabled() {
		return nil
	}
	env := platformevents.NewEnvelope(Source, "scm.lot.qr_revoked", map[string]any{
		"lot_business_id": lotBusinessID,
		"public_token":    token,
	})
	return p.producer.Publish(ctx, Topic, lotBusinessID, env)
}
