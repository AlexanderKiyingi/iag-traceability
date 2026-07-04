package kafkabus

import (
	"context"
	"encoding/json"

	platformevents "github.com/alvor-technologies/iag-platform-go/events"

	"iag-traceability/backend/internal/store"
)

const (
	Topic  = "iag.supply-chain"
	Source = "iag.traceability"
)

// Publisher sends domain-event envelopes to Kafka. Emission is now routed
// through the transactional outbox (see store.OutboxRow + Relay): callers
// enqueue an OutboxRow atomically with their state change, and the Relay uses
// this Publisher to deliver rows to the bus with retries. This decouples event
// durability from broker availability at request time.
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

// PublishEnvelope delivers a pre-built envelope to the bus. Used by the Relay
// to drain the outbox. A no-op (nil) when Kafka is not configured.
func (p *Publisher) PublishEnvelope(ctx context.Context, topic, key string, env platformevents.Envelope) error {
	if !p.Enabled() {
		return nil
	}
	return p.producer.Publish(ctx, topic, key, env)
}

// ---- Outbox row builders -------------------------------------------------
// Each builds the CloudEvents envelope and serializes it into an OutboxRow so
// the event id (used for downstream dedupe) is fixed at enqueue time and
// preserved across relay retries.

func outboxRow(env platformevents.Envelope, key string) store.OutboxRow {
	payload, _ := json.Marshal(env)
	return store.OutboxRow{
		EventID: env.ID,
		Topic:   Topic,
		Key:     key,
		Subject: env.Type,
		Payload: payload,
	}
}

func LotQRPublishedRow(lotBusinessID, token, publicURL string) store.OutboxRow {
	return outboxRow(platformevents.NewEnvelope(Source, "scm.lot.qr_published", map[string]any{
		"lot_business_id": lotBusinessID,
		"public_token":    token,
		"public_url":      publicURL,
	}), lotBusinessID)
}

func LotQRRevokedRow(lotBusinessID, token string) store.OutboxRow {
	return outboxRow(platformevents.NewEnvelope(Source, "scm.lot.qr_revoked", map[string]any{
		"lot_business_id": lotBusinessID,
		"public_token":    token,
	}), lotBusinessID)
}

func LotStoryUpdatedRow(lotBusinessID string) store.OutboxRow {
	return outboxRow(platformevents.NewEnvelope(Source, "scm.lot.story_updated", map[string]any{
		"lot_business_id": lotBusinessID,
	}), lotBusinessID)
}

// OutboxEmitter enqueues story-updated events to the transactional outbox,
// satisfying consumer.StoryPublisher so the Kafka consumer's projections emit
// durably rather than via a fire-and-forget dual-write.
type OutboxEmitter struct {
	st *store.Store
}

func NewOutboxEmitter(st *store.Store) *OutboxEmitter { return &OutboxEmitter{st: st} }

func (e *OutboxEmitter) EmitLotStoryUpdated(ctx context.Context, lotBusinessID string) error {
	return e.st.EnqueueOutbox(ctx, LotStoryUpdatedRow(lotBusinessID))
}
