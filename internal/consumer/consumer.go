package consumer

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"iag-traceability/backend/internal/metrics"
	"iag-traceability/backend/internal/scmclient"
	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/story"
)

// Config for Kafka consumers.
type Config struct {
	Brokers          []string
	GroupID          string
	SupplyChainTopic string
	ProductionTopic  string
	QualityTopic     string
}

// StoryPublisher emits the lot story-updated event. Satisfied by
// *kafkabus.Publisher; an interface here keeps the consumer decoupled/testable.
type StoryPublisher interface {
	EmitLotStoryUpdated(ctx context.Context, lotBusinessID string) error
}

type Consumer struct {
	cfg     Config
	store   *store.Store
	scm     *scmclient.Client
	metrics *metrics.Counters
	pub     StoryPublisher
}

func New(cfg Config, st *store.Store, scm *scmclient.Client, m *metrics.Counters, pub StoryPublisher) *Consumer {
	if m == nil {
		m = metrics.New()
	}
	return &Consumer{cfg: cfg, store: st, scm: scm, metrics: m, pub: pub}
}

func (c *Consumer) Run(ctx context.Context) error {
	if len(c.cfg.Brokers) == 0 {
		log.Printf("traceability consumer: KAFKA_BROKERS unset — skipping")
		return nil
	}
	topics := uniqueTopics(c.cfg.SupplyChainTopic, c.cfg.ProductionTopic, c.cfg.QualityTopic)
	if len(topics) == 0 {
		return nil
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.cfg.Brokers,
		GroupID:     c.cfg.GroupID,
		GroupTopics: topics,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer r.Close()

	log.Printf("traceability consumer: listening on %v (group=%s)", topics, c.cfg.GroupID)
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("traceability consumer fetch: %v", err)
			continue
		}
		if err := c.handleMessage(ctx, msg); err != nil {
			c.metrics.IncFailed()
			log.Printf("traceability consumer handle topic=%s: %v", msg.Topic, err)
			continue
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("traceability consumer commit: %v", err)
		}
	}
}

type cloudEvent struct {
	ID   string         `json:"id"`
	Type string         `json:"type"`
	Time string         `json:"time"`
	Data map[string]any `json:"data"`
}

func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var env cloudEvent
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return err
	}
	eventID := env.ID
	if eventID == "" {
		eventID = string(msg.Key)
	}
	if eventID == "" {
		eventID = string(msg.Value)
		if len(eventID) > 128 {
			eventID = eventID[:128]
		}
	}

	// Dedupe is a read-then-mark, not an insert-up-front: we only record the
	// event as seen AFTER projection succeeds. Inserting up front meant a
	// projection failure (which doesn't commit the Kafka offset) would be
	// permanently skipped on redelivery because the dedupe row already
	// existed. The single consumer-per-partition guarantee makes the
	// check-then-act safe; AppendEvent's idempotency key is the backstop.
	var seen bool
	if err := c.store.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM kafka_dedupe WHERE event_id = $1)`, eventID,
	).Scan(&seen); err != nil {
		return err
	}
	if seen {
		c.metrics.IncDeduped()
		return nil
	}
	c.metrics.IncConsumed()

	eventType := env.Type
	if eventType == "" {
		eventType = string(msg.Key)
	}
	if err := c.projectEvent(ctx, eventID, eventType, msg.Topic, env.Data); err != nil {
		return err
	}

	_, err := c.store.Pool().Exec(ctx, `
		INSERT INTO kafka_dedupe (event_id, topic) VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING`, eventID, msg.Topic)
	return err
}

func (c *Consumer) projectEvent(ctx context.Context, eventID, eventType, topic string, data map[string]any) error {
	if data == nil {
		data = map[string]any{}
	}
	occurred := time.Now().UTC()
	mappedType, entityType, entityID := mapEvent(eventType, data)
	if mappedType == "" {
		// Unrecognized event type: dead-letter it instead of silently
		// dropping, so upstream contract drift is visible. Returning nil
		// lets the caller mark it deduped (no infinite reprocessing).
		log.Printf("traceability consumer: unmapped event type=%q topic=%s — dead-lettering", eventType, topic)
		c.deadLetter(ctx, eventID, topic, eventType, "unmapped_event_type", data)
		return nil
	}
	idemKey := eventID
	_, err := c.store.AppendEvent(ctx, store.AppendEventInput{
		OccurredAt:       occurred,
		EventType:        mappedType,
		EntityType:       entityType,
		EntityBusinessID: entityID,
		SourceService:    sourceFor(eventType),
		RelatedIDs:       data,
		Payload:          data,
		IdempotencyKey:   &idemKey,
	})
	if err != nil {
		return err
	}
	c.metrics.IncProjected()
	c.projectEntity(ctx, eventType, mappedType, entityID, data)
	if lotID, ok := strField(data, "lot_business_id"); ok && lotID != "" {
		c.maybeRebuildLotStory(ctx, lotID, data, mappedType)
	} else if bid, ok := strField(data, "batch_business_id"); ok && bid != "" {
		c.maybeRebuildLotsForBatch(ctx, bid, data, mappedType)
	}
	return nil
}

func (c *Consumer) maybeRebuildLotsForBatch(ctx context.Context, batchID string, data map[string]any, mappedType string) {
	switch mappedType {
	case "WET_MILL_STARTED", "WET_MILL_COMPLETE", "DRYING_STARTED", "DRYING_COMPLETE",
		"DRY_MILL_COMPLETE", "SAMPLE_SUBMITTED", "LAB_RESULT_RECORDED", "CHERRY_RECEIVED",
		"STAGE_CHANGED":
	default:
		return
	}
	if c.scm == nil || !c.scm.Enabled() {
		return
	}
	lotIDs, err := c.scm.ListLotsForBatch(ctx, batchID)
	if err != nil || len(lotIDs) == 0 {
		return
	}
	for _, lotID := range lotIDs {
		c.maybeRebuildLotStory(ctx, lotID, data, mappedType)
	}
}

func (c *Consumer) projectEntity(ctx context.Context, eventType, mappedType, entityID string, data map[string]any) {
	if entityID == "" {
		return
	}
	payload := map[string]any{}
	for k, v := range data {
		payload[k] = v
	}
	switch eventType {
	case "scm.party.created", "scm.party.updated":
		_ = c.store.UpsertEntityProjection(ctx, "party", entityID, payload)
	case "scm.farm.registered", "scm.farm.updated":
		_ = c.store.UpsertEntityProjection(ctx, "farm", entityID, payload)
	case "scm.lot.assembled":
		if mappedType == "LOT_ASSEMBLED" {
			_ = c.store.UpsertEntityProjection(ctx, "lot", entityID, payload)
		}
	}
}

func (c *Consumer) maybeRebuildLotStory(ctx context.Context, lotID string, data map[string]any, mappedType string) {
	if !story.AffectsLotStory(mappedType) {
		return
	}
	publicURL := ""
	if u, ok := strField(data, "public_url"); ok {
		publicURL = u
	}
	if composed, err := story.RebuildLotProjection(ctx, c.store, c.scm, lotID, publicURL); err == nil {
		if err := c.store.UpsertStoryProjection(ctx, lotID, composed); err == nil && c.pub != nil {
			if emitErr := c.pub.EmitLotStoryUpdated(ctx, lotID); emitErr != nil {
				log.Printf("traceability consumer: emit story_updated lot=%s: %v", lotID, emitErr)
			}
		}
	}
}

func mapEvent(eventType string, data map[string]any) (mappedType, entityType, entityID string) {
	switch eventType {
	case "scm.batch.stage_changed":
		bid, _ := strField(data, "batch_business_id")
		return "STAGE_CHANGED", "batch", bid
	case "scm.lot.qr_published":
		lid, _ := strField(data, "lot_business_id")
		return "LOT_QR_PUBLISHED", "lot", lid
	case "scm.intake.received":
		bid, _ := strField(data, "batch_business_id")
		return "CHERRY_RECEIVED", "batch", bid
	case "scm.farm.registered":
		fid, _ := strField(data, "farm_business_id")
		return "FARM_REGISTERED", "farm", fid
	case "scm.party.created":
		pid, _ := strField(data, "party_business_id")
		return "PARTY_CREATED", "party", pid
	case "scm.party.updated":
		pid, _ := strField(data, "party_business_id")
		return "PARTY_UPDATED", "party", pid
	case "scm.farm.updated":
		fid, _ := strField(data, "farm_business_id")
		return "FARM_UPDATED", "farm", fid
	case "scm.lot.assembled":
		lid, _ := strField(data, "lot_business_id")
		return "LOT_ASSEMBLED", "lot", lid
	case "mes.wetmill.started":
		bid, _ := strField(data, "batch_business_id")
		return "WET_MILL_STARTED", "batch", bid
	case "mes.drying.started":
		bid, _ := strField(data, "batch_business_id")
		return "DRYING_STARTED", "batch", bid
	case "mes.wetmill.completed":
		bid, _ := strField(data, "batch_business_id")
		return "WET_MILL_COMPLETE", "batch", bid
	case "mes.drying.completed":
		bid, _ := strField(data, "batch_business_id")
		return "DRYING_COMPLETE", "batch", bid
	case "mes.drymill.completed":
		bid, _ := strField(data, "batch_business_id")
		return "DRY_MILL_COMPLETE", "batch", bid
	case "qc.lab.result_recorded":
		bid, _ := strField(data, "batch_business_id")
		return "LAB_RESULT_RECORDED", "batch", bid
	case "qc.sample.submitted":
		bid, _ := strField(data, "batch_business_id")
		return "SAMPLE_SUBMITTED", "batch", bid
	case "qc.coa.issued":
		lid, _ := strField(data, "lot_business_id")
		return "COA_ISSUED", "lot", lid
	default:
		return "", "", ""
	}
}

// deadLetter records an event the consumer could not handle. Best-effort:
// a dead-letter write failure must not block the consumer, so the error is
// logged and swallowed.
func (c *Consumer) deadLetter(ctx context.Context, eventID, topic, eventType, reason string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	// ON CONFLICT keeps the table free of duplicates if the same event is
	// redelivered before its kafka_dedupe row commits (e.g. a crash between
	// the dead-letter write and the dedupe mark). Matches the partial unique
	// index on non-empty event_id in migration 004.
	if _, err := c.store.Pool().Exec(ctx, `
		INSERT INTO kafka_dead_letter (event_id, topic, event_type, reason, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) WHERE event_id <> '' DO NOTHING`, eventID, topic, eventType, reason, data); err != nil {
		log.Printf("traceability consumer: dead-letter write failed event_id=%s: %v", eventID, err)
	}
	c.metrics.IncDeadLettered()
}

func sourceFor(eventType string) string {
	switch {
	case strings.HasPrefix(eventType, "mes."):
		return "iag-mes"
	case strings.HasPrefix(eventType, "qc."):
		return "iag-quality-control"
	default:
		return "iag-supply-chain"
	}
}

func strField(data map[string]any, key string) (string, bool) {
	v, ok := data[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok && s != ""
}

func uniqueTopics(parts ...string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}
