package story

import (
	"context"
	"log"

	"iag-traceability/backend/internal/kafkabus"
	"iag-traceability/backend/internal/store"
)

// AfterAppend is what follows a newly appended trace event on every write
// path that is not the Kafka consumer (POST /events, the SCM sync): when the
// event affects a lot's story, recompose and persist that lot's projection and
// enqueue scm.lot.story_updated to the outbox. Best-effort — failures are
// logged, never returned, so they cannot fail the write that triggered them.
func AfterAppend(ctx context.Context, st *store.Store, eventType, entityType, entityBusinessID string, related map[string]any) {
	if !AffectsLotStory(eventType) {
		return
	}
	lotID := ""
	if entityType == "lot" {
		lotID = entityBusinessID
	} else if v, ok := related["lot_business_id"].(string); ok {
		lotID = v
	}
	if lotID == "" {
		return
	}
	if ok, err := RebuildStoredLot(ctx, st, lotID, ""); err == nil && ok {
		// Durable emit: the relay delivers it to Kafka with retries.
		if emitErr := st.EnqueueOutbox(ctx, kafkabus.LotStoryUpdatedRow(lotID)); emitErr != nil {
			log.Printf("traceability: enqueue story_updated lot=%s: %v", lotID, emitErr)
		}
	}
}
