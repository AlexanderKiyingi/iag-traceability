package kafkabus

import (
	"context"
	"encoding/json"
	"log"
	"time"

	platformevents "github.com/alvor-technologies/iag-platform-go/events"

	"iag-traceability/backend/internal/store"
)

// Relay drains the transactional outbox to Kafka. It runs only when Kafka is
// configured; rows accumulate durably in Postgres until then. Delivery
// preserves per-row order and is at-least-once — the CloudEvents id in each
// envelope lets downstream consumers dedupe a re-published row.
type Relay struct {
	st       *store.Store
	pub      *Publisher
	interval time.Duration
	batch    int
}

func NewRelay(st *store.Store, pub *Publisher) *Relay {
	return &Relay{st: st, pub: pub, interval: 5 * time.Second, batch: 100}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	// Drain once at start so events enqueued while the relay was down go out
	// promptly rather than waiting a full tick.
	r.drain(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.drain(ctx)
		}
	}
}

func (r *Relay) drain(ctx context.Context) {
	rows, err := r.st.FetchUnpublishedOutbox(ctx, r.batch)
	if err != nil {
		log.Printf("outbox relay: fetch failed: %v", err)
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		var env platformevents.Envelope
		if err := json.Unmarshal(row.Payload, &env); err != nil {
			// Unparseable row would block the queue forever — mark it delivered
			// and log loudly so it is investigated rather than silently retried.
			log.Printf("outbox relay: dropping unparseable row id=%d: %v", row.ID, err)
			_ = r.st.MarkOutboxPublished(ctx, row.ID)
			continue
		}
		if err := r.pub.PublishEnvelope(ctx, row.Topic, row.Key, env); err != nil {
			// Stop this pass to preserve ordering; the row (and everything after
			// it) is retried on the next tick.
			_ = r.st.MarkOutboxFailed(ctx, row.ID, err.Error())
			return
		}
		if err := r.st.MarkOutboxPublished(ctx, row.ID); err != nil {
			// Published but not marked: stop so we don't churn. A redelivery on
			// the next tick is deduped downstream by the envelope id.
			log.Printf("outbox relay: mark-published failed id=%d: %v", row.ID, err)
			return
		}
	}
}
