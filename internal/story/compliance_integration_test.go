package story

import (
	"context"
	"errors"
	"testing"

	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/testdb"
)

// TestValidateLotPublish_Integration exercises the local (SCM-disabled) CoA
// gate against a real database. scmFetcher is nil here, so ValidateLotPublish
// falls through to store.LotHasCOA.
func TestValidateLotPublish_Integration(t *testing.T) {
	pool := testdb.Pool(t)
	st := store.New(pool)
	ctx := context.Background()

	// No CoA recorded → publish blocked.
	if err := ValidateLotPublish(ctx, st, "LOT-NONE"); !errors.Is(err, ErrCoARequired) {
		t.Fatalf("no-CoA lot: got %v, want ErrCoARequired", err)
	}

	// CoA projected directly onto the lot entity → allowed.
	mustAppend(t, st, store.AppendEventInput{
		EventType: "COA_ISSUED", EntityType: "lot", EntityBusinessID: "LOT-1",
	})
	if err := ValidateLotPublish(ctx, st, "LOT-1"); err != nil {
		t.Fatalf("lot-entity CoA: got %v, want nil", err)
	}

	// CoA recorded on a BATCH entity but referencing the lot via related_ids —
	// the case the LotHasCOA fix added coverage for.
	mustAppend(t, st, store.AppendEventInput{
		EventType: "COA_ISSUED", EntityType: "batch", EntityBusinessID: "BAT-9",
		RelatedIDs: map[string]any{"lot_business_id": "LOT-2"},
	})
	if err := ValidateLotPublish(ctx, st, "LOT-2"); err != nil {
		t.Fatalf("related-ids CoA: got %v, want nil", err)
	}

	// A different lot with no CoA is still blocked (no cross-contamination).
	if err := ValidateLotPublish(ctx, st, "LOT-3"); !errors.Is(err, ErrCoARequired) {
		t.Fatalf("unrelated lot: got %v, want ErrCoARequired", err)
	}
}

func mustAppend(t *testing.T, st *store.Store, in store.AppendEventInput) {
	t.Helper()
	if _, err := st.AppendEvent(context.Background(), in); err != nil {
		t.Fatalf("append %s/%s: %v", in.EntityType, in.EntityBusinessID, err)
	}
}
