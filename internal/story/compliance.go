package story

import (
	"context"

	"iag-traceability/backend/internal/store"
)

// ValidateLotPublish blocks QR publish using SCM compliance gate when configured.
func ValidateLotPublish(ctx context.Context, st *store.Store, lotBusinessID string) error {
	if scmFetcher != nil && scmFetcher.Enabled() {
		return scmFetcher.ValidateLotPublish(ctx, lotBusinessID)
	}
	events, err := st.ListEventsForEntity(ctx, "lot", lotBusinessID, 50)
	if err != nil {
		return err
	}
	for _, ev := range events {
		if ev.EventType == "COA_ISSUED" || ev.EventType == "qc.coa.issued" {
			return nil
		}
	}
	return ErrCoARequired
}
