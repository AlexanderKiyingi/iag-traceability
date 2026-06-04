package story

import (
	"context"

	"iag-traceability/backend/internal/store"
)

// ValidateLotPublish blocks QR publish when CoA evidence is missing from events/SCM snapshot.
func ValidateLotPublish(ctx context.Context, st *store.Store, lotBusinessID string) error {
	if scmFetcher != nil && scmFetcher.Enabled() {
		if lot, err := scmFetcher.GetExportLot(ctx, lotBusinessID); err == nil {
			if lot.CoaNumber != nil && *lot.CoaNumber != "" {
				return nil
			}
		}
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
