package story

import (
	"context"
	"strings"

	"iag-traceability/backend/internal/store"
)

// ValidateLotPublish blocks QR publish using SCM compliance gate when configured.
func ValidateLotPublish(ctx context.Context, st *store.Store, lotBusinessID string) error {
	if scmFetcher != nil && scmFetcher.Enabled() {
		if err := scmFetcher.ValidateLotPublish(ctx, lotBusinessID); err != nil {
			if isSCMTransportError(err) {
				return ErrComplianceGateUnavailable
			}
			return err
		}
		return nil
	}
	hasCoA, err := st.LotHasCOA(ctx, lotBusinessID)
	if err != nil {
		return err
	}
	if hasCoA {
		return nil
	}
	return ErrCoARequired
}

func isSCMTransportError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "scm 5") ||
		strings.Contains(msg, "scm 502") ||
		strings.Contains(msg, "scm 503")
}
