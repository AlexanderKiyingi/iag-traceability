package anchor

import (
	"context"
	"fmt"
	"strings"
)

// AnchorProvider posts an L2 Merkle root to an external (public or permissioned)
// ledger for L3 anchoring, and can later confirm a posted root. The network is a
// deployment decision — the interface stays stable so Polygon, Celo, a
// Hyperledger consortium, or "none" are all pluggable without touching the
// anchor service.
//
// Providers post ONLY the Merkle root and small metadata — never PII or per-kg
// custody rows.
type AnchorProvider interface {
	// Name is the stored provider tag (matches anchor_batches.provider).
	Name() string
	// PublishRoot posts root and returns a ledger tx reference.
	PublishRoot(ctx context.Context, root []byte, meta AnchorMeta) (txRef string, err error)
	// VerifyRoot confirms txRef anchors root (best-effort; may be a no-op).
	VerifyRoot(ctx context.Context, txRef string, root []byte) (bool, error)
}

// AnchorMeta is the small, non-sensitive payload posted alongside a root.
type AnchorMeta struct {
	Schema     string `json:"schema"` // e.g. "iag.custody.anchor.v1"
	EventCount int    `json:"event_count"`
	FromSeq    int64  `json:"from_seq"`
	ToSeq      int64  `json:"to_seq"`
	PeriodEnd  string `json:"period_end"`
	KeyID      string `json:"platform_key_id"`
}

// NewProvider selects an L3 provider by name. "none" (the default) disables
// public anchoring — L2 notarization still works fully.
func NewProvider(name, rpcURL, contractOrWallet string) (AnchorProvider, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "none":
		return noopProvider{}, nil
	case "polygon":
		return newPolygonProvider(rpcURL, contractOrWallet)
	default:
		return nil, fmt.Errorf("unknown ANCHOR_PROVIDER %q", name)
	}
}

// noopProvider is the default: L3 disabled. It never publishes, so anchor rows
// keep provider="none" and tx_ref NULL.
type noopProvider struct{}

func (noopProvider) Name() string { return "none" }
func (noopProvider) PublishRoot(context.Context, []byte, AnchorMeta) (string, error) {
	return "", ErrProviderDisabled
}
func (noopProvider) VerifyRoot(context.Context, string, []byte) (bool, error) {
	return false, ErrProviderDisabled
}

// ErrProviderDisabled signals that no L3 provider is configured; the anchor
// service treats it as "L2 only" rather than a failure.
var ErrProviderDisabled = fmt.Errorf("anchor: public provider disabled")
