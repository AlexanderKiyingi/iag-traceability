package anchor

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
)

// polygonProvider is the L3 adapter shell for an EVM chain (Polygon PoS by
// default). It is intentionally a thin, dependency-free scaffold: wiring a real
// signed-transaction path pulls in an Ethereum client + hot-wallet key custody,
// which is a deliberate, legal-reviewed step (see COFFEE_BLOCKCHAIN_TRACEABILITY
// §6/§10). Until that wiring lands, PublishRoot returns ErrNotImplemented so the
// anchor service records a valid L2 notarization and leaves provider/tx_ref
// unset rather than silently claiming an on-chain anchor that does not exist.
//
// The contract we intend to call is a trivial notary: store(bytes32 root) that
// emits an event; only the 32-byte root and small metadata ever leave the
// platform — never PII or per-kg custody rows.
type polygonProvider struct {
	rpcURL   string
	contract string
}

func newPolygonProvider(rpcURL, contract string) (AnchorProvider, error) {
	if strings.TrimSpace(rpcURL) == "" {
		return nil, fmt.Errorf("polygon provider: ANCHOR_RPC_URL is required")
	}
	if strings.TrimSpace(contract) == "" {
		return nil, fmt.Errorf("polygon provider: ANCHOR_CONTRACT_ADDRESS is required")
	}
	return &polygonProvider{rpcURL: strings.TrimSpace(rpcURL), contract: strings.TrimSpace(contract)}, nil
}

func (p *polygonProvider) Name() string { return "polygon" }

func (p *polygonProvider) PublishRoot(_ context.Context, root []byte, _ AnchorMeta) (string, error) {
	if len(root) != 32 {
		return "", fmt.Errorf("polygon provider: root must be 32 bytes, got %d", len(root))
	}
	// TODO(L3): build+sign an EIP-1559 tx calling store(bytes32) on p.contract via
	// p.rpcURL using a KMS-held hot wallet, submit, and return the tx hash.
	_ = hex.EncodeToString(root)
	return "", ErrNotImplemented
}

func (p *polygonProvider) VerifyRoot(_ context.Context, _ string, _ []byte) (bool, error) {
	return false, ErrNotImplemented
}

// ErrNotImplemented marks an L3 provider that is selected but whose on-chain path
// is not yet wired. The anchor service degrades to L2-only on this error.
var ErrNotImplemented = fmt.Errorf("anchor: on-chain publish not implemented")
