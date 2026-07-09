// Package hashchain computes the tamper-evident custody-chain hash. The
// algorithm mirrors iag-traceag-portal's hashchain so the two systems share one
// definition of a custody block hash.
package hashchain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// GenesisHash is the prev_hash of the first block in the chain.
const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// EventHash returns the sha256 hex of the canonical block payload. quality
// metrics are marshalled deterministically by encoding/json (map keys sorted),
// so the hash is stable across processes.
func EventHash(index int64, timestampUnixMilli int64, batchID, action, actor string, qualityMetrics map[string]interface{}, prevHash string) string {
	metricsJSON, _ := json.Marshal(qualityMetrics)
	payload := fmt.Sprintf("%d|%d|%s|%s|%s|%s|%s", index, timestampUnixMilli, batchID, action, actor, string(metricsJSON), prevHash)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}
