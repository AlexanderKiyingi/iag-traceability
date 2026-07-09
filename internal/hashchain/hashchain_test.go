package hashchain_test

import (
	"testing"

	"iag-traceability/backend/internal/hashchain"
)

func TestEventHashDeterministic(t *testing.T) {
	metrics := map[string]interface{}{"weightKg": 850.0, "cherryGrade": "A"}
	h1 := hashchain.EventHash(1, 1720000000000, "BTC-101", "batch.registered", "clerk", metrics, hashchain.GenesisHash)
	h2 := hashchain.EventHash(1, 1720000000000, "BTC-101", "batch.registered", "clerk", metrics, hashchain.GenesisHash)
	if h1 != h2 {
		t.Fatalf("expected deterministic hash, got %s vs %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("expected sha256 hex length 64, got %d", len(h1))
	}
}

func TestEventHashChangesWhenPrevChanges(t *testing.T) {
	metrics := map[string]interface{}{}
	a := hashchain.EventHash(2, 1, "BTC-101", "batch.status.drying", "actor", metrics, "abc")
	b := hashchain.EventHash(2, 1, "BTC-101", "batch.status.drying", "actor", metrics, "def")
	if a == b {
		t.Fatal("hash should differ when prev_hash differs")
	}
}

func TestEventHashChangesWhenContentChanges(t *testing.T) {
	prev := hashchain.GenesisHash
	base := hashchain.EventHash(1, 100, "BTC-1", "a", "x", map[string]interface{}{"k": 1.0}, prev)
	diffMetric := hashchain.EventHash(1, 100, "BTC-1", "a", "x", map[string]interface{}{"k": 2.0}, prev)
	if base == diffMetric {
		t.Fatal("hash should differ when quality metrics differ")
	}
}
