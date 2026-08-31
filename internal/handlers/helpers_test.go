package handlers

import (
	"testing"

	"iag-traceability/backend/internal/store"
)

func TestParseLimit(t *testing.T) {
	cases := []struct {
		raw  string
		want int
	}{
		{"", defaultListLimit},
		{"abc", defaultListLimit},
		{"0", defaultListLimit},
		{"-5", defaultListLimit},
		{"50", 50},
		{"1000", maxListLimit},
	}
	for _, tc := range cases {
		if got := parseLimit(tc.raw); got != tc.want {
			t.Fatalf("parseLimit(%q)=%d want %d", tc.raw, got, tc.want)
		}
	}
}

func TestTrimForLimit(t *testing.T) {
	mk := func(n int) []store.TraceEvent {
		out := make([]store.TraceEvent, n)
		return out
	}
	// Over-fetched by one → truncate and signal has_more.
	got, more := trimForLimit(mk(11), 10)
	if len(got) != 10 || !more {
		t.Fatalf("over-fetch: len=%d more=%v want len=10 more=true", len(got), more)
	}
	// Exactly at limit → no truncation, no has_more.
	got, more = trimForLimit(mk(10), 10)
	if len(got) != 10 || more {
		t.Fatalf("at-limit: len=%d more=%v want len=10 more=false", len(got), more)
	}
	// Under limit → unchanged.
	got, more = trimForLimit(mk(3), 10)
	if len(got) != 3 || more {
		t.Fatalf("under: len=%d more=%v want len=3 more=false", len(got), more)
	}
}

func TestValidEntityType(t *testing.T) {
	for _, ok := range []string{"lot", "batch", "farm", "party"} {
		if !validEntityType(ok) {
			t.Fatalf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "Lot", "shipment", "user"} {
		if validEntityType(bad) {
			t.Fatalf("%q should be invalid", bad)
		}
	}
}

// The event ledger regression.
//
// ListEvents used to require entity_type + entity_business_id unconditionally,
// which left the ledger page — which has no entity in hand — with no endpoint
// it could call. The frontend worked around it by reading the activity feed
// instead, which is a superset and not the ledger. These pin the rule that
// replaced it, including the half-filter case, which is the one that would
// silently widen a scoped query into an unscoped one.
func TestEventFilterMode(t *testing.T) {
	cases := []struct {
		name       string
		entityType string
		businessID string
		wantScoped bool
		wantRefuse bool
	}{
		{"neither — the ledger", "", "", false, false},
		{"both — a drill-down", "batch", "BATCH-001", true, false},
		{"type without id", "batch", "", false, true},
		{"id without type", "", "BATCH-001", false, true},
		{"unknown entity type", "tractor", "T-1", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scoped, reason := eventFilterMode(tc.entityType, tc.businessID)
			if scoped != tc.wantScoped {
				t.Fatalf("scoped = %v, want %v", scoped, tc.wantScoped)
			}
			if (reason != "") != tc.wantRefuse {
				t.Fatalf("reason = %q, want refuse = %v", reason, tc.wantRefuse)
			}
		})
	}
}
