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
