package handlers

import (
	"strings"
	"testing"
	"time"

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
// it could call. These pin the rule that replaced it. entity_type alone is now
// a real filter (it used to be refused because it would have been silently
// ignored); an id without a type is still ambiguous and refused.
func TestParseEventFilter(t *testing.T) {
	cases := []struct {
		name                   string
		entityType, businessID string
		eventTypes, from, to   string
		wantRefuse             bool
		wantTypes              int
		wantFrom, wantTo       string
	}{
		{name: "neither — the ledger"},
		{name: "both — a drill-down", entityType: "batch", businessID: "BATCH-001"},
		{name: "type alone filters", entityType: "lot"},
		{name: "id without type", businessID: "BATCH-001", wantRefuse: true},
		{name: "unknown entity type", entityType: "tractor", businessID: "T-1", wantRefuse: true},
		{name: "event type list", eventTypes: "CUSTODY_TRANSFERRED, CUSTODY_RECEIVED,,", wantTypes: 2},
		{name: "date-only range includes the to day", from: "2026-10-01", to: "2026-10-02",
			wantFrom: "2026-10-01T00:00:00Z", wantTo: "2026-10-03T00:00:00Z"},
		{name: "rfc3339 to is exclusive as given", to: "2026-10-02T10:00:00+03:00", wantTo: "2026-10-02T07:00:00Z"},
		{name: "bad from", from: "yesterday", wantRefuse: true},
		{name: "bad to", to: "2026-13-40", wantRefuse: true},
		{name: "inverted range", from: "2026-10-05", to: "2026-10-01", wantRefuse: true},
		{name: "too many types", eventTypes: strings.Repeat("A,", 26), wantRefuse: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, reason := parseEventFilter(tc.entityType, tc.businessID, tc.eventTypes, tc.from, tc.to)
			if (reason != "") != tc.wantRefuse {
				t.Fatalf("reason = %q, want refuse = %v", reason, tc.wantRefuse)
			}
			if tc.wantRefuse {
				return
			}
			if f.EntityType != tc.entityType || f.EntityBusinessID != tc.businessID {
				t.Fatalf("entity = %q/%q", f.EntityType, f.EntityBusinessID)
			}
			if len(f.EventTypes) != tc.wantTypes {
				t.Fatalf("event types = %v, want %d", f.EventTypes, tc.wantTypes)
			}
			if tc.wantFrom != "" && (f.From == nil || f.From.Format(time.RFC3339) != tc.wantFrom) {
				t.Fatalf("from = %v, want %s", f.From, tc.wantFrom)
			}
			if tc.wantTo != "" && (f.To == nil || f.To.Format(time.RFC3339) != tc.wantTo) {
				t.Fatalf("to = %v, want %s", f.To, tc.wantTo)
			}
		})
	}
}

func TestValidateStoryPatch(t *testing.T) {
	s := func(v string) *string { return &v }
	if validateStoryPatch(nil, nil) == "" {
		t.Fatal("empty patch must be refused")
	}
	if r := validateStoryPatch(s(""), nil); r != "" {
		t.Fatalf("empty string clears, must be accepted: %s", r)
	}
	if validateStoryPatch(s(strings.Repeat("é", 201)), nil) == "" {
		t.Fatal("201-char title must be refused")
	}
	if r := validateStoryPatch(s(strings.Repeat("é", 200)), s(strings.Repeat("x", 5000))); r != "" {
		t.Fatalf("limits are inclusive: %s", r)
	}
	if validateStoryPatch(nil, s(strings.Repeat("x", 5001))) == "" {
		t.Fatal("5001-char body must be refused")
	}
}
