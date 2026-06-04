package story

import (
	"testing"
	"time"

	"iag-traceability/backend/internal/store"
)

func TestJourneyStage(t *testing.T) {
	cases := map[string]string{
		"CHERRY_RECEIVED":    "Farm",
		"WET_MILL_COMPLETE": "Wet mill",
		"COA_ISSUED":         "Lab",
		"LOT_QR_PUBLISHED":   "Export",
		"UNKNOWN_STEP":       "UNKNOWN_STEP",
	}
	for in, want := range cases {
		if got := journeyStage(in); got != want {
			t.Fatalf("journeyStage(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestComposeJourney_dedupesAndSorts(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	events := []store.TraceEvent{
		{EventType: "CHERRY_RECEIVED", OccurredAt: now, SourceService: "scm"},
		{EventType: "CHERRY_RECEIVED", OccurredAt: now, SourceService: "scm"},
		{EventType: "COA_ISSUED", OccurredAt: now.Add(time.Hour), SourceService: "qc"},
	}
	journey := composeJourney(events)
	if len(journey) != 2 {
		t.Fatalf("expected 2 journey steps, got %d", len(journey))
	}
	if journey[0]["stage"] != "Farm" || journey[1]["stage"] != "Lab" {
		t.Fatalf("unexpected journey order: %+v", journey)
	}
}

func TestDefaultJourney_nonempty(t *testing.T) {
	if len(defaultJourney()) < 3 {
		t.Fatal("default journey should include farm→export steps")
	}
}
