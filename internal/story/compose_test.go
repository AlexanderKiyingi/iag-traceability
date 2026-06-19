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

func TestBatchIDsFromLot(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want []string
	}{
		{"native string slice", []string{"BAT-1", "BAT-2"}, []string{"BAT-1", "BAT-2"}},
		{"json any slice", []any{"BAT-1", "BAT-2"}, []string{"BAT-1", "BAT-2"}},
		{"json any slice with non-strings dropped", []any{"BAT-1", 42, "", "BAT-2"}, []string{"BAT-1", "BAT-2"}},
		{"nil", nil, nil},
		{"wrong type", "BAT-1", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := batchIDsFromLot(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestEnrichProductFromEvents_labResultsOnBatchEntity guards the A3 fix: lab
// results are recorded against the *batch* entity, so the composer must lift
// moisture/cup_score/grade from those batch events into the public story.
func TestEnrichProductFromEvents_labResultsOnBatchEntity(t *testing.T) {
	story := map[string]any{"product": map[string]any{"origin_country": "Uganda"}}
	events := []store.TraceEvent{
		{EventType: "CHERRY_RECEIVED", EntityType: "batch", EntityBusinessID: "BAT-1"},
		{
			EventType:        "LAB_RESULT_RECORDED",
			EntityType:       "batch",
			EntityBusinessID: "BAT-1",
			Payload: map[string]any{
				"moisture":  11.5,
				"cup_score": 84.0,
				"grade":     "AA",
			},
		},
	}
	enrichProductFromEvents(story, events)
	product, _ := story["product"].(map[string]any)
	if product == nil {
		t.Fatal("product missing")
	}
	if product["moisture"] != 11.5 {
		t.Fatalf("moisture = %v, want 11.5", product["moisture"])
	}
	if product["cup_score"] != 84.0 {
		t.Fatalf("cup_score = %v, want 84.0", product["cup_score"])
	}
	if product["grade"] != "AA" {
		t.Fatalf("grade = %v, want AA", product["grade"])
	}
}
