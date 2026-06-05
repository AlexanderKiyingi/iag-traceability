package story

import (
	"testing"
	"time"

	"iag-traceability/backend/internal/store"
)

func TestEnrichProductFromEvents(t *testing.T) {
	story := map[string]any{"product": map[string]any{"origin_country": "Uganda"}}
	events := []store.TraceEvent{{
		EventType: "LAB_RESULT_RECORDED",
		Payload: map[string]any{
			"moisture":  11.2,
			"cup_score": 86.5,
			"grade":     "Specialty",
		},
	}}
	enrichProductFromEvents(story, events)
	product := story["product"].(map[string]any)
	if product["moisture"] != 11.2 || product["cup_score"] != 86.5 || product["grade"] != "Specialty" {
		t.Fatalf("unexpected product: %+v", product)
	}
}

func TestComposeJourney_noPlaceholderWhenDisabled(t *testing.T) {
	old := opts.PlaceholderJourney
	opts.PlaceholderJourney = false
	t.Cleanup(func() { opts.PlaceholderJourney = old })

	journey := composeJourney(nil)
	if len(journey) != 0 {
		t.Fatalf("expected empty journey without placeholder, got %d steps", len(journey))
	}
}

func TestComposeJourney_withEvents(t *testing.T) {
	now := time.Now().UTC()
	journey := composeJourney([]store.TraceEvent{{
		EventType: "WET_MILL_STARTED", OccurredAt: now, SourceService: "iag-mes",
	}})
	if len(journey) != 1 || journey[0]["stage"] != "Wet mill" {
		t.Fatalf("unexpected journey: %+v", journey)
	}
}
