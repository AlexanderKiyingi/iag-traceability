package story

import (
	"strings"
	"testing"

	"iag-traceability/backend/internal/store"
)

func TestTransferTypesAccepted(t *testing.T) {
	for _, typ := range []string{"CUSTODY_TRANSFERRED", "CUSTODY_IN_TRANSIT", "CUSTODY_RECEIVED"} {
		if err := ValidateEventType(typ); err != nil {
			t.Fatalf("%s should be accepted: %v", typ, err)
		}
	}
}

func TestValidateEventPayload_Transfers(t *testing.T) {
	ok := map[string]any{"from": "Wet mill A", "to": "Warehouse 2", "quantity_kg": 120.5}
	cases := []struct {
		name       string
		eventType  string
		entityType string
		payload    map[string]any
		wantErr    string
	}{
		{"valid lot transfer", "CUSTODY_TRANSFERRED", "lot", ok, ""},
		{"valid batch receipt, zero kg", "CUSTODY_RECEIVED", "batch",
			map[string]any{"from": "a", "to": "b", "quantity_kg": float64(0)}, ""},
		{"farm is not a custody entity", "CUSTODY_IN_TRANSIT", "farm", ok, "entity_type lot or batch"},
		{"nil payload", "CUSTODY_TRANSFERRED", "lot", nil, "payload.from"},
		{"blank from", "CUSTODY_TRANSFERRED", "lot",
			map[string]any{"from": "  ", "to": "b", "quantity_kg": 1.0}, "payload.from"},
		{"missing to", "CUSTODY_TRANSFERRED", "lot",
			map[string]any{"from": "a", "quantity_kg": 1.0}, "payload.to"},
		{"non-string to", "CUSTODY_TRANSFERRED", "lot",
			map[string]any{"from": "a", "to": 7.0, "quantity_kg": 1.0}, "payload.to"},
		{"missing quantity", "CUSTODY_TRANSFERRED", "lot",
			map[string]any{"from": "a", "to": "b"}, "quantity_kg"},
		{"string quantity", "CUSTODY_TRANSFERRED", "lot",
			map[string]any{"from": "a", "to": "b", "quantity_kg": "12"}, "number"},
		{"negative quantity", "CUSTODY_TRANSFERRED", "lot",
			map[string]any{"from": "a", "to": "b", "quantity_kg": -0.1}, ">= 0"},
		{"other types are untouched", "MANUAL_NOTE", "farm", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEventPayload(tc.eventType, tc.entityType, tc.payload)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestJourneyStage_CustodyTypes(t *testing.T) {
	for _, typ := range []string{"CUSTODY_TRANSFERRED", "CUSTODY_IN_TRANSIT", "CUSTODY_RECEIVED"} {
		if got := journeyStage(typ); got != "Custody" {
			t.Fatalf("journeyStage(%s) = %q, want Custody", typ, got)
		}
	}
}

func TestApplyStoryOverride(t *testing.T) {
	title, body := "Grown at altitude", "A short story."
	composed := func() map[string]any {
		return map[string]any{"journey": []any{"x"}, "product": map[string]any{"grade": "AA"}}
	}

	s := composed()
	ApplyStoryOverride(s, nil)
	if _, ok := s["story_title"]; ok {
		t.Fatal("nil override must not add keys")
	}

	s = composed()
	ApplyStoryOverride(s, &store.StoryOverride{StoryTitle: &title})
	if s["story_title"] != title {
		t.Fatalf("title = %v", s["story_title"])
	}
	if _, ok := s["story_body"]; ok {
		t.Fatal("unset body must not be added")
	}
	if s["product"].(map[string]any)["grade"] != "AA" || s["journey"] == nil {
		t.Fatal("the rest of the composed story must stay")
	}

	s = composed()
	s["story_title"] = "composed title"
	ApplyStoryOverride(s, &store.StoryOverride{StoryTitle: &title, StoryBody: &body})
	if s["story_title"] != title || s["story_body"] != body {
		t.Fatalf("override must win: %v / %v", s["story_title"], s["story_body"])
	}
}
