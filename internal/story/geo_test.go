package story

import "testing"

func TestPublicCoords_CoarsenedByDefault(t *testing.T) {
	old := opts
	defer func() { opts = old }()

	opts = Options{PreciseGeo: false}
	lat, lng := publicCoords(0.347596, 32.582520)
	if lat != 0.35 || lng != 32.58 {
		t.Fatalf("coarsened got (%v,%v) want (0.35,32.58)", lat, lng)
	}
	// Negative coordinates round away from zero symmetrically.
	lat, lng = publicCoords(-1.286389, -36.817223)
	if lat != -1.29 || lng != -36.82 {
		t.Fatalf("coarsened negative got (%v,%v) want (-1.29,-36.82)", lat, lng)
	}
}

func TestPublicCoords_PreciseWhenEnabled(t *testing.T) {
	old := opts
	defer func() { opts = old }()

	opts = Options{PreciseGeo: true}
	lat, lng := publicCoords(0.347596, 32.582520)
	if lat != 0.347596 || lng != 32.582520 {
		t.Fatalf("precise got (%v,%v) want exact input", lat, lng)
	}
}

func TestRedactPublicEntity_StripsPIIAndCoarsensGeo(t *testing.T) {
	old := opts
	defer func() { opts = old }()
	opts = Options{PreciseGeo: false}

	entry := map[string]any{
		"business_id": "FRM-1",
		"name":        "Jane Coffee",
		"phone":       "+256700000000",
		"Email":       "jane@example.com", // mixed-case key must still be removed
		"national_id": "CM900001",
		"address":     "Plot 5, Mbale",
		"lat":         1.075123,
		"lng":         34.175987,
		"location": map[string]any{
			"district": "Mbale",
			"lat":      1.075123,
			"lng":      34.175987,
		},
	}
	redactPublicEntity(entry)

	for _, leaked := range []string{"phone", "Email", "national_id", "address", "lat", "lng", "location"} {
		if _, ok := entry[leaked]; ok {
			t.Fatalf("sensitive key %q must be removed from public entity", leaked)
		}
	}
	if entry["name"] != "Jane Coffee" {
		t.Fatalf("name should be preserved, got %v", entry["name"])
	}
	if entry["district"] != "Mbale" {
		t.Fatalf("district should be preserved, got %v", entry["district"])
	}
	m, ok := entry["map"].(map[string]any)
	if !ok || m["lat"] != 1.08 || m["lng"] != 34.18 {
		t.Fatalf("coarsened map got %v want lat=1.08 lng=34.18", entry["map"])
	}
}

func TestSanitizePreviewEntities_RedactsArrays(t *testing.T) {
	old := opts
	defer func() { opts = old }()
	opts = Options{PreciseGeo: false}

	preview := map[string]any{
		"farmers": []any{
			map[string]any{"name": "A", "phone": "123", "lat": 0.5, "lng": 32.6},
		},
		"farms": []map[string]any{
			{"name": "Estate", "email": "x@y.z", "location": map[string]any{"lat": 0.5, "lng": 32.6}},
		},
	}
	sanitizePreviewEntities(preview)

	farmer := preview["farmers"].([]any)[0].(map[string]any)
	if _, ok := farmer["phone"]; ok {
		t.Fatal("preview farmer phone must be redacted")
	}
	if _, ok := farmer["lat"]; ok {
		t.Fatal("preview farmer lat must be redacted")
	}
	farm := preview["farms"].([]map[string]any)[0]
	if _, ok := farm["email"]; ok {
		t.Fatal("preview farm email must be redacted")
	}
	if _, ok := farm["location"]; ok {
		t.Fatal("preview farm location must be redacted")
	}
}

func TestSanitizePublicGeo_StripsRawLocation(t *testing.T) {
	old := opts
	defer func() { opts = old }()
	opts = Options{PreciseGeo: false}

	entry := map[string]any{
		"business_id": "FARM-1",
		"location": map[string]any{
			"district": "Mbale",
			"lat":      1.075123,
			"lng":      34.175987,
		},
	}
	sanitizePublicGeo(entry)

	if _, ok := entry["location"]; ok {
		t.Fatal("raw location must be removed from public entry")
	}
	if entry["district"] != "Mbale" {
		t.Fatalf("district should be preserved, got %v", entry["district"])
	}
	m, ok := entry["map"].(map[string]any)
	if !ok {
		t.Fatal("coarsened map missing")
	}
	if m["lat"] != 1.08 || m["lng"] != 34.18 {
		t.Fatalf("coarsened map got %v want lat=1.08 lng=34.18", m)
	}
}
