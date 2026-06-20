package story

import (
	"context"
	"sort"
	"strings"

	"iag-traceability/backend/internal/scmclient"
	"iag-traceability/backend/internal/store"
)

// lotStoryEventTypes are the mapped event types whose arrival should refresh a
// lot's composed story projection. Shared by the Kafka consumer and the manual
// HTTP record-event path so both stay consistent.
var lotStoryEventTypes = map[string]bool{
	"LOT_QR_PUBLISHED":    true,
	"COA_ISSUED":          true,
	"LAB_RESULT_RECORDED": true,
	"LOT_ASSEMBLED":       true,
	"WET_MILL_STARTED":    true,
	"WET_MILL_COMPLETE":   true,
	"DRYING_STARTED":      true,
	"DRYING_COMPLETE":     true,
	"DRY_MILL_COMPLETE":   true,
	"SAMPLE_SUBMITTED":    true,
	"CHERRY_RECEIVED":     true,
	"STAGE_CHANGED":       true,
}

// AffectsLotStory reports whether an event of the given mapped type should
// trigger a lot story rebuild.
func AffectsLotStory(mappedType string) bool {
	return lotStoryEventTypes[mappedType]
}

// RebuildStoredLot recomposes a lot's story (using the configured SCM client)
// and persists it. Returns true if the projection was rebuilt and stored.
func RebuildStoredLot(ctx context.Context, st *store.Store, lotBusinessID, publicURL string) (bool, error) {
	composed, err := RebuildLotProjection(ctx, st, scmFetcher, lotBusinessID, publicURL)
	if err != nil {
		return false, err
	}
	if err := st.UpsertStoryProjection(ctx, lotBusinessID, composed); err != nil {
		return false, err
	}
	return true, nil
}

// RebuildLotProjection composes a public coffee story from trace events + SCM lot snapshot.
func RebuildLotProjection(ctx context.Context, st *store.Store, scm *scmclient.Client, lotBusinessID, publicURL string) (map[string]any, error) {
	story := map[string]any{
		"brand":      "IAG Africa Coffee Park",
		"public_url": publicURL,
		"standards":  []string{"ISO 22005:2007", "GS1 EPCIS 2.0", "EU Reg. 2023/1115", "SCA Protocol"},
		"product":    map[string]any{"origin_country": "Uganda"},
		"lot":        map[string]any{"business_id": lotBusinessID},
	}

	if scm != nil && scm.Enabled() {
		if preview, err := scm.GetQRPreview(ctx, lotBusinessID); err == nil && preview != nil {
			// The SCM preview is raw upstream JSON returned verbatim — it bypasses
			// the per-entity enrichment below, so apply the same redaction to any
			// farmers/farms it carries before serving it to anonymous consumers.
			sanitizePreviewEntities(preview)
			return preview, nil
		}
		if lot, err := scm.GetExportLot(ctx, lotBusinessID); err == nil {
			story["lot"] = map[string]any{
				"business_id": lot.BusinessID,
				"buyer_name":  lot.BuyerName,
				"destination": lot.Destination,
				"batch_ids":   lot.BatchIDs,
			}
			if lot.CoaNumber != nil {
				story["lot"].(map[string]any)["coa_number"] = *lot.CoaNumber
			}
		}
	}

	events, _ := st.ListEventsForEntity(ctx, "lot", lotBusinessID, 100)
	// Always aggregate events from the lot's constituent batches, not only
	// when the lot has no events of its own. Quality data (LAB_RESULT_RECORDED,
	// CHERRY_RECEIVED, mill stages) is recorded against the *batch* entity, so
	// skipping this whenever the lot had any event of its own silently dropped
	// moisture/cup-score/grade from the public story in the common case.
	seenEvent := map[string]bool{}
	for _, ev := range events {
		seenEvent[ev.ID.String()] = true
	}
	if lotMap, ok := story["lot"].(map[string]any); ok {
		for _, bid := range batchIDsFromLot(lotMap["batch_ids"]) {
			batchEv, _ := st.ListEventsForEntity(ctx, "batch", bid, 50)
			for _, ev := range batchEv {
				if seenEvent[ev.ID.String()] {
					continue
				}
				seenEvent[ev.ID.String()] = true
				events = append(events, ev)
			}
		}
	}

	journey := composeJourney(events)
	if len(journey) == 0 && opts.PlaceholderJourney {
		journey = defaultJourney()
	}
	story["journey"] = journey
	enrichFarmsFromProjections(ctx, st, story, events)
	enrichFarmersFromProjections(ctx, st, story, events)
	enrichProductFromEvents(story, events)
	return story, nil
}

// batchIDsFromLot extracts batch business IDs from a lot map's "batch_ids"
// field, tolerating both a native []string (from GetExportLot) and a
// JSON-decoded []any (from an SCM preview/snapshot).
func batchIDsFromLot(v any) []string {
	switch ids := v.(type) {
	case []string:
		return ids
	case []any:
		out := make([]string, 0, len(ids))
		for _, e := range ids {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func enrichFarmsFromProjections(ctx context.Context, st *store.Store, story map[string]any, events []store.TraceEvent) {
	seen := map[string]bool{}
	var farms []map[string]any
	for _, ev := range events {
		farmID := ""
		if ev.EntityType == "farm" {
			farmID = ev.EntityBusinessID
		} else if id, ok := strMapField(ev.RelatedIDs, "farm_business_id"); ok {
			farmID = id
		}
		if farmID == "" || seen[farmID] {
			continue
		}
		seen[farmID] = true
		if proj, err := st.GetEntityProjection(ctx, "farm", farmID); err == nil {
			entry := map[string]any{"business_id": farmID}
			for k, v := range proj {
				entry[k] = v
			}
			redactPublicEntity(entry)
			farms = append(farms, entry)
		}
	}
	if len(farms) > 0 {
		story["farms"] = farms
	}
}

func enrichFarmersFromProjections(ctx context.Context, st *store.Store, story map[string]any, events []store.TraceEvent) {
	seen := map[string]bool{}
	var farmers []map[string]any
	for _, ev := range events {
		partyID := ""
		if ev.EntityType == "party" {
			partyID = ev.EntityBusinessID
		}
		if partyID == "" {
			if id, ok := strMapField(ev.RelatedIDs, "party_business_id"); ok {
				partyID = id
			} else if id, ok := strMapField(ev.RelatedIDs, "farmer_business_id"); ok {
				partyID = id
			}
		}
		if partyID == "" || seen[partyID] {
			continue
		}
		seen[partyID] = true
		entry := map[string]any{"business_id": partyID}
		if proj, err := st.GetEntityProjection(ctx, "party", partyID); err == nil {
			for k, v := range proj {
				entry[k] = v
			}
		}
		// The party projection is the raw SCM event payload, which may carry PII
		// (phone, email, national ID, exact GPS) never meant for the anonymous
		// public story. Redact it down to safe, coarsened fields.
		redactPublicEntity(entry)
		farmers = append(farmers, entry)
	}
	if len(farmers) > 0 {
		story["farmers"] = farmers
	}
}

func enrichProductFromEvents(story map[string]any, events []store.TraceEvent) {
	product, _ := story["product"].(map[string]any)
	if product == nil {
		product = map[string]any{"origin_country": "Uganda"}
		story["product"] = product
	}
	for _, ev := range events {
		if ev.EventType != "LAB_RESULT_RECORDED" && ev.EventType != "qc.lab.result_recorded" {
			continue
		}
		data := ev.Payload
		if data == nil {
			data = ev.RelatedIDs
		}
		if data == nil {
			continue
		}
		if v, ok := numField(data, "moisture"); ok {
			product["moisture"] = v
		}
		if v, ok := numField(data, "cup_score"); ok {
			product["cup_score"] = v
		}
		if g, ok := strMapField(data, "grade"); ok {
			product["grade"] = g
		}
		if region, ok := strMapField(data, "origin_region"); ok {
			product["origin_region"] = region
		}
	}
}

func numField(data map[string]any, key string) (float64, bool) {
	v, ok := data[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

// sensitivePublicKeys are projection field names that must never appear in the
// anonymous public QR payload. Matched case-insensitively against each key. The
// party/farm projection is the raw SCM event, so it may carry any of these.
var sensitivePublicKeys = map[string]bool{
	"phone": true, "phone_number": true, "telephone": true, "mobile": true,
	"msisdn": true, "contact": true, "contact_number": true,
	"email": true, "email_address": true,
	"national_id": true, "nin": true, "id_number": true, "id_no": true,
	"passport": true, "passport_number": true, "tin": true,
	"address": true, "physical_address": true, "residential_address": true,
	"next_of_kin": true, "kin": true,
	"dob": true, "date_of_birth": true, "birth_date": true, "gender": true,
	"bank": true, "bank_account": true, "account_number": true,
	"gps": true, "geo": true, "coordinates": true, "coords": true,
	"latitude": true, "longitude": true, "lat": true, "lng": true, "location": true,
}

// redactPublicEntity makes a farmer/farm projection entry safe for the anonymous
// public payload: it coarsens geo (preserving district + a ~1km map point) and
// removes any known PII / precise-location keys. Applied uniformly to every
// entity that reaches the public story.
func redactPublicEntity(entry map[string]any) {
	// Derive the coarsened map + district first (reads location/lat/lng), then
	// the denylist sweep below removes the raw precise fields.
	sanitizePublicGeo(entry)
	for k := range entry {
		if sensitivePublicKeys[strings.ToLower(k)] {
			delete(entry, k)
		}
	}
}

// sanitizePreviewEntities redacts the farmers/farms arrays of an SCM preview,
// which is otherwise returned verbatim and bypasses per-entity enrichment.
func sanitizePreviewEntities(preview map[string]any) {
	for _, key := range []string{"farmers", "farms"} {
		switch arr := preview[key].(type) {
		case []map[string]any:
			for _, e := range arr {
				redactPublicEntity(e)
			}
		case []any:
			for _, e := range arr {
				if m, ok := e.(map[string]any); ok {
					redactPublicEntity(m)
				}
			}
		}
	}
}

// sanitizePublicGeo coarsens or strips precise location data from a public
// projection entry: it replaces a nested "location" map's coordinates with a
// coarsened {lat,lng} "map" (district preserved) and drops the raw location and
// any top-level lat/lng so anonymous consumers never see exact GPS.
func sanitizePublicGeo(entry map[string]any) {
	if loc, ok := entry["location"].(map[string]any); ok {
		if district, ok := strMapField(loc, "district"); ok {
			entry["district"] = district
		}
		if lat, lng, ok := coordsFromMap(loc); ok {
			lat, lng = publicCoords(lat, lng)
			entry["map"] = map[string]any{"lat": lat, "lng": lng}
		}
		delete(entry, "location")
	}
	if lat, latOK := numField(entry, "lat"); latOK {
		if lng, lngOK := numField(entry, "lng"); lngOK {
			lat, lng = publicCoords(lat, lng)
			entry["map"] = map[string]any{"lat": lat, "lng": lng}
		}
		delete(entry, "lat")
		delete(entry, "lng")
	}
}

// publicCoords coarsens coordinates for the anonymous public payload. Unless
// PreciseGeo is enabled, lat/lng are rounded to 2 decimal places (~1.1km) so a
// scanned QR reveals the growing area without pinpointing a farmer's home.
func publicCoords(lat, lng float64) (float64, float64) {
	if opts.PreciseGeo {
		return lat, lng
	}
	round2 := func(v float64) float64 {
		// Round half away from zero at 2dp without importing math.
		if v >= 0 {
			return float64(int64(v*100+0.5)) / 100
		}
		return float64(int64(v*100-0.5)) / 100
	}
	return round2(lat), round2(lng)
}

func coordsFromMap(loc map[string]any) (lat, lng float64, ok bool) {
	lat, latOK := numField(loc, "lat")
	lng, lngOK := numField(loc, "lng")
	return lat, lng, latOK && lngOK
}

func strMapField(data map[string]any, key string) (string, bool) {
	if data == nil {
		return "", false
	}
	v, ok := data[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok && s != ""
}

func composeJourney(events []store.TraceEvent) []map[string]any {
	seen := map[string]bool{}
	var journey []map[string]any
	for _, ev := range events {
		step := map[string]any{
			"stage":       journeyStage(ev.EventType),
			"occurred_at": ev.OccurredAt.UTC().Format("2006-01-02T15:04:05Z"),
			"source":      ev.SourceService,
			"summary":     ev.EventType,
		}
		key := ev.EventType + step["occurred_at"].(string)
		if seen[key] {
			continue
		}
		seen[key] = true
		journey = append(journey, step)
	}
	sort.Slice(journey, func(i, j int) bool {
		return journey[i]["occurred_at"].(string) < journey[j]["occurred_at"].(string)
	})
	return journey
}

func journeyStage(eventType string) string {
	switch eventType {
	case "FARMER_REGISTERED", "FARM_REGISTERED":
		return "Farm"
	case "CHERRY_RECEIVED", "INTAKE_RECEIVED":
		return "Farm"
	case "WET_MILL_COMPLETE", "mes.wetmill.completed", "WET_MILL_STARTED", "mes.wetmill.started":
		return "Wet mill"
	case "DRYING_COMPLETE", "mes.drying.completed", "DRYING_STARTED", "mes.drying.started":
		return "Drying"
	case "DRY_MILL_COMPLETE", "mes.drymill.completed":
		return "Dry mill"
	case "LAB_RESULT_RECORDED", "qc.lab.result_recorded", "SAMPLE_SUBMITTED", "qc.sample.submitted":
		return "Lab"
	case "COA_ISSUED", "qc.coa.issued":
		return "Lab"
	case "LOT_QR_PUBLISHED", "LOT_ASSEMBLED":
		return "Export"
	default:
		return eventType
	}
}

func defaultJourney() []map[string]any {
	return []map[string]any{
		{"stage": "Farm", "source": "scm", "summary": "Farm origin"},
		{"stage": "Wet mill", "source": "mes", "summary": "Processing"},
		{"stage": "Lab", "source": "qc", "summary": "Quality analysis"},
		{"stage": "Export", "source": "scm", "summary": "Export lot"},
	}
}
