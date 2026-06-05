package story

import (
	"context"
	"sort"

	"iag-traceability/backend/internal/scmclient"
	"iag-traceability/backend/internal/store"
)

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
	if len(events) == 0 {
		// Aggregate batch events linked to lot batches when lot entity events sparse.
		if batchIDs, ok := story["lot"].(map[string]any)["batch_ids"].([]string); ok {
			for _, bid := range batchIDs {
				batchEv, _ := st.ListEventsForEntity(ctx, "batch", bid, 50)
				events = append(events, batchEv...)
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
		if name, ok := strMapField(entry, "name"); ok {
			entry["name"] = name
		}
		if loc, ok := entry["location"].(map[string]any); ok {
			if district, ok := strMapField(loc, "district"); ok {
				entry["district"] = district
			}
			if lat, lng, ok := coordsFromMap(loc); ok {
				entry["map"] = map[string]any{"lat": lat, "lng": lng}
			}
		}
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
