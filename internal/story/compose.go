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
	if len(journey) == 0 {
		journey = defaultJourney()
	}
	story["journey"] = journey
	return story, nil
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
	case "WET_MILL_COMPLETE", "mes.wetmill.completed":
		return "Wet mill"
	case "DRYING_COMPLETE", "mes.drying.completed":
		return "Drying"
	case "DRY_MILL_COMPLETE", "mes.drymill.completed":
		return "Dry mill"
	case "LAB_RESULT_RECORDED", "qc.lab.result_recorded":
		return "Lab"
	case "COA_ISSUED", "qc.coa.issued":
		return "Lab"
	case "LOT_QR_PUBLISHED":
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
