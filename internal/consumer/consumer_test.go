package consumer

import "testing"

func TestMapEvent_supplyChainTypes(t *testing.T) {
	data := map[string]any{
		"batch_business_id":  "BAT-1",
		"lot_business_id":    "LOT-1",
		"party_business_id":  "FRM-1",
		"farm_business_id":   "FARM-1",
		"batch_business_ids": []any{"BAT-1", "BAT-2"},
	}
	cases := []struct {
		eventType                            string
		wantMapped, wantEntity, wantEntityID string
	}{
		{"scm.batch.stage_changed", "STAGE_CHANGED", "batch", "BAT-1"},
		{"scm.intake.received", "CHERRY_RECEIVED", "batch", "BAT-1"},
		{"scm.party.created", "PARTY_CREATED", "party", "FRM-1"},
		{"scm.party.updated", "PARTY_UPDATED", "party", "FRM-1"},
		{"scm.farm.registered", "FARM_REGISTERED", "farm", "FARM-1"},
		{"scm.farm.updated", "FARM_UPDATED", "farm", "FARM-1"},
		{"scm.lot.assembled", "LOT_ASSEMBLED", "lot", "LOT-1"},
		{"mes.wetmill.started", "WET_MILL_STARTED", "batch", "BAT-1"},
		{"mes.drying.started", "DRYING_STARTED", "batch", "BAT-1"},
		{"qc.sample.submitted", "SAMPLE_SUBMITTED", "batch", "BAT-1"},
		{"scm.lot.qr_published", "LOT_QR_PUBLISHED", "lot", "LOT-1"},
		{"mes.wetmill.completed", "WET_MILL_COMPLETE", "batch", "BAT-1"},
		{"qc.coa.issued", "COA_ISSUED", "lot", "LOT-1"},
		{"unknown.event", "", "", ""},
	}
	for _, tc := range cases {
		mapped, entity, id := mapEvent(tc.eventType, data)
		if mapped != tc.wantMapped || entity != tc.wantEntity || id != tc.wantEntityID {
			t.Fatalf("%s: got (%q,%q,%q) want (%q,%q,%q)", tc.eventType, mapped, entity, id, tc.wantMapped, tc.wantEntity, tc.wantEntityID)
		}
	}
}

func TestMapEvent_CooperativeMemberChanged(t *testing.T) {
	mapped, entity, id := mapEvent("scm.party.member_changed", map[string]any{
		"cooperative_business_id": "COOP-001",
		"member_business_id":      "FRM-007",
		"action":                  "added",
	})
	if mapped != "MEMBER_CHANGED" || entity != "party" || id != "FRM-007" {
		t.Fatalf("got (%q,%q,%q) want (MEMBER_CHANGED,party,FRM-007)", mapped, entity, id)
	}
}

func TestMapEvent_CoAFallsBackToBatch(t *testing.T) {
	// QC may emit the CoA keyed by batch (no lot_business_id). It must map to the
	// batch entity, not resolve to an empty ID and get dropped.
	mapped, entity, id := mapEvent("qc.coa.issued", map[string]any{"batch_business_id": "BAT-9"})
	if mapped != "COA_ISSUED" || entity != "batch" || id != "BAT-9" {
		t.Fatalf("got (%q,%q,%q) want (COA_ISSUED,batch,BAT-9)", mapped, entity, id)
	}
	// Lot key still wins when present.
	mapped, entity, id = mapEvent("qc.coa.issued", map[string]any{"lot_business_id": "LOT-9", "batch_business_id": "BAT-9"})
	if mapped != "COA_ISSUED" || entity != "lot" || id != "LOT-9" {
		t.Fatalf("got (%q,%q,%q) want (COA_ISSUED,lot,LOT-9)", mapped, entity, id)
	}
}
